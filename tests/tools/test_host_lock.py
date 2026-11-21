"""tools.host_lock: one exclusive flock mutex per target, a fair TTL waiter queue, scoped release."""

import fcntl
import json
import multiprocessing
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.waiting import wait_until
from tools.host_lock import EXIT_QUEUED, HostLock, host_lock, lock_path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _free(path) -> bool:
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


@pytest.fixture
def lock_dir(tmp_path):
    return str(tmp_path)


def _lock(lock_dir, owner, **kwargs):
    return HostLock("db", owner=owner, lock_dir=lock_dir, prefix="test-admin-", **kwargs)


def test_lock_path_is_the_shared_flock_file(lock_dir):
    assert lock_path("db", lock_dir=lock_dir, prefix="test-admin-") == os.path.join(lock_dir, "test-admin-db.lock")
    for bad in ("", "../etc", "a/b", ".hidden"):
        with pytest.raises(ValueError):
            lock_path(bad, lock_dir=lock_dir)


def test_raw_flock_users_and_the_helper_exclude_each_other(lock_dir):
    lock = _lock(lock_dir, "helper")
    raw = os.open(lock.path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(raw, fcntl.LOCK_EX)
    try:
        attempt = lock.try_acquire(ttl=60)
        assert not attempt.acquired and attempt.position == 1
    finally:
        os.close(raw)
    assert lock.try_acquire(ttl=60).acquired
    assert not _free(lock.path)
    lock.release()
    assert _free(lock.path)


def test_holder_metadata_is_written_on_acquire_and_cleared_on_release(lock_dir):
    lock = _lock(lock_dir, "job-1", labels={"task": "T-1", "run": "r-9"})
    with lock:
        holder = _lock(lock_dir, "reader").holder()
        assert holder["pid"] == os.getpid() and holder["owner"] == "job-1"
        assert holder["labels"] == {"task": "T-1", "run": "r-9"} and holder["started_at"] > 0
    assert _lock(lock_dir, "reader").holder() is None


def test_a_holder_record_left_by_a_killed_process_is_flagged_stale(lock_dir):
    code = ("import os, sys; sys.path.insert(0, sys.argv[2]); from tools.host_lock import HostLock\n"
            "lock = HostLock('db', owner='killed', lock_dir=sys.argv[1], prefix='test-admin-')\n"
            "assert lock.try_acquire(ttl=5).acquired\n"
            "os._exit(0)")  # no release: the record stays, the kernel drops the flock
    subprocess.run([sys.executable, "-c", code, lock_dir, str(REPO_ROOT)], timeout=30, check=True)
    reader = _lock(lock_dir, "reader")
    assert reader.holder()["owner"] == "killed" and reader.holder()["stale"] is True
    assert reader.try_acquire(ttl=5).acquired and reader.holder()["stale"] is False
    reader.release()


# ── pass criterion: no overlap ───────────────────────────────────────────────────────────────
def _contender(lock_dir, owner, iterations, journal):
    for _ in range(iterations):
        with host_lock("db", owner=owner, lock_dir=lock_dir, prefix="test-admin-", poll=0.002, ttl=30):
            with open(journal, "a") as fh:
                fh.write(f"enter {owner}\n")
                fh.flush()
                time.sleep(0.001)  # health: allow HX013 -- widens the critical section so an overlap would show
                fh.write(f"exit {owner}\n")


def test_no_two_holders_under_multi_process_contention(lock_dir, tmp_path):
    journal = str(tmp_path / "journal")
    ctx = multiprocessing.get_context("fork")
    procs = [ctx.Process(target=_contender, args=(lock_dir, f"p{i}", 40, journal)) for i in range(4)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(120)
        assert proc.exitcode == 0
    lines = Path(journal).read_text().splitlines()
    assert len(lines) == 4 * 40 * 2
    inside = None
    for line in lines:
        kind, owner = line.split()
        if kind == "enter":
            assert inside is None, f"{owner} entered while {inside} held the lock"
            inside = owner
        else:
            assert inside == owner
            inside = None


# ── pass criterion: finite work yields ───────────────────────────────────────────────────────
def test_multi_window_job_yields_to_a_waiter_between_windows(lock_dir):
    order = []
    job_a, waiter_b = _lock(lock_dir, "job-a"), _lock(lock_dir, "job-b")

    assert job_a.try_acquire(ttl=60).acquired
    order.append("A1")
    # B queues during window 1 with a non-blocking try and will retry later.
    attempt = waiter_b.try_acquire(ttl=60)
    assert not attempt.acquired and attempt.position == 1 and attempt.holder["owner"] == "job-a"
    assert job_a.should_yield()
    # Window boundary: A yields and queues itself at the back.
    assert job_a.yield_if_waiters(ttl=60)
    assert not job_a.held
    # A's next window start is refused while B's older ticket waits, even though the flock is free.
    assert _free(job_a.path)
    again = job_a.try_acquire(ttl=60)
    assert not again.acquired and again.position == 2
    # B's retry wins.
    assert waiter_b.try_acquire(ttl=60).acquired
    order.append("B")
    assert waiter_b.should_yield()  # A is queued now
    waiter_b.release()
    # A resumes and finishes its remaining windows; nobody waits, so it does not yield.
    assert job_a.try_acquire(ttl=60).acquired
    order.append("A2")
    assert not job_a.should_yield() and not job_a.yield_if_waiters(ttl=60)
    order.append("A3")
    job_a.release()
    assert order == ["A1", "B", "A2", "A3"]
    assert job_a.queue() == []


def _window_job(lock_dir, journal, windows):
    lock = HostLock("db", owner="job-a", lock_dir=lock_dir, prefix="test-admin-")
    done = 0
    while done < windows:
        if not lock.try_acquire(ttl=30).acquired:
            time.sleep(0.01)  # health: allow HX013 -- the retry interval of a job that rescheduled itself
            continue
        with open(journal, "a") as fh:
            fh.write(f"A{done + 1}\n")
        time.sleep(0.15)  # health: allow HX013 -- one work window; the waiter must try inside it
        done += 1
        if done < windows and not lock.yield_if_waiters(ttl=30):
            lock.release()
    lock.release()


def _waiter(lock_dir, journal, ready):
    lock = HostLock("db", owner="job-b", lock_dir=lock_dir, prefix="test-admin-")
    wait_until(ready.exists, timeout=30)
    first = lock.try_acquire(ttl=30)
    with open(journal, "a") as fh:
        fh.write(f"B-try {'acquired' if first.acquired else 'queued'}\n")
    while not first.acquired:
        time.sleep(0.02)  # health: allow HX013 -- the waiter's retry interval (keeps its ticket)
        first = lock.try_acquire(ttl=30)
    with open(journal, "a") as fh:
        fh.write("B\n")
    lock.release()


def test_multi_window_job_yields_across_processes(lock_dir, tmp_path):
    journal, ready = tmp_path / "journal", tmp_path / "ready"
    ctx = multiprocessing.get_context("fork")
    job = ctx.Process(target=_window_job, args=(lock_dir, str(journal), 3))
    waiter = ctx.Process(target=_waiter, args=(lock_dir, str(journal), ready))
    waiter.start()
    job.start()
    wait_until(lambda: journal.exists() and "A1" in journal.read_text(), timeout=30)
    ready.touch()  # B tries during window 1
    for proc in (job, waiter):
        proc.join(60)
        assert proc.exitcode == 0
    assert journal.read_text().splitlines() == ["A1", "B-try queued", "B", "A2", "A3"]


def test_queue_positions_follow_registration_order(lock_dir):
    holder = _lock(lock_dir, "holder")
    assert holder.try_acquire(ttl=60).acquired
    first, second = _lock(lock_dir, "w1"), _lock(lock_dir, "w2")
    assert first.try_acquire(ttl=60).position == 1
    assert second.try_acquire(ttl=60).position == 2
    # Refreshing keeps the original place.
    assert first.try_acquire(ttl=60).position == 1
    assert [t["owner"] for t in holder.queue()] == ["w1", "w2"]
    holder.release()
    # Free flock, but w1 is older: w2 must not jump it.
    assert not second.try_acquire(ttl=60).acquired
    assert first.try_acquire(ttl=60).acquired
    first.release()
    assert second.try_acquire(ttl=60).acquired
    second.release()


# ── pass criterion: expired ticket ───────────────────────────────────────────────────────────
def test_an_expired_ticket_does_not_block_acquisition(lock_dir):
    now = [1000.0]
    dead = _lock(lock_dir, "dead-waiter", clock=lambda: now[0])
    holder = _lock(lock_dir, "holder", clock=lambda: now[0])
    assert holder.try_acquire(ttl=60).acquired
    assert dead.try_acquire(ttl=10).position == 1
    holder.release()
    late = _lock(lock_dir, "late", clock=lambda: now[0])
    assert not late.try_acquire(ttl=60).acquired  # the live ticket is honoured
    now[0] += 11  # the dead waiter never retried: its ticket expired
    assert late.try_acquire(ttl=60).acquired
    assert [t["owner"] for t in late.queue()] == []  # pruned
    late.release()


def test_a_dead_waiter_process_ticket_expires(lock_dir):
    code = ("import sys; sys.path.insert(0, sys.argv[2]); from tools.host_lock import HostLock\n"
            "print(HostLock('db', owner='gone', lock_dir=sys.argv[1], prefix='test-admin-').enqueue(ttl=0.3))")
    out = subprocess.run([sys.executable, "-c", code, lock_dir, str(REPO_ROOT)], capture_output=True, text=True,
                         timeout=30, check=True)
    assert out.stdout.strip() == "1"
    other = _lock(lock_dir, "alive")
    assert not other.try_acquire(ttl=60).acquired
    wait_until(lambda: other.try_acquire(ttl=60).acquired, timeout=10)
    assert other.held
    other.release()


# ── pass criterion: scoped release ───────────────────────────────────────────────────────────
def test_exception_inside_with_releases_the_lock(lock_dir):
    path = lock_path("db", lock_dir=lock_dir, prefix="test-admin-")
    with pytest.raises(RuntimeError):
        with host_lock("db", owner="boom", lock_dir=lock_dir, prefix="test-admin-"):
            assert not _free(path)
            raise RuntimeError("window failed")
    assert _free(path)
    assert _lock(lock_dir, "reader").holder() is None


def test_blocking_acquire_times_out_and_withdraws_its_ticket(lock_dir):
    holder = _lock(lock_dir, "holder")
    assert holder.try_acquire(ttl=60).acquired
    with pytest.raises(TimeoutError):
        with host_lock("db", owner="impatient", lock_dir=lock_dir, prefix="test-admin-", timeout=0.2, poll=0.05):
            pass
    assert holder.queue() == []
    holder.release()


def _cli(*args, **kwargs):
    return subprocess.run([sys.executable, "-m", "tools.host_lock", *args], cwd=REPO_ROOT,
                          capture_output=True, text=True, timeout=60, check=False, **kwargs)


def test_cli_run_holds_the_lock_only_while_the_child_runs(lock_dir):
    path = lock_path("db", lock_dir=lock_dir, prefix="test-admin-")
    child = ("import fcntl, json, os, sys\nfd = os.open(sys.argv[1], os.O_RDWR)\n"
             "try:\n    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\nexcept BlockingIOError:\n    held = True\n"
             "else:\n    held = False\nholder = json.load(open(sys.argv[1] + '.holder'))\n"
             "sys.exit(0 if held and holder['owner'] == 'cli' and holder['labels'] == {'task': 'T-2'} else 9)")
    result = _cli("run", "db", "--lock-dir", lock_dir, "--prefix", "test-admin-", "--owner", "cli",
                  "--label", "task=T-2", "--", sys.executable, "-c", child, path)
    assert result.returncode == 0, result.stderr
    assert _free(path)
    assert _lock(lock_dir, "reader").holder() is None
    # The child's exit status passes through, and the lock is still released.
    failed = _cli("run", "db", "--lock-dir", lock_dir, "--prefix", "test-admin-", "--",
                  sys.executable, "-c", "import sys; sys.exit(4)")
    assert failed.returncode == 4 and _free(path)


def test_cli_no_wait_reports_its_queue_place(lock_dir):
    holder = _lock(lock_dir, "holder")
    assert holder.try_acquire(ttl=60).acquired
    result = _cli("run", "db", "--lock-dir", lock_dir, "--prefix", "test-admin-", "--owner", "cron-job",
                  "--no-wait", "--ttl", "120", "--", sys.executable, "-c", "raise SystemExit(0)")
    assert result.returncode == EXIT_QUEUED
    report = json.loads(result.stderr.strip().splitlines()[-1])
    assert report["queued"] and report["position"] == 1 and report["holder"]["owner"] == "holder"
    assert [t["owner"] for t in holder.queue()] == ["cron-job"]
    status = json.loads(_cli("status", "db", "--lock-dir", lock_dir, "--prefix", "test-admin-").stdout)
    assert status["holder"]["owner"] == "holder" and status["queue"][0]["owner"] == "cron-job"
    holder.release()
    retry = _cli("run", "db", "--lock-dir", lock_dir, "--prefix", "test-admin-", "--owner", "cron-job",
                 "--no-wait", "--", sys.executable, "-c", "raise SystemExit(0)")
    assert retry.returncode == 0 and holder.queue() == []
