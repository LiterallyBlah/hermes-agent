"""Scoped host-administration lock: one exclusive ``flock`` mutex per target, plus a fair waiter queue.

Usage (from an execute_code cell, a script, or the CLI below)::

    from tools.host_lock import HostLock, host_lock

    # Blocking, scoped: released on exit and on exception.
    with host_lock("db", owner="job-42", prefix="myteam-admin-") as lock:
        ...

    # Non-blocking try from a job that retries later (monitor / cron):
    lock = HostLock("db", owner="job-43", prefix="myteam-admin-")
    attempt = lock.try_acquire(ttl=3900)        # TTL = retry interval + margin
    if attempt.acquired:
        with lock:                               # already held; released on exit
            ...
    else:
        ...  # attempt.position is this owner's place; retry later with the SAME owner

    # Multi-window job: yield to queued waiters at each window boundary.
    with host_lock("db", owner="job-44") as lock:
        do_window()
        if lock.yield_if_waiters(ttl=3900):      # released, own ticket now at the back
            ...  # schedule the next window; resume by calling try_acquire with the same owner

CLI (holds the lock only while the child runs)::

    python -m tools.host_lock run db --owner job-42 --prefix myteam-admin- -- ./maintenance.sh
    python -m tools.host_lock run db --owner job-43 --no-wait --ttl 3900 -- ./maintenance.sh
    python -m tools.host_lock status db --prefix myteam-admin-

Contract:

- The lock file is ``<lock_dir>/<prefix><target>.lock`` and the mutex is a plain exclusive ``flock``
  on it, so the helper interoperates with existing ``flock(1)`` / ``fcntl.flock`` users of the same
  file. There is never more than one holder: no lock-breaking, no killing holders, no timeout steal.
- ``<lock>.holder`` carries JSON holder metadata (pid, start time, owner, caller labels) while held,
  and is emptied before release.
- ``<lock>.queue`` is a JSON list of waiter tickets, guarded by its own short ``flock``. A ticket
  keeps its creation time across refreshes (so retrying keeps the owner's place) and expires
  ``ttl`` seconds after its last refresh; expired tickets are ignored and pruned, so a dead waiter
  cannot block the queue. ``try_acquire`` acquires only when no older live ticket from a different
  owner exists. Callers that take the raw ``flock`` without this helper do not see the queue and can
  still win the race; fairness holds among helper users.
- Files are rewritten in place (never unlinked or renamed), so a shared sticky directory such as
  ``/run/lock`` works across users who can write the files.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl  # windows-footgun: ok -- flock host locks are POSIX-only by design
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

DEFAULT_LOCK_DIR = "/run/lock"
DEFAULT_PREFIX = "hermes-host-lock-"
DEFAULT_TICKET_TTL = 3900.0
EXIT_QUEUED = 75  # EX_TEMPFAIL: the CLI's non-blocking run did not get the lock
_TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_FILE_MODE = 0o664


def _default_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _pid_alive(pid: Any) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        import psutil
    except ImportError:  # a foreign interpreter (project venv) without Hermes's deps
        try:
            os.kill(pid, 0)  # windows-footgun: ok -- POSIX-only module (fcntl.flock)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True
    return psutil.pid_exists(pid)


def lock_path(target: str, *, lock_dir: Optional[str] = None, prefix: Optional[str] = None) -> str:
    if not _TARGET_RE.match(target or ""):
        raise ValueError(f"invalid lock target {target!r}")
    prefix = DEFAULT_PREFIX if prefix is None else prefix
    if "/" in prefix:
        raise ValueError(f"invalid lock prefix {prefix!r}")
    return os.path.join(lock_dir or DEFAULT_LOCK_DIR, f"{prefix}{target}.lock")


def _open_rw(path: str) -> int:
    """Open (creating) *path* for an flock. Falls back to read-only: flock works on any fd."""
    try:
        return os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, _FILE_MODE)
    except PermissionError:
        return os.open(path, os.O_RDONLY | os.O_CLOEXEC)


def _read_fd(fd: int) -> str:
    os.lseek(fd, 0, os.SEEK_SET)
    chunks = []
    while chunk := os.read(fd, 65536):
        chunks.append(chunk)
    return b"".join(chunks).decode("utf-8", "replace")


def _write_fd(fd: int, text: str) -> None:
    data = text.encode("utf-8")
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


@dataclass
class Ticket:
    owner: str
    created: float
    expires: float
    pid: int = 0
    labels: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"owner": self.owner, "created": self.created, "expires": self.expires,
                "pid": self.pid, "labels": dict(self.labels)}

    @classmethod
    def from_dict(cls, raw: Any) -> Optional[Ticket]:
        try:
            return cls(owner=str(raw["owner"]), created=float(raw["created"]), expires=float(raw["expires"]),
                       pid=int(raw.get("pid", 0)), labels=dict(raw.get("labels") or {}))
        except (KeyError, TypeError, ValueError):
            return None


@dataclass
class Attempt:
    """Result of ``try_acquire``: ``acquired``, else ``position`` (1 = next in line) and the holder."""
    acquired: bool
    position: int = 0
    holder: Optional[dict[str, Any]] = None


class HostLock:
    """One target's exclusive lock, as held by one *owner* (a stable id the caller chooses)."""

    def __init__(self, target: str, *, owner: Optional[str] = None, lock_dir: Optional[str] = None,
                 prefix: Optional[str] = None, path: Optional[str] = None,
                 labels: Optional[dict[str, str]] = None, clock=time.time):
        self.target = target
        self.path = path or lock_path(target, lock_dir=lock_dir, prefix=prefix)
        self.owner = owner or _default_owner()
        self.labels = {str(k): str(v) for k, v in (labels or {}).items()}
        self._clock = clock
        self._fd: Optional[int] = None
        self._acquired_at: Optional[float] = None

    # ── paths ──────────────────────────────────────────────────────────────────────
    @property
    def holder_path(self) -> str:
        return self.path + ".holder"

    @property
    def queue_path(self) -> str:
        return self.path + ".queue"

    @property
    def held(self) -> bool:
        return self._fd is not None

    # ── queue ──────────────────────────────────────────────────────────────────────
    @contextlib.contextmanager
    def _queue(self, *, write: bool) -> Iterator[list[Ticket]]:
        """Yield the live (unexpired) tickets under the queue's flock; persist edits if *write*."""
        fd = _open_rw(self.queue_path)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if write else fcntl.LOCK_SH)
            try:
                raw = json.loads(_read_fd(fd) or "[]")
            except ValueError:
                raw = []
            now = self._clock()
            tickets = [t for t in (Ticket.from_dict(r) for r in (raw if isinstance(raw, list) else []))
                       if t is not None and t.expires > now]
            yield tickets
            if write:
                tickets.sort(key=lambda t: (t.created, t.owner))
                _write_fd(fd, json.dumps([t.as_dict() for t in tickets], indent=1))
        finally:
            os.close(fd)  # closing drops the flock

    def _upsert_ticket(self, tickets: list[Ticket], ttl: float) -> Ticket:
        now = self._clock()
        mine = next((t for t in tickets if t.owner == self.owner), None)
        if mine is None:
            mine = Ticket(owner=self.owner, created=now, expires=now + ttl, pid=os.getpid(), labels=self.labels)
            tickets.append(mine)
        else:
            mine.expires, mine.pid, mine.labels = now + ttl, os.getpid(), self.labels
        return mine

    @staticmethod
    def _ahead_of(tickets: list[Ticket], mine: Ticket) -> list[Ticket]:
        return [t for t in tickets if t.owner != mine.owner and (t.created, t.owner) < (mine.created, mine.owner)]

    def queue(self) -> list[dict[str, Any]]:
        """Live waiter tickets, oldest first."""
        with self._queue(write=False) as tickets:
            return [t.as_dict() for t in sorted(tickets, key=lambda t: (t.created, t.owner))]

    def enqueue(self, ttl: float = DEFAULT_TICKET_TTL) -> int:
        """Register or refresh this owner's ticket without trying the lock; returns its position."""
        with self._queue(write=True) as tickets:
            mine = self._upsert_ticket(tickets, ttl)
            return len(self._ahead_of(tickets, mine)) + 1

    def withdraw(self) -> None:
        """Drop this owner's ticket (a waiter giving up)."""
        with self._queue(write=True) as tickets:
            tickets[:] = [t for t in tickets if t.owner != self.owner]

    # ── acquire / release ──────────────────────────────────────────────────────────
    def try_acquire(self, ttl: float = DEFAULT_TICKET_TTL) -> Attempt:
        """Non-blocking. Acquire if no older live foreign ticket exists and the flock is free;
        otherwise keep (register/refresh) this owner's ticket and report its queue position.
        *ttl* should exceed the caller's retry interval, so a live retrier never expires."""
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        if self.held:
            return Attempt(acquired=True)
        with self._queue(write=True) as tickets:
            mine = self._upsert_ticket(tickets, ttl)
            ahead = self._ahead_of(tickets, mine)
            if not ahead:
                fd = _open_rw(self.path)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    os.close(fd)
                else:
                    tickets.remove(mine)
                    self._fd, self._acquired_at = fd, self._clock()
                    self._write_holder()
                    return Attempt(acquired=True)
        return Attempt(acquired=False, position=len(ahead) + 1, holder=self.holder())

    def acquire(self, *, timeout: Optional[float] = None, poll: float = 1.0,
                ttl: Optional[float] = None) -> bool:
        """Blocking acquire through the queue (keeps this owner's ticket fresh while it waits).
        Returns False on *timeout* (the ticket is withdrawn)."""
        ttl = ttl if ttl is not None else max(30.0, poll * 10)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self.try_acquire(ttl=ttl).acquired:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                self.withdraw()
                return False
            time.sleep(poll if deadline is None else max(0.0, min(poll, deadline - time.monotonic())))

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            self._clear_holder()
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def should_yield(self) -> bool:
        """True when a live ticket from a different owner is waiting (check at window boundaries)."""
        with self._queue(write=False) as tickets:
            return any(t.owner != self.owner for t in tickets)

    def yield_if_waiters(self, ttl: float = DEFAULT_TICKET_TTL) -> bool:
        """At a window boundary: if anyone else is queued, release and queue this owner at the back.
        Returns True when it yielded (schedule the next window and resume via ``try_acquire``)."""
        if not self.held or not self.should_yield():
            return False
        self.release()
        self.withdraw()  # a stale ticket of ours must not keep an old place
        self.enqueue(ttl)
        return True

    # ── holder metadata ────────────────────────────────────────────────────────────
    def _write_holder(self, extra: Optional[dict[str, Any]] = None) -> None:
        info = {"pid": os.getpid(), "host": socket.gethostname(), "owner": self.owner,
                "started_at": self._acquired_at, "labels": self.labels, **(extra or {})}
        try:
            fd = _open_rw(self.holder_path)
            try:
                _write_fd(fd, json.dumps(info))
            finally:
                os.close(fd)
        except OSError:
            pass  # metadata is best-effort; the flock is the mutex

    def _clear_holder(self) -> None:
        try:
            fd = os.open(self.holder_path, os.O_WRONLY | os.O_CLOEXEC)
        except OSError:
            return
        try:
            os.ftruncate(fd, 0)
        finally:
            os.close(fd)

    def set_holder_detail(self, **extra: Any) -> None:
        """Add fields (e.g. a child pid) to the holder record while held."""
        if self.held:
            self._write_holder(extra)

    def holder(self) -> Optional[dict[str, Any]]:
        """The current holder's metadata, or None (unheld, or held by a non-helper flock user)."""
        try:
            with open(self.holder_path, encoding="utf-8-sig") as fh:
                info = json.loads(fh.read() or "null")
        except (OSError, ValueError):
            return None
        if not isinstance(info, dict):
            return None
        if info.get("host") == socket.gethostname():
            # A holder killed without cleanup leaves its record behind (the kernel already dropped
            # the flock): flag it rather than report a dead process as the holder.
            info["stale"] = not any(_pid_alive(info.get(key)) for key in ("pid", "child_pid"))
        return info

    # ── scope ──────────────────────────────────────────────────────────────────────
    def __enter__(self) -> HostLock:
        if not self.held:
            self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def __del__(self):  # pragma: no cover - last-resort: never leak a held fd past the object
        with contextlib.suppress(Exception):
            self.release()


@contextlib.contextmanager
def host_lock(target: str, *, owner: Optional[str] = None, timeout: Optional[float] = None,
              poll: float = 1.0, ttl: Optional[float] = None, **kwargs: Any) -> Iterator[HostLock]:
    """Blocking scoped lock: ``with host_lock("db", owner="job") as lock: ...``. Raises
    ``TimeoutError`` if *timeout* elapses first."""
    lock = HostLock(target, owner=owner, **kwargs)
    if not lock.acquire(timeout=timeout, poll=poll, ttl=ttl):
        raise TimeoutError(f"host lock {target!r} not acquired within {timeout}s")
    try:
        yield lock
    finally:
        lock.release()


# ── CLI ────────────────────────────────────────────────────────────────────────────
def _run_child(lock: HostLock, cmd: list[str]) -> int:
    """Run *cmd* holding *lock*; the child inherits the locked fd, so the mutex outlives this
    wrapper if the wrapper dies first. Released here as soon as the child exits."""
    assert lock._fd is not None
    child = subprocess.Popen(cmd, pass_fds=(lock._fd,))
    lock.set_holder_detail(child_pid=child.pid, command=cmd[0])

    def forward(signum, _frame):
        with contextlib.suppress(ProcessLookupError):
            child.send_signal(signum)

    forwarded = [sig for sig in (getattr(signal, name, None) for name in ("SIGINT", "SIGTERM", "SIGHUP")) if sig]
    previous = {sig: signal.signal(sig, forward) for sig in forwarded}
    try:
        while True:
            try:
                return child.wait()  # health: allow HX006 -- the lock is held exactly as long as the child runs
            except InterruptedError:  # pragma: no cover - PEP 475 retries, kept for safety
                continue
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.host_lock", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p):
        p.add_argument("target")
        p.add_argument("--lock-dir", default=None, help=f"default {DEFAULT_LOCK_DIR}")
        p.add_argument("--prefix", default=None, help=f"lock file prefix (default {DEFAULT_PREFIX!r})")
        p.add_argument("--path", default=None, help="explicit lock file path (overrides dir/prefix)")

    run = sub.add_parser("run", help="run a command while holding the lock")
    common(run)
    run.add_argument("--owner", default=None, help="stable owner id (required for --no-wait retries)")
    run.add_argument("--label", action="append", default=[], metavar="KEY=VALUE")
    run.add_argument("--no-wait", action="store_true", help=f"try once; exit {EXIT_QUEUED} if queued")
    run.add_argument("--timeout", type=float, default=None, help="give up after N seconds (blocking mode)")
    run.add_argument("--ttl", type=float, default=None, help="ticket TTL seconds (retry interval + margin)")
    status = sub.add_parser("status", help="print holder and queue as JSON")
    common(status)
    argv = list(sys.argv[1:] if argv is None else argv)
    cmd: list[str] = []
    if "--" in argv:
        split = argv.index("--")
        argv, cmd = argv[:split], argv[split + 1:]
    args = parser.parse_args(argv)

    location = {"lock_dir": args.lock_dir, "prefix": args.prefix, "path": args.path}
    if args.command == "status":
        lock = HostLock(args.target, owner="status-reader", **location)
        print(json.dumps({"path": lock.path, "holder": lock.holder(), "queue": lock.queue()}, indent=1))
        return 0

    if not cmd:
        parser.error("run needs a command after --")
    labels = dict(item.split("=", 1) for item in args.label if "=" in item)
    lock = HostLock(args.target, owner=args.owner, labels=labels, **location)
    if args.no_wait:
        if not args.owner:
            parser.error("--no-wait needs --owner so a retry keeps its queue place")
        attempt = lock.try_acquire(ttl=args.ttl or DEFAULT_TICKET_TTL)
        if not attempt.acquired:
            print(json.dumps({"queued": True, "position": attempt.position, "holder": attempt.holder}),
                  file=sys.stderr)
            return EXIT_QUEUED
    elif not lock.acquire(timeout=args.timeout, ttl=args.ttl):
        print(json.dumps({"timeout": True, "holder": lock.holder()}), file=sys.stderr)
        return EXIT_QUEUED
    try:
        return _run_child(lock, cmd)
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
