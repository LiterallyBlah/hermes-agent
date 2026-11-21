"""execute_code kernels are scoped to the agent run that used them.

A kernel that took an ``flock`` (or any other process-held resource) used to keep it after the run
ended, until ``kernel_idle_timeout`` reaped the idle kernel. The run boundary
(``agent.conversation_loop.run_conversation`` → ``tools.code_kernel.run_scope``) now disposes the
run's kernels on every exit path, while a kernel another live run of the same owner uses survives.
"""

import json
import subprocess
import sys
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from tools import code_kernel
from tools.approval_context import reset_current_session_key, set_current_session_key
from tools.code_execution_tool import execute_code
from tools.code_kernel import _KERNELS, run_scope, shutdown_all_kernels


@pytest.fixture(autouse=True)
def _local_backend(monkeypatch):
    monkeypatch.setenv("TERMINAL_ENV", "local")
    shutdown_all_kernels()
    yield
    shutdown_all_kernels()
    code_kernel._LIVE_RUNS.clear()


@contextmanager
def _config(**overrides):
    config = {"mode": "strict", "timeout": 30, **overrides}
    with patch("tools.code_execution_tool._load_config", return_value=config):
        yield


@contextmanager
def _session(key):
    token = set_current_session_key(key)
    try:
        yield
    finally:
        reset_current_session_key(token)


def _cell(code, task_id="turn"):
    result = json.loads(execute_code(code, task_id=task_id))
    assert result["status"] == "success", result
    return result


def _take_flock_in_kernel(lock_file):
    # The kernel keeps the fd in its globals: exactly the idle-retention shape (a raw fcntl.flock).
    _cell(f"import fcntl, os\n_fd = os.open({str(lock_file)!r}, os.O_RDWR | os.O_CREAT)\n"
          "fcntl.flock(_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\nprint('locked')")


def _free_for_another_process(lock_file) -> bool:
    probe = ("import fcntl, os, sys\nfd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT)\n"
             "try:\n    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\nexcept BlockingIOError:\n    sys.exit(1)\n")
    return subprocess.run([sys.executable, "-c", probe, str(lock_file)], timeout=30, check=False).returncode == 0


def _kernel_procs():
    return [k.proc for k in _KERNELS.values() if k.proc is not None]


def test_run_end_releases_a_kernel_held_flock_immediately(tmp_path):
    lock_file = tmp_path / "admin.lock"
    with _config(), _session("conv-run-end"):
        with run_scope():
            _take_flock_in_kernel(lock_file)
            procs = _kernel_procs()
            assert procs and not _free_for_another_process(lock_file)
        # No idle sweep: the run boundary itself disposed the kernel.
        assert _KERNELS == {}
        for proc in procs:
            proc.wait(timeout=10)
        assert _free_for_another_process(lock_file)


def test_state_persists_within_a_run(tmp_path):
    with _config(), _session("conv-within"):
        with run_scope():
            _cell("x = 41")
            second = _cell("print(x + 1)")
    assert "42" in second["output"] and second["kernel"]["reused"] is True


def test_kernel_used_by_another_live_run_is_kept(tmp_path):
    """Two overlapping runs of one owner: the first ending must not kill the kernel the second uses."""
    lock_file = tmp_path / "admin.lock"
    second_started, first_done, release_second = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def second_run():
        try:
            with _session("conv-shared"), run_scope():
                _cell("y = 1", task_id="turn-b")
                second_started.set()
                release_second.wait(30)
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    with _config():
        with _session("conv-shared"):
            with run_scope():
                _take_flock_in_kernel(lock_file)
                worker = threading.Thread(target=second_run)
                worker.start()
                assert second_started.wait(30)
            first_done.set()
        # Run A ended, run B is live and used the same kernel: kernel and lock survive.
        assert len(_KERNELS) == 1 and all(proc.poll() is None for proc in _kernel_procs())
        assert not _free_for_another_process(lock_file)
        procs = _kernel_procs()
        release_second.set()
        worker.join(30)
        assert not errors
        assert _KERNELS == {}
        for proc in procs:
            proc.wait(timeout=10)
        assert _free_for_another_process(lock_file)


def test_outside_a_run_lifetime_is_unchanged():
    with _config(), _session("conv-no-run"):
        _cell("z = 3")
        assert len(_KERNELS) == 1
        assert "3" in _cell("print(z)")["output"]


def test_session_lifetime_opt_out_keeps_the_kernel_across_runs(tmp_path):
    with _config(kernel_lifetime="session"), _session("conv-opt-out"):
        with run_scope():
            _cell("w = 5")
        assert len(_KERNELS) == 1
        with run_scope():
            assert "5" in _cell("print(w)")["output"]


def _fake_agent():
    return SimpleNamespace()


@pytest.mark.parametrize("outcome", ["completed", "raised"])
def test_run_conversation_disposes_the_runs_kernels_on_every_exit(tmp_path, outcome):
    """The real turn boundary is wired: a turn that took a lock in its kernel and then returned or
    raised leaves no kernel and no held lock behind."""
    from agent import conversation_loop

    lock_file = tmp_path / "admin.lock"

    def turn(agent, user_message, **kwargs):
        _take_flock_in_kernel(lock_file)
        assert not _free_for_another_process(lock_file)
        if outcome == "raised":
            raise KeyboardInterrupt("interrupted mid-turn")
        return {"completed": True, "messages": []}

    with _config(), _session("conv-turn"), \
            patch.object(conversation_loop, "_run_conversation_turn", turn), \
            patch("agent.turn_finalizer.settle_held_final", lambda *a, **k: None), \
            patch("agent.turn_context.export_current_turn_boundary", lambda agent, result, msg: result), \
            patch.object(conversation_loop, "_close_durable_failed_turn", lambda *a, **k: None), \
            patch("agent.voice_turn_route.end_voice_turn_route", lambda *a, **k: None):
        if outcome == "raised":
            with pytest.raises(KeyboardInterrupt):
                conversation_loop.run_conversation(_fake_agent(), "hi")
        else:
            conversation_loop.run_conversation(_fake_agent(), "hi")
    assert _KERNELS == {}
    assert _free_for_another_process(lock_file)
    assert code_kernel.live_run_count("conv-turn") == 0
