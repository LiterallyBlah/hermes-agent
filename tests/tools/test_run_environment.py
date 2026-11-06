"""Transient grants through the registered shell, including reused snapshots."""
import hashlib
import json
from pathlib import Path
import shlex
import sys

import pytest


@pytest.mark.platforms('linux')
def test_single_profile_shell_never_snapshots_or_resurrects_run_grant(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_YOLO_MODE', '1')
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    from hermes_constants import get_hermes_home
    from agent.run_environment import RunEnvironment, run_environment_scope
    from agent.secret_scope import is_multiplex_active, set_multiplex_active
    from tools.registry import registry
    from tools import terminal_tool
    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    (home / 'config.yaml').write_text(json.dumps({'terminal': {
        'backend': 'local', 'run_env_passthrough': ['JOB_API_KEY']}}))
    command = shlex.join([sys.executable, '-c',
        "import os,hashlib;print(hashlib.sha256(os.environ.get('JOB_API_KEY','').encode()).hexdigest())"])
    multiplex = is_multiplex_active()
    set_multiplex_active(False)

    def dispatch():
        return registry.dispatch('terminal', {'command': command, 'workdir': str(home)}, task_id='same-session')

    def digest(value):
        return hashlib.sha256(value.encode()).hexdigest()

    try:
        for name in ('first', 'replacement'):
            value = 'opaque-fixture-credential-' + name
            run = RunEnvironment(str(home.resolve()), name, {'JOB_API_KEY': value})
            with run_environment_scope(run):
                assert digest(value) in dispatch()
                snapshots = [Path(env._snapshot_path) for env in terminal_tool._active_environments.values()]
                assert snapshots
                assert all(value not in p.read_text() for p in snapshots)
                # Simulate a snapshot captured before this name became transient.
                with snapshots[0].open('a') as snapshot:
                    snapshot.write('declare -x JOB_API_KEY=stale-fixture-value\n')
                assert digest(value) in dispatch()
                assert 'stale-fixture-value' not in snapshots[0].read_text()
                run.revoke()
            assert digest('') in dispatch()
            assert all(value not in p.read_text() for p in snapshots)
    finally:
        terminal_tool.cleanup_all_environments()
        set_multiplex_active(multiplex)


def test_redaction_only_scope_never_restores_grant_or_crosses_profile(tmp_path):
    from contextvars import copy_context
    from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
    from agent.run_environment import (RunEnvironment, run_environment_scope, run_redaction_scope,
                                       run_values, redact_run_values)
    home = str(get_hermes_home().resolve())
    first = RunEnvironment(home, 'old', {'JOB_API_KEY': 'opaque-old-fixture'})
    settling = RunEnvironment(home, 'new', {'JOB_API_KEY': 'opaque-new-fixture'})
    with run_environment_scope(first):
        late = copy_context()
        first.revoke()
        with run_redaction_scope(settling):
            assert run_values() == {}
            assert redact_run_values('opaque-old-fixture opaque-new-fixture') == (
                '«redacted-run-secret» «redacted-run-secret»')
            token = set_hermes_home_override(tmp_path / 'other-profile')
            try:
                assert run_values() == {}
                assert redact_run_values('opaque-old-fixture opaque-new-fixture') == (
                    'opaque-old-fixture opaque-new-fixture')
            finally:
                reset_hermes_home_override(token)
    assert late.run(run_values) == {}
    assert late.run(redact_run_values, 'opaque-old-fixture') == '«redacted-run-secret»'
    assert redact_run_values('opaque-new-fixture') == 'opaque-new-fixture'
