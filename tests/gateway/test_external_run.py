"""Real canonical admission contracts for transient credentials and controller lifetime.

Only the inference/turn boundary is substituted: no model requests or real homes.
"""
import asyncio
from contextvars import copy_context
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tests.gateway.test_session_follow_profile_config import _owner
from tests.gateway.test_finite_unattended_wire_grant import _native, _rpc
from tests.waiting import async_wait_until


async def _fixture(monkeypatch):
    from gateway import run
    from hermes_constants import get_hermes_home
    home = get_hermes_home()
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(run, '_hermes_home', home)
    (home / 'config.yaml').write_text(json.dumps({
        'model': {'provider': 'custom', 'default': 'fixture', 'base_url': 'http://127.0.0.1:1/v1'},
        'gateway': {'external_run_max_seconds': 60},
        'terminal': {'run_env_passthrough': ['JOB_API_KEY', 'JOB_RUN_ID']}}))
    store, _runner, authority, dashboard = _owner(home)
    client = _native(authority, 'controller')
    created = await _rpc(client, 'session.create', request_id='fixture', source='oneshot', cwd=str(home), toolsets=[])
    assert 'result' in created, created
    return home, store, authority, client, dashboard, created['result']['session_id']


def _params(sid, request, value='opaque-run-credential-unique', seconds=60):
    return dict(session_id=sid, input_id=request, text='do work', finite=True,
                external_run={'seconds': seconds, 'env': {'JOB_API_KEY': value, 'JOB_RUN_ID': request}})


async def _idle(authority, sid):
    await async_wait_until(lambda: sid not in getattr(authority, '_external_runs', {}), timeout=10)


@pytest.mark.asyncio
async def test_tools_refresh_and_credentials_never_enter_admission_or_snapshot(monkeypatch):
    from agent.run_environment import run_values
    from agent.redact import redact_sensitive_text
    from tools.environments.local import build_subprocess_env
    from tools.code_execution_env import _scrub_child_env
    from hermes_state_runtime import list_session_admissions
    _home, store, authority, client, dashboard, sid = await _fixture(monkeypatch)
    observed, contexts = [], []

    async def execute(owner, ref, row):
        contexts.append(copy_context())
        values = run_values()
        # These are the real terminal and execute_code child-environment builders.
        terminal = build_subprocess_env()
        code = _scrub_child_env({})
        for env in (terminal, code):
            assert env['JOB_API_KEY'] == values['JOB_API_KEY']
            result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c',
                'import os;print(os.environ["JOB_RUN_ID"])'], env=env, capture_output=True, text=True, check=True)
            assert result.stdout.strip() == row['request_id']
        assert redact_sensitive_text(values['JOB_API_KEY']) == '«redacted-run-secret»'
        assert 'JOB_API_KEY' not in os.environ
        observed.append(values)
        return 'completed'

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        for request, value in [('first', 'opaque-run-credential-one'), ('second', 'opaque-run-credential-two')]:
            response = await _rpc(client, 'prompt.submit', **_params(sid, request, value))
            assert 'result' in response, response
            await _idle(authority, sid)
            assert run_values() == {}
            assert all(ctx.run(run_values) == {} for ctx in contexts)
            for ctx, old in zip(contexts, observed):
                assert ctx.run(redact_sensitive_text, old['JOB_API_KEY']) == '«redacted-run-secret»'
            rows = list_session_admissions(authority.db, session_id=sid, pending_only=False)
            snapshot = await _rpc(client, 'session.resume', session_id=sid)
            assert value not in json.dumps([rows, snapshot])
            assert value.encode() not in Path(authority.db.db_path).read_bytes()
        assert [v['JOB_RUN_ID'] for v in observed] == ['first', 'second']
        assert observed[0]['JOB_API_KEY'] != observed[1]['JOB_API_KEY']
    finally:
        await client.close()
        await dashboard.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_disconnect_interrupts_exact_claim_and_busy_replacement_is_refused(monkeypatch):
    from agent.run_environment import run_values

    _home, store, authority, client, dashboard, sid = await _fixture(monkeypatch)
    started, interrupted = asyncio.Event(), asyncio.Event()
    from types import SimpleNamespace
    agent = SimpleNamespace(hard_interrupt=lambda *a, **k: interrupted.set())
    captured = {}

    async def execute(owner, ref, row):
        captured.update(run_values())
        owner.adopt_agent(sid, row['generation'], agent)
        started.set()
        await interrupted.wait()
        return 'interrupted'

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        response = await _rpc(client, 'prompt.submit', **_params(sid, 'owned'))
        assert 'result' in response, response
        await asyncio.wait_for(started.wait(), 10)
        replacement = await _rpc(client, 'prompt.submit', **_params(sid, 'replacement', 'other-credential'))
        assert replacement['error']['message'] == 'session_busy', replacement
        assert captured['JOB_RUN_ID'] == 'owned'
        await client.close()
        await asyncio.wait_for(interrupted.wait(), 10)
        await _idle(authority, sid)
        from gateway.session_contract import SessionRef
        receipt = await authority.receipt(client.actor, SessionRef(authority.profile_id, sid),
                                          response['result']['admission_id'])
        assert receipt.status == 'terminal'
        assert not authority.executing
    finally:
        await dashboard.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_deadline_cancels_even_with_live_controller(monkeypatch):
    _home, store, authority, client, dashboard, sid = await _fixture(monkeypatch)
    interrupted = asyncio.Event()
    from types import SimpleNamespace
    async def execute(owner, ref, row):
        owner.adopt_agent(sid, row['generation'], SimpleNamespace(hard_interrupt=lambda *a, **k: interrupted.set()))
        await interrupted.wait()
        return 'deadline interrupted'
    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        response = await _rpc(client, 'prompt.submit', **_params(sid, 'bounded', seconds=1))
        assert 'result' in response, response
        await asyncio.wait_for(interrupted.wait(), 10)
        await _idle(authority, sid)
    finally:
        await client.close()
        await dashboard.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_settlement_and_completion_keep_redaction_without_tool_access(monkeypatch):
    from agent.run_environment import run_values
    from gateway.session_results import admission_result
    _home, store, authority, client, dashboard, sid = await _fixture(monkeypatch)
    value = 'opaque-settlement-fixture-credential'
    published = []
    stream = authority.sessions[sid].event_stream
    publish = stream.publish

    def observe(*args, **kwargs):
        published.append((args, kwargs))
        return publish(*args, **kwargs)

    async def execute(owner, ref, row):
        assert run_values()['JOB_API_KEY'] == value
        return value

    monkeypatch.setattr(stream, 'publish', observe)
    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)
    try:
        response = await _rpc(client, 'prompt.submit', **_params(sid, 'settlement', value))
        assert 'result' in response, response
        await _idle(authority, sid)
        saved = admission_result(authority.db, response['result']['admission_id'])
        assert saved is not None
        assert saved['result']['final_response'] == '«redacted-run-secret»'
        assert published
        assert value not in json.dumps([saved, published])
        assert run_values() == {}
    finally:
        await client.close()
        await dashboard.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_explicit_profile_optin_and_local_owner_required(monkeypatch):
    home, store, _authority, client, dashboard, sid = await _fixture(monkeypatch)
    try:
        for params in [dict(_params(sid, 'too-long'), external_run={'seconds': 61, 'env': {}}),
                       dict(_params(sid, 'unlisted'), external_run={'seconds': 60, 'env': {'OTHER_API_KEY': 'no'}})]:
            response = await _rpc(client, 'prompt.submit', **params)
            assert response['error']['message'] == 'invalid_params', response
        await _rpc(dashboard, 'session.resume', session_id=sid)
        response = await _rpc(dashboard, 'prompt.submit', **_params(sid, 'dashboard'))
        assert response['error']['message'] == 'permission_denied', response
        from agent.run_environment import RunEnvironment, run_environment_scope, run_values
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        with run_environment_scope(RunEnvironment(str(home.resolve()), 'run', {'JOB_API_KEY': 'opaque'})):
            assert run_values()['JOB_API_KEY'] == 'opaque'
            token = set_hermes_home_override(home / 'other-profile')
            try:
                assert run_values() == {}
            finally:
                reset_hermes_home_override(token)
    finally:
        await client.close()
        await dashboard.close()
        store.close_all_db_handles()
