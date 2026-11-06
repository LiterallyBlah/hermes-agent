"""Owned runs through the real daemon, CLI, socket, agent and registered tools.

Only inference is replaced, by a loopback OpenAI-compatible scripted provider.
"""
import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import threading

import psutil
import pytest

from tests.gateway.fixtures.local_recovery_probe import child_env, daemon, rpc, websocket
from tests.waiting import async_wait_until


class ScriptedModel(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/api/show':
            self.send_error(404)
            return
        self.server.requests.append(body)
        messages = body.get('messages', [])
        starts = [i for i, m in enumerate(messages) if m['role'] == 'user']
        assert starts, {'path': self.path, 'payload': body}
        start = max(starts)
        text = messages[start]['content']
        results = [m for m in messages[start + 1:] if m['role'] == 'tool']
        calls = self.server.scripts[text]
        if len(results) < len(calls):
            name, args = calls[len(results)]
            message = {'role': 'assistant', 'content': None, 'tool_calls': [{
                'id': f'call_{len(results)}', 'type': 'function',
                'function': {'name': name, 'arguments': json.dumps(args)}}]}
            finish = 'tool_calls'
        else:
            message, finish = {'role': 'assistant', 'content': 'TOOLS_COMPLETED'}, 'stop'
        payload = {'id': 'fixture', 'choices': [{'index': 0, 'message': message, 'finish_reason': finish}],
                   'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}}
        kind = 'application/json'
        if body.get('stream'):
            choice = payload['choices'][0]
            choice['delta'] = choice.pop('message')
            payload = ('data: ' + json.dumps(payload) + '\n\ndata: [DONE]\n\n').encode()
            kind = 'text/event-stream'
        else:
            payload = json.dumps(payload).encode()
        try:
            self.send_response(200)
            self.send_header('Content-Type', kind)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


@contextmanager
def _runtime(tmp_path, scripts):
    root = Path(__file__).resolve().parents[2]
    home, user = tmp_path / 'state', tmp_path / 'user'
    home.mkdir(mode=0o700)
    user.mkdir()
    peer = ThreadingHTTPServer(('127.0.0.1', 0), ScriptedModel)
    peer.requests, peer.scripts = [], scripts
    thread = threading.Thread(target=peer.serve_forever, daemon=True)
    thread.start()
    url = f'http://127.0.0.1:{peer.server_port}/v1'
    (home / 'config.yaml').write_text(json.dumps({
        'gateway': {'multiplex_profiles': False, 'external_run_max_seconds': 60},
        'model': {'provider': 'custom', 'default': 'fixture', 'base_url': url},
        'terminal': {'backend': 'local', 'run_env_passthrough': ['JOB_API_KEY', 'JOB_RUN_ID']},
        'auxiliary': {'title_generation': {'enabled': False}},
        'memory': {'memory_enabled': False, 'user_profile_enabled': False},
        'platform_toolsets': {'cli': ['terminal', 'code_execution']}}))
    env = child_env() | dict(HOME=str(user), USERPROFILE=str(user), HERMES_HOME=str(home),
                             PYTHONPATH=str(root), OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL=url,
                             PYTHONUNBUFFERED='1', HERMES_YOLO_MODE='1')
    try:
        with daemon(root, home, env, barrier=False) as (_, desc):
            yield root, home, env, peer, desc
    finally:
        peer.shutdown()
        peer.server_close()
        thread.join(timeout=5)


def _rows(home):
    with sqlite3.connect(f'file:{home / "state.db"}?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute('SELECT * FROM session_admissions')]


def _live(pid):
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def _terminal(code):
    return ('terminal', {'command': shlex.join([sys.executable, '-c', code]), 'timeout': 300})


@pytest.mark.platforms('linux')
def test_registered_tools_refresh_persistent_kernel_and_redact(tmp_path):
    proof = tmp_path / 'proof'
    code = ("import os,json; from pathlib import Path; "
            f"p=Path({str(proof)!r}); p.mkdir(exist_ok=True); "
            "p.joinpath(os.environ['JOB_RUN_ID']+'-terminal').write_text(os.environ['JOB_API_KEY']); "
            "print(os.environ['JOB_API_KEY'])")
    first = ("assert 'counter' not in globals(); import os; from pathlib import Path; counter=1; "
             f"Path({str(proof)!r}).joinpath(os.environ['JOB_RUN_ID']+'-kernel').write_text(os.environ['JOB_API_KEY']); "
             "print(os.environ['JOB_API_KEY'])")
    second = "counter += 1; print('PERSISTENT_COUNTER', counter); print('KERNEL_PID', os.getpid())"
    scripts = {name: [_terminal(code), ('execute_code', {'code': first}),
                      ('execute_code', {'code': second})] for name in ('first', 'second')}

    async def exercise(home, desc, peer):
        async with websocket(home, desc) as ws:
            created = await rpc(ws, 'session.create', request_id='tools', source='oneshot', cwd=str(home),
                                toolsets=['terminal', 'code_execution'])
            sid = created['result']['session_id']
            for name in scripts:
                value = 'opaque-credential-for-' + name
                # A terminal receipt/kernel exit can precede the ownership monitor's
                # final release. Only retry its documented busy refusal, without
                # committing a second admission while the first is being reaped.
                previous_count = len(_rows(home))

                def attempt():
                    return asyncio.create_task(rpc(ws, 'prompt.submit', session_id=sid,
                        input_id=name, text=name, finite=True,
                        external_run={'seconds': 60, 'env': {'JOB_API_KEY': value, 'JOB_RUN_ID': name}}))

                pending = attempt()

                def accepted():
                    nonlocal pending
                    if not pending.done():
                        return False
                    response = pending.result()
                    if 'error' not in response:
                        return response
                    assert previous_count and response['error']['message'] == 'session_busy', response
                    assert len(_rows(home)) == previous_count
                    pending = attempt()
                    return False

                try:
                    submitted = await async_wait_until(accepted, timeout=15,
                        what='previous owned admission fully released')
                finally:
                    if not pending.done():
                        pending.cancel()
                        await asyncio.gather(pending, return_exceptions=True)
                assert isinstance(submitted, dict) and 'result' in submitted, submitted
                await async_wait_until(lambda: all(r['status'] == 'terminal' for r in _rows(home)), timeout=60)
                assert _rows(home)[-1]['outcome'] == 'completed', _rows(home)
                assert proof.exists(), {'requests': peer.requests, 'gateway': (home / 'restart.log').read_text()}
                assert (proof / (name + '-terminal')).read_text() == value
                assert (proof / (name + '-kernel')).read_text() == value
                snapshot = await rpc(ws, 'session.resume', session_id=sid)
                assert value not in json.dumps(snapshot), snapshot
                # A fresh run may not reuse the prior kernel or its namespace.
                tools = [m['content'] for r in peer.requests for m in r['messages'] if m['role'] == 'tool']
                assert any('PERSISTENT_COUNTER 2' in str(t) for t in tools), tools
                assert value not in json.dumps(peer.requests)
                assert value.encode() not in (home / 'state.db').read_bytes()
                assert value not in (home / 'restart.log').read_text()
                # The completion event can precede final owner cleanup; a native busy
                # refusal is expected until the old run's physical resources are gone.
                await async_wait_until(lambda: not any(_live(int(t.split('KERNEL_PID ')[1].split('\\n')[0]))
                    for t in tools if 'KERNEL_PID ' in t), timeout=15)
            assert len({r['target_session_id'] for r in _rows(home)}) == 1

    with _runtime(tmp_path, scripts) as (_, home, _, peer, desc):
        asyncio.run(exercise(home, desc, peer))


@pytest.mark.platforms('linux')
@pytest.mark.parametrize('ending', ['terminate', 'kill', 'socket', 'detach'])
@pytest.mark.parametrize('tool', ['terminal', 'execute_code'])
def test_controller_death_cancels_physical_tool_but_ordinary_detach_does_not(tmp_path, ending, tool):
    pidfile, release = tmp_path / 'worker.pid', tmp_path / 'release'
    # File release allows the ordinary detach control to finish without a timer race.
    code = ("import os,time; from pathlib import Path; "
            f"Path({str(pidfile)!r}).write_text(str(os.getpid())); "
            f"exec(\"while not Path({str(release)!r}).exists(): time.sleep(.05)\")")
    call = _terminal(code) if tool == 'terminal' else (tool, {'code': code})

    async def exercise(root, home, env, desc):
        cli = None
        ws = await websocket(home, desc).__aenter__()
        try:
            if ending == 'socket':
                created = await rpc(ws, 'session.create', request_id='socket', source='oneshot', cwd=str(home),
                                    toolsets=['terminal', 'code_execution'])
                response = await rpc(ws, 'prompt.submit', session_id=created['result']['session_id'],
                                     input_id='blocked', text='blocked', finite=True,
                                     external_run={'seconds': 60, 'env': {}})
                assert 'result' in response, response
            else:
                command = [sys.executable, '-m', 'hermes_cli.main', 'chat', '-q', 'blocked',
                           '--toolsets', 'terminal,code_execution']
                if ending != 'detach':
                    command += ['--owned-run-seconds', '60']
                else:
                    command += ['--format', 'stream-json']
                cli = await asyncio.to_thread(subprocess.Popen, command, cwd=root, env=env,
                                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            await async_wait_until(pidfile.exists, timeout=60, what='registered tool worker started')
            pid = int(pidfile.read_text())
            assert _live(pid)
            if ending == 'socket':
                ws.transport.abort()
            else:
                getattr(cli, 'terminate' if ending == 'detach' else ending)()
                await asyncio.to_thread(cli.wait, 10)
            if ending == 'detach':
                # A second socket confirms the admission remains claimed and the
                # physical worker remains alive after the original CLI is gone.
                async with websocket(home, desc) as observer:
                    sid = _rows(home)[0]['target_session_id']
                    snapshot = await rpc(observer, 'session.resume', session_id=sid)
                    assert snapshot['result']['running'], snapshot
                    assert _rows(home)[0]['status'] == 'started', _rows(home)
                    assert _live(pid)
                release.touch()
            await async_wait_until(lambda: not _live(pid) if ending != 'detach' else
                                   all(r['status'] == 'terminal' for r in _rows(home)), timeout=30,
                                   what='physical tool stopped or ordinary detached work completed')
            await async_wait_until(lambda: all(r['status'] == 'terminal' for r in _rows(home)), timeout=20)
            assert _rows(home)[0]['outcome'] == ('completed' if ending == 'detach' else 'interrupted'), _rows(home)
        finally:
            release.touch()
            await ws.close()
            if cli is not None:
                if cli.poll() is None:
                    cli.kill()
                out, err = await asyncio.to_thread(cli.communicate, timeout=10)
                if not pidfile.exists():
                    pytest.fail(f'CLI never started tool: {cli.returncode}\n{out.decode()}\n{err.decode()}')

    with _runtime(tmp_path, {'blocked': [call]}) as (root, home, env, _, desc):
        asyncio.run(exercise(root, home, env, desc))


@pytest.mark.platforms('linux')
def test_names_only_cli_forwards_selected_environment_to_real_tool(tmp_path):
    proof = tmp_path / 'cli-proof'
    value = 'opaque-cli-grant-fixture'
    code = ("import os;from pathlib import Path;"
            f"Path({str(proof)!r}).write_text(os.environ['JOB_API_KEY']);"
            "print(os.environ['JOB_API_KEY'])")
    with _runtime(tmp_path, {'cli-grant': [_terminal(code)]}) as (root, home, env, peer, _desc):
        command = [sys.executable, '-m', 'hermes_cli.main', 'chat', '-q', 'cli-grant',
                   '--toolsets', 'terminal', '--owned-run-seconds', '60', '--run-env', 'JOB_API_KEY']
        result = subprocess.run(command, cwd=root, env=env | {'JOB_API_KEY': value},
                                capture_output=True, text=True, timeout=60, check=False)
        assert result.returncode == 0, result.stdout + result.stderr
        assert proof.read_text() == value
        assert json.loads(_rows(home)[0]['payload_json'])['external_run_v1'] is True
        assert value not in json.dumps([command, peer.requests, result.stdout, result.stderr])
        assert value not in (home / 'restart.log').read_text()
