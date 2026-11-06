"""Opt-in local controller ownership of a single finite admission.

Only a non-secret marker enters the ledger. The authenticated socket owns a
revocable environment and a deadline in memory; ordinary disconnect still detaches.
"""
import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
import uuid

from agent.run_environment import RunEnvironment, run_environment_scope, run_redaction_scope, validate_environment
from hermes_state_runtime import RuntimeStoreError


@dataclass(repr=False)
class ExternalRun:
    environment: RunEnvironment
    connection: object
    ref: object
    request_id: str
    seconds: int
    disconnected: asyncio.Event = field(default_factory=asyncio.Event)
    receipt: object = None
    monitor: object = None


def _runs(authority):
    return authority.__dict__.setdefault('_external_runs', {})


def _validate(connection, ref, params):
    from gateway.session_finite import _may_run_unattended
    from gateway.run import _load_gateway_config
    raw = params['external_run']
    if (not _may_run_unattended(connection, ref) or params.get('finite') is not True
            or not isinstance(raw, dict) or set(raw) != {'seconds', 'env'}):
        raise RuntimeStoreError('permission_denied')
    home = Path(connection.authority.db.db_path).resolve().parent
    config = _load_gateway_config(home / 'config.yaml')
    from gateway.session_policy import policy_for_source
    policy = policy_for_source(connection.authority.runner, connection.authority.sessions[ref.session_id].source)
    if (policy.safe_mode or policy.ignore_user_config
            or (config.get('terminal') or {}).get('backend', 'local') != 'local'):
        raise RuntimeStoreError('invalid_params', field='external_run')
    maximum = (config.get('gateway') or {}).get('external_run_max_seconds', 0)
    seconds = raw['seconds']
    if (type(maximum) is not int or not 0 < maximum <= 86400
            or type(seconds) is not int or not 0 < seconds <= maximum):
        raise RuntimeStoreError('invalid_params', field='external_run')
    try:
        values = validate_environment(raw['env'], config)
    except ValueError:
        raise RuntimeStoreError('invalid_params', field='external_run') from None
    return RunEnvironment(str(home), uuid.uuid4().hex, values), seconds


async def submit(connection, ref, params):
    authority = connection.authority
    authority.authorize(connection.actor, ref, 'session:control')
    environment, seconds = _validate(connection, ref, params)
    request = connection.submission(ref, {k: v for k, v in params.items() if k != 'external_run'})
    authority.authorize(connection.actor, ref, 'session:submit')
    async with authority.sessions[ref.session_id].mutation_lock:
        authority.authorize(connection.actor, ref, 'session:submit')
        handle = authority._handle(ref)
        # No queueing behind another run, and no replacement of a grant while a
        # native worker is still unwinding, even if the logical claim has settled.
        from hermes_state_runtime import list_session_admissions
        if (ref.session_id in _runs(authority) or handle.execution_state != 'idle'
                or list_session_admissions(authority.db, session_id=ref.session_id)):
            raise RuntimeStoreError('session_busy')
        run = ExternalRun(environment, connection, ref, request.request_id, seconds)
        _runs(authority)[ref.session_id] = run
        connection.__dict__.setdefault('_external_runs', []).append(run)
        # Shield admission itself, not just the SQLite commit: disconnect between
        # commit and receipt must still attach the native cancellation monitor.
        from gateway.session_runtime_workers import track_mutation
        async def admit():
            try:
                run.receipt = await authority._submit(connection.actor, request, _external_run=True)
            except BaseException:
                environment.revoke()
                _runs(authority).pop(ref.session_id, None)
                connection._external_runs.remove(run)
                raise
            run.monitor = track_mutation(authority, _monitor(run))
            return run.receipt
        return await asyncio.shield(track_mutation(authority, admit()))


def _physically_running(authority, sid):
    from gateway.session_runtime_workers import physical_workers
    return any(worker.task_id == sid for worker, _ in physical_workers(authority))


async def _monitor(run):
    authority, ref = run.connection.authority, run.ref
    deadline = asyncio.get_running_loop().time() + run.seconds
    stopped = False
    try:
        while True:
            receipt = await authority.receipt(run.connection.actor, ref, run.receipt.admission_id)
            if receipt.status == 'terminal' and not _physically_running(authority, ref.session_id):
                return
            if not stopped and (run.disconnected.is_set() or asyncio.get_running_loop().time() >= deadline):
                # Native cancel covers queued -> claimed races, then interrupts only
                # that receipt's execution_generation (never the next session turn).
                await run.connection.cancel(ref, {'admission_id': receipt.admission_id})
                stopped = True
                run.environment.revoke()
            await asyncio.sleep(.05)
    finally:
        run.environment.revoke()
        if not _physically_running(authority, ref.session_id):
            if _runs(authority).get(ref.session_id) is run:
                del _runs(authority)[ref.session_id]
            run.connection._external_runs.remove(run)


def disconnect(connection):
    for run in getattr(connection, '_external_runs', ()):
        run.disconnected.set()


@contextmanager
def admission_redaction_scope(authority, ref, row):
    run = _runs(authority).get(ref.session_id)
    environment = (run.environment if run is not None and run.request_id == row['request_id']
                   and row['payload'].get('external_run_v1') is True else None)
    with run_redaction_scope(environment):
        yield


@asynccontextmanager
async def execution_scope(authority, ref, row):
    run = _runs(authority).get(ref.session_id)
    marked = row['payload'].get('external_run_v1') is True
    if not marked:
        with run_environment_scope(None):
            yield
        return
    if run is None or run.request_id != row['request_id']:
        raise RuntimeStoreError('launch_credentials_unavailable')
    from tools.process_registry import process_registry
    baseline = process_registry.snapshot_running_ids(ref.session_id)
    with run_environment_scope(run.environment):
        try:
            yield
        finally:
            run.environment.revoke()
            # Kernels retain os.environ between cells; never resume one with a
            # previous run's credentials, even on this same persisted session.
            from tools.code_kernel import _REGISTRY
            from tools.code_kernel_remote import shutdown_remote_kernels_where
            suffix = '::run::' + run.environment.identity
            matcher = lambda owner: owner.endswith(suffix)
            await asyncio.to_thread(_REGISTRY.shutdown, owner_matches=matcher)
            await asyncio.to_thread(shutdown_remote_kernels_where, matcher)
            await asyncio.to_thread(process_registry.kill_started_since, ref.session_id, baseline,
                                    source='external_run_complete')
