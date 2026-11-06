"""Revocable, memory-only environment for one externally owned admission's tools.

Not a client-environment policy or a profile secret source. A copied context holds
this same revocable object, so a late callback cannot retain a completed run's grant.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import re
from typing import Any

_BOUND = ContextVar['RunEnvironment | None']('run_environment', default=None)
_REDACTION = ContextVar['RunEnvironment | None']('run_redaction', default=None)
_NAME = re.compile(r'[A-Z][A-Z0-9_]{0,127}')


def configured_names(config):
    from tools.env_passthrough import _is_hermes_provider_credential
    terminal = config.get('terminal') or {}
    names = terminal.get('run_env_passthrough') or []
    if not isinstance(names, list):
        raise ValueError('invalid run environment policy')
    accepted = set()
    for name in names:
        if (not isinstance(name, str) or not _NAME.fullmatch(name)
                or name.startswith(('HERMES_', 'TERMINAL_', 'PYTHON', 'LD_', 'DYLD_'))
                or name in {'PATH', 'HOME', 'SHELL', 'BASH_ENV', 'ENV', 'NODE_OPTIONS'}
                or _is_hermes_provider_credential(name)):
            raise ValueError('invalid run environment policy')
        accepted.add(name)
    return frozenset(accepted)


def validate_environment(values, config):
    names = configured_names(config)
    if (not isinstance(values, dict) or len(values) > 64
            or any(not isinstance(k, str) or k not in names or not isinstance(v, str)
                   or '\x00' in v for k, v in values.items())
            or sum(len(k.encode()) + len(v.encode()) for k, v in values.items()) > 65536):
        raise ValueError('invalid run environment')
    return dict(values)


@dataclass(repr=False)
class RunEnvironment:
    home: str
    identity: str
    values: dict = field(repr=False)
    _redaction_values: frozenset = field(init=False, repr=False)

    def __post_init__(self):
        from tools.code_execution_env import is_secret_env_name
        self._redaction_values = frozenset(v for k, v in self.values.items() if v and is_secret_env_name(k))

    def revoke(self):
        # Copied contexts lose tool access immediately, but can still emit late
        # output. Keep their redaction material until the owning contexts die.
        self.values.clear()


def current_run_environment():
    from hermes_constants import get_hermes_home
    bound = _BOUND.get()
    return bound if bound is not None and str(get_hermes_home().resolve()) == bound.home else None


def run_values():
    bound = current_run_environment()
    return {} if bound is None else dict(bound.values)


@contextmanager
def run_environment_scope(environment):
    token = _BOUND.set(environment)
    try:
        yield
    finally:
        _BOUND.reset(token)


@contextmanager
def run_redaction_scope(environment):
    """Keep late settlement/output redaction without granting tool access."""
    token = _REDACTION.set(environment)
    try:
        yield
    finally:
        _REDACTION.reset(token)


def redact_run_values(text):
    from hermes_constants import get_hermes_home
    home = str(get_hermes_home().resolve())
    values = {value for bound in (current_run_environment(), _REDACTION.get())
              if bound is not None and bound.home == home for value in bound._redaction_values}
    for value in sorted(values, key=len, reverse=True):
        text = text.replace(value, '«redacted-run-secret»')
    return text


def redact_run_payload(value) -> Any:
    if isinstance(value, str):
        return redact_run_values(value)
    if isinstance(value, dict):
        return {k: redact_run_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_run_payload(v) for v in value]
    return value
