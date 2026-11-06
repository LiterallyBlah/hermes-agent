"""Names-only CLI selection for a finite externally owned gateway admission."""
import os

from hermes_cli.gateway_client import GatewayClientError


def use_transport_chat(args):
    from hermes_cli.stream_json import stream_json_requested
    return (stream_json_requested(args) or getattr(args, 'owned_run_seconds', None) is not None
            or bool(getattr(args, 'run_env', None)))


def launch_run_environment(args, *, finite):
    seconds = getattr(args, 'owned_run_seconds', None)
    names = getattr(args, 'run_env', None) or []
    if seconds is None and not names:
        return None
    if type(seconds) is not int or not 0 < seconds <= 86400 or not finite:
        raise GatewayClientError('--run-env requires a finite --owned-run-seconds run (1..86400)')
    from agent.run_environment import configured_names, validate_environment
    from hermes_cli.config import get_config_path
    from hermes_cli.config_effective import load_user_config_effective
    config = load_user_config_effective(get_config_path())
    try:
        allowed = configured_names(config)
        if any(name not in allowed for name in names):
            raise ValueError('run environment name not allowed')
        values = validate_environment({name: os.environ[name] for name in names if name in os.environ}, config)
    except ValueError:
        raise GatewayClientError('Run environment refused by terminal.run_env_passthrough') from None
    return {'seconds': seconds, 'env': values}
