"""The admitted turn's captured usage carries per-turn cache deltas alongside input/output.

``input_tokens`` stays the whole prompt (input + cache read + cache write, as the agent's
``session_prompt_tokens`` counts it) and ``total_tokens`` stays input + output, so a consumer
that derives uncached input as ``input_tokens - cache_read_tokens`` keeps working.
"""
from types import SimpleNamespace

from gateway.run_turn_runner import TurnRunner
from gateway.session_results import execution_result
from gateway.turn_context import TurnContext


def _turn():
    return TurnRunner(SimpleNamespace(session_authority=None, _consume_pending_native_image_paths=lambda key: []),
                      TurnContext(message='hello', session_id='cache-usage', session_key='cache-usage',
                                  source=SimpleNamespace(user_id='u', user_name='U', is_bot=False)))


def _capture(agent):
    captured = {}
    token = execution_result.set(captured)
    try:
        _turn()._run_conversation_with_approval(agent, [], None, None, None)
    finally:
        execution_result.reset(token)
    return captured['usage']


def test_captured_usage_reports_cache_deltas_for_the_turn():
    agent = SimpleNamespace(session_prompt_tokens=1000, session_completion_tokens=200,
                            session_cache_read_tokens=600, session_cache_write_tokens=100)

    def run(message, **kwargs):
        # One API call: 50 uncached + 300 cache read + 40 cache write prompt tokens, 25 output.
        agent.session_prompt_tokens += 390
        agent.session_completion_tokens += 25
        agent.session_cache_read_tokens += 300
        agent.session_cache_write_tokens += 40
        return {'final_response': 'ok'}
    agent.run_conversation = run

    assert _capture(agent) == {'input_tokens': 390, 'output_tokens': 25, 'total_tokens': 415,
                               'cache_read_tokens': 300, 'cache_write_tokens': 40}


def test_captured_usage_without_cache_counters_reports_zero_cache():
    agent = SimpleNamespace(session_prompt_tokens=10, session_completion_tokens=5)

    def run(message, **kwargs):
        agent.session_prompt_tokens += 7
        agent.session_completion_tokens += 3
        return {'final_response': 'ok'}
    agent.run_conversation = run

    assert _capture(agent) == {'input_tokens': 7, 'output_tokens': 3, 'total_tokens': 10,
                               'cache_read_tokens': 0, 'cache_write_tokens': 0}
