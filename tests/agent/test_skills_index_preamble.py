"""``skills.index_preamble``: the per-profile wording of the ## Skills index preamble.

Default (key absent, ``mandatory`` or unknown) is byte-identical to the historical prompt; ``on_demand``
keeps the same index rows but drops every mandatory-load instruction.
"""
import json

import pytest

from agent.prompt_builder import build_skills_system_prompt, clear_skills_system_prompt_cache
from agent.skills_index_delta import index_entries

MANDATORY_WORDING = (
    "MUST load", "Err on the side of loading", "always better to have context",
    "load them even for tasks you already know", "Only proceed without loading",
)


@pytest.fixture(autouse=True)
def _fresh_cache():
    clear_skills_system_prompt_cache(clear_snapshot=True)
    yield
    clear_skills_system_prompt_cache(clear_snapshot=True)


@pytest.fixture
def home(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    for rel, desc in (("devops/paperclip", "Coordinate work through the Paperclip API."),
                      ("devops/deploy", "Deploy a service."), ("notes", "Keep notes.")):
        d = tmp_path / "skills" / rel
        d.mkdir(parents=True)
        (d / "SKILL.md").write_text(f"---\nname: {d.name}\ndescription: {desc}\n---\n\nBody.\n", encoding="utf-8")
    return tmp_path


def _set_preamble(home, value):
    (home / "config.yaml").write_text(f"skills:\n  index_preamble: {json.dumps(value)}\n", encoding="utf-8")
    clear_skills_system_prompt_cache(clear_snapshot=True)


# The historical default block, verbatim: the default must not drift for any profile that never sets the key.
EXPECTED_DEFAULT = (
    "## Skills\n"
    "Before replying, scan the skills below. If a skill matches or is even partially relevant to your "
    "task, you MUST load it with skill_view(name) and follow its instructions. Err on the side of "
    "loading — it is always better to have context you don't need than to miss critical steps, pitfalls, "
    "or established workflows. Skills contain specialized knowledge — API endpoints, tool-specific "
    "commands, and proven workflows that outperform general-purpose approaches. Load the skill "
    "even if you think you could handle the task with basic tools like web_search or terminal. "
    "Skills also encode the user's preferred approach, conventions, and quality standards for tasks like "
    "code review, planning, and testing — load them even for tasks you already know how to do, because "
    "the skill defines how it should be done here.\n"
    "If a skill has issues, fix it with skill_manage(action='patch').\n"
    "After difficult/iterative tasks, offer to save as a skill. If a skill you loaded was missing steps, "
    "had wrong commands, or needed pitfalls you discovered, update it before finishing.\n"
    "\n"
    "<available_skills>\n"
    "  devops:\n"
    "    - deploy: Deploy a service.\n"
    "    - paperclip: Coordinate work through the Paperclip API.\n"
    "  - notes: Keep notes.\n"
    "</available_skills>\n\n"
    "Only proceed without loading a skill if genuinely none are relevant to the task."
)


@pytest.mark.parametrize("value", [None, "mandatory", "MANDATORY", "bogus", 3])
def test_default_preamble_is_byte_identical(home, value):
    if value is not None:
        _set_preamble(home, value)
    assert build_skills_system_prompt() == EXPECTED_DEFAULT


@pytest.mark.parametrize("value", ["on_demand", " On_Demand "])
def test_on_demand_preamble_has_no_mandatory_load_wording(home, value):
    _set_preamble(home, value)
    prompt = build_skills_system_prompt()

    assert prompt.startswith("## Skills\n")
    for phrase in MANDATORY_WORDING:
        assert phrase not in prompt
    assert "only when the instructions already in your context do not cover the action" in prompt
    assert "at most once per run" in prompt and "never reload a skill you already loaded in this run" in prompt
    # Same index rows as the default, so skill_view names and the turn-delta parser are unaffected.
    _set_preamble(home, "mandatory")
    assert index_entries(prompt) == index_entries(build_skills_system_prompt())


def test_switching_mode_is_not_served_from_the_other_modes_cache(home):
    """The mode is part of the prompt cache key: one process serving several profiles must not leak wording."""
    default = build_skills_system_prompt()
    (home / "config.yaml").write_text("skills:\n  index_preamble: on_demand\n", encoding="utf-8")
    on_demand = build_skills_system_prompt()  # no cache clear: config change alone must change the result
    assert default == EXPECTED_DEFAULT and on_demand != default and "MUST load" not in on_demand
