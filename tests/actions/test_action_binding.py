from __future__ import annotations

from monitor.actions import LogActionConfig, LogEventConfig, instantiate_action_binding
from monitor.conditions import AlwaysTrueConditionConfig


def test_instantiate_action_binding() -> None:
    event = LogEventConfig(
        name="ready",
        pattern="READY",
        action=LogActionConfig(message="ok"),
        conditions=[AlwaysTrueConditionConfig()],
    )
    binding = instantiate_action_binding(event, event_name="ready", kind="log", index=0)
    assert binding.action is not None
    assert binding.conditions
