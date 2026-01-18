import pytest

from monitor.event_bindings import EventActionConfig, instantiate_action_binding
from monitor.actions import LogActionConfig, LogAction
from monitor.conditions import AlwaysTrueConditionConfig, AlwaysTrueCondition


def test_instantiate_action_binding_valid():
    config = EventActionConfig(
        action=LogActionConfig(message="hello"),
        conditions=[AlwaysTrueConditionConfig()],
        mode="queue",
    )

    binding = instantiate_action_binding(config, event_name="evt", kind="log", index=0)

    assert isinstance(binding.action, LogAction)
    assert binding.action.config.message == "hello"
    assert binding.mode == "queue"
    assert len(binding.conditions) == 1
    assert isinstance(binding.conditions[0], AlwaysTrueCondition)
    assert binding.action_id == "log:evt:LogAction:0"


def test_instantiate_action_binding_invalid():
    config = EventActionConfig()
    with pytest.raises(ValueError, match="requires 'action'"):
        instantiate_action_binding(config, event_name="evt", kind="log", index=0)


def test_instantiate_action_binding_from_dict():
    config = EventActionConfig(action={"class_name": "LogAction", "message": "from-dict"})

    binding = instantiate_action_binding(config, event_name="evt", kind="log", index=0)

    assert isinstance(binding.action, LogAction)
    assert binding.action.config.message == "from-dict"
