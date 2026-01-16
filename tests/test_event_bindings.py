import pytest
from monitor.event_bindings import (
    instantiate_bindings, 
    EventActionConfig, 
    EventActionBinding,
    EventAction
)
from monitor.actions import LogActionConfig, LogAction, ActionContext
from monitor.conditions import AlwaysTrueConditionConfig, AlwaysTrueCondition
from monitor.events import EventRecord

def test_instantiate_bindings_valid():
    configs = [
        EventActionConfig(
            action=LogActionConfig(message="hello"),
            conditions=[AlwaysTrueConditionConfig()],
            mode="queue"
        )
    ]
    
    bindings = instantiate_bindings(configs)
    
    assert len(bindings) == 1
    binding = bindings[0]
    assert isinstance(binding.action, LogAction)
    assert binding.action.config.message == "hello"
    assert binding.mode == "queue"
    assert len(binding.conditions) == 1
    assert isinstance(binding.conditions[0], AlwaysTrueCondition)

def test_instantiate_bindings_shorthand():
    # Helper support for raw action config (implicit inline mode, no conditions)
    configs = [
        LogActionConfig(message="simple")
    ]
    
    bindings = instantiate_bindings(configs)
    
    assert len(bindings) == 1
    binding = bindings[0]
    assert isinstance(binding.action, LogAction)
    assert binding.mode == "inline"
    assert len(binding.conditions) == 0

def test_instantiate_bindings_invalid():
    # Missing action
    with pytest.raises(ValueError, match="requires 'action'"):
        instantiate_bindings([EventActionConfig()])
        
    # Unsupported type
    with pytest.raises(TypeError, match="Unsupported action config"):
        instantiate_bindings(["not-a-config"])

def test_event_action_wrapper():
    # Test the EventAction wrapper class
    log_cfg = LogActionConfig(message="test")
    config = EventActionConfig(action=log_cfg)
    
    wrapper = EventAction(config)
    
    # It should delegate execute to the inner action
    event = EventRecord(event_id="e1", name="n", source="s")
    ctx = ActionContext(event=event)
    
    result = wrapper.execute(ctx)
    assert result.status == "success"
    assert result.message == "test"
