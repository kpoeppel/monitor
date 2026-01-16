import pytest
import time
from unittest.mock import MagicMock, patch
from monitor.conditions import (
    ConditionContext, 
    MaxAttemptsCondition, 
    MaxAttemptsConditionConfig,
    CooldownCondition,
    CooldownConditionConfig,
    ConditionResult
)
from monitor.events import EventRecord

@pytest.fixture
def mock_event():
    return EventRecord(
        event_id="evt-1",
        name="test_event",
        source="test_source",
        metadata={"last_action_ts": time.time() - 100},
        payload={}
    )

def test_max_attempts_condition(mock_event):
    config = MaxAttemptsConditionConfig(max_attempts=3)
    condition = MaxAttemptsCondition(config)
    
    # 0 attempts
    ctx = ConditionContext(event=mock_event, attempts=0)
    assert condition.check(ctx).status == "pass"
    
    # 2 attempts
    ctx = ConditionContext(event=mock_event, attempts=2)
    assert condition.check(ctx).status == "pass"
    
    # 3 attempts (at limit)
    ctx = ConditionContext(event=mock_event, attempts=3)
    result = condition.check(ctx)
    assert result.status == "fail"
    assert "attempts 3 >= limit 3" in result.message

def test_cooldown_condition_passed(mock_event):
    # event happened 100s ago, cooldown is 60s
    config = CooldownConditionConfig(cooldown_seconds=60)
    condition = CooldownCondition(config)
    ctx = ConditionContext(event=mock_event)
    
    assert condition.check(ctx).status == "pass"

def test_cooldown_condition_waiting(mock_event):
    # event happened 100s ago, cooldown is 150s
    config = CooldownConditionConfig(cooldown_seconds=150)
    condition = CooldownCondition(config)
    ctx = ConditionContext(event=mock_event)
    
    result = condition.check(ctx)
    assert result.status == "waiting"
    assert "cooldown" in result.message
