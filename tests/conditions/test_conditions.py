import pytest
import time
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch
from monitor.conditions import (
    ConditionContext,
    ConditionResult,
    AlwaysTrueCondition, AlwaysTrueConditionConfig,
    MaxAttemptsCondition, MaxAttemptsConditionConfig,
    CooldownCondition, CooldownConditionConfig,
    FileExistsCondition, FileExistsConditionConfig,
    FileContentCondition, FileContentConditionConfig,
    GlobExistsCondition, GlobExistsConditionConfig,
    CommandCondition, CommandConditionConfig,
    ShellCommandCondition, ShellCommandConditionConfig,
    CompositeCondition, CompositeConditionConfig,
    AndCondition, AndConditionConfig,
    OrCondition, OrConditionConfig,
    NotCondition, NotConditionConfig,
    MetadataCondition, MetadataConditionConfig,
    replace_braced_keys
)
from monitor.events import EventRecord

@pytest.fixture
def mock_event():
    return EventRecord(
        event_id="evt-1",
        name="test_event",
        source="test_source",
        metadata={"last_action_ts": time.time() - 100, "key": "value"},
        payload={"pay": "load"}
    )

def test_replace_braced_keys():
    values = {"name": "test", "id": 123}
    assert replace_braced_keys("Hello {name}", values) == "Hello test"
    assert replace_braced_keys("ID: {id}", values) == "ID: 123"
    assert replace_braced_keys("Missing {missing}", values) == "Missing {missing}"
    assert replace_braced_keys("No braces", values) == "No braces"

def test_always_true_condition():
    config = AlwaysTrueConditionConfig(message="always pass")
    condition = AlwaysTrueCondition(config)
    result = condition.check(ConditionContext())
    assert result.passed
    assert result.message == "always pass"

def test_max_attempts_condition():
    config = MaxAttemptsConditionConfig(max_attempts=2)
    condition = MaxAttemptsCondition(config)
    
    assert condition.check(ConditionContext(attempts=0)).passed
    assert condition.check(ConditionContext(attempts=1)).passed
    
    result = condition.check(ConditionContext(attempts=2))
    assert not result.passed
    assert "attempts 2 >= limit 2" in result.message

def test_cooldown_condition(mock_event):
    # Event happened 100s ago
    
    # 60s cooldown -> Pass
    config_pass = CooldownConditionConfig(cooldown_seconds=60)
    cond_pass = CooldownCondition(config_pass)
    assert cond_pass.check(ConditionContext(event=mock_event)).passed
    
    # 200s cooldown -> Waiting
    config_wait = CooldownConditionConfig(cooldown_seconds=200)
    cond_wait = CooldownCondition(config_wait)
    result = cond_wait.check(ConditionContext(event=mock_event))
    assert not result.passed
    assert "cooldown" in result.message

def test_file_exists_condition(tmp_path):
    f = tmp_path / "exists.txt"
    config = FileExistsConditionConfig(path=str(f))
    condition = FileExistsCondition(config)
    ctx = ConditionContext()
    
    # Missing
    assert not condition.check(ctx).passed
    
    # Exists
    f.touch()
    assert condition.check(ctx).passed

def test_file_content_condition(tmp_path):
    f = tmp_path / "content.txt"
    config = FileContentConditionConfig(path=str(f), pattern="TARGET", mode="contains")
    condition = FileContentCondition(config)
    ctx = ConditionContext()
    
    # Missing file
    assert not condition.check(ctx).passed
    
    # Wrong content
    f.write_text("WRONG")
    assert not condition.check(ctx).passed
    
    # Correct content
    f.write_text("Found TARGET here")
    assert condition.check(ctx).passed
    
    # Regex mode
    config_regex = FileContentConditionConfig(path=str(f), pattern=r"T\w+T", mode="regex")
    condition_regex = FileContentCondition(config_regex)
    assert condition_regex.check(ctx).passed

def test_glob_exists_condition(tmp_path):
    config = GlobExistsConditionConfig(pattern=str(tmp_path / "*.txt"), min_matches=2)
    condition = GlobExistsCondition(config)
    ctx = ConditionContext()
    
    assert not condition.check(ctx).passed
    
    (tmp_path / "1.txt").touch()
    assert not condition.check(ctx).passed
    
    (tmp_path / "2.txt").touch()
    assert condition.check(ctx).passed

def test_command_condition():
    # Success
    config_ok = CommandConditionConfig(command=["echo", "hello"])
    cond_ok = CommandCondition(config_ok)
    assert cond_ok.check(ConditionContext()).passed
    
    # Failure
    config_fail = CommandConditionConfig(command=["false"])
    cond_fail = CommandCondition(config_fail)
    result = cond_fail.check(ConditionContext())
    assert not result.passed
    assert "exited with 1" in result.message
    
    # No command
    config_empty = CommandConditionConfig(command=[])
    cond_empty = CommandCondition(config_empty)
    assert not cond_empty.check(ConditionContext()).passed

def test_shell_command_condition():
    # Success
    config_ok = ShellCommandConditionConfig(command="echo hello")
    cond_ok = ShellCommandCondition(config_ok)
    assert cond_ok.check(ConditionContext()).passed
    
    # Failure
    config_fail = ShellCommandConditionConfig(command="exit 1")
    cond_fail = ShellCommandCondition(config_fail)
    result = cond_fail.check(ConditionContext())
    assert not result.passed
    assert "exited with 1" in result.message

def test_metadata_condition(mock_event):
    # Key present
    config = MetadataConditionConfig(key="key", equals="value")
    cond = MetadataCondition(config)
    assert cond.check(ConditionContext(event=mock_event)).passed
    
    # Key missing
    config_missing = MetadataConditionConfig(key="missing")
    cond_missing = MetadataCondition(config_missing)
    assert not cond_missing.check(ConditionContext(event=mock_event)).passed
    
    # Equals mismatch
    config_neq = MetadataConditionConfig(key="key", equals="other")
    cond_neq = MetadataCondition(config_neq)
    assert not cond_neq.check(ConditionContext(event=mock_event)).passed
    
    # Within
    config_in = MetadataConditionConfig(key="key", within=["a", "value", "b"])
    cond_in = MetadataCondition(config_in)
    assert cond_in.check(ConditionContext(event=mock_event)).passed
    
    # Not within
    config_notin = MetadataConditionConfig(key="key", within=["a", "b"])
    cond_notin = MetadataCondition(config_notin)
    assert not cond_notin.check(ConditionContext(event=mock_event)).passed
    
    # No key configured
    config_nokey = MetadataConditionConfig(key="")
    cond_nokey = MetadataCondition(config_nokey)
    assert not cond_nokey.check(ConditionContext(event=mock_event)).passed

def test_composite_conditions():
    # Mocks
    pass_cond = MagicMock(spec=AlwaysTrueCondition)
    pass_cond.check.return_value = ConditionResult(passed=True)
    
    fail_cond = MagicMock(spec=AlwaysTrueCondition)
    fail_cond.check.return_value = ConditionResult(passed=False)
    
    # We can't easily inject mocks into CompositeCondition because it instantiates children from config.
    # So we use concrete classes wrapped in config.
    
    c_pass = AlwaysTrueConditionConfig(message="pass")
    c_fail = CommandConditionConfig(command=["false"]) # always fails
    c_wait = FileExistsConditionConfig(path="/non/existent/path") # always false
    
    # AND: All pass -> Pass
    and_pass = AndCondition(AndConditionConfig(conditions=[c_pass, c_pass]))
    assert and_pass.check(ConditionContext()).passed
    
    # AND: One fail -> Fail
    and_fail = AndCondition(AndConditionConfig(conditions=[c_pass, c_fail]))
    assert not and_fail.check(ConditionContext()).passed
    
    # AND: One wait -> Wait (if no fail)
    and_wait = AndCondition(AndConditionConfig(conditions=[c_pass, c_wait]))
    assert not and_wait.check(ConditionContext()).passed
    
    # OR: One pass -> Pass
    or_pass = OrCondition(OrConditionConfig(conditions=[c_fail, c_pass]))
    assert or_pass.check(ConditionContext()).passed
    
    # OR: All fail -> Fail
    or_fail = OrCondition(OrConditionConfig(conditions=[c_fail, c_fail]))
    assert not or_fail.check(ConditionContext()).passed
    
    # OR: Wait (if no pass)
    or_wait = OrCondition(OrConditionConfig(conditions=[c_fail, c_wait]))
    assert not or_wait.check(ConditionContext()).passed

def test_not_condition():
    c_pass = AlwaysTrueConditionConfig(message="pass")
    c_fail = CommandConditionConfig(command=["false"])
    c_wait = FileExistsConditionConfig(path="/non/existent/path")
    
    # Not Pass -> Fail
    not_pass = NotCondition(NotConditionConfig(condition=c_pass))
    assert not_pass.check(ConditionContext()).passed is False
    
    # Not Fail -> Pass
    not_fail = NotCondition(NotConditionConfig(condition=c_fail))
    assert not_fail.check(ConditionContext()).passed
    
    # Not Wait -> Wait
    not_wait = NotCondition(NotConditionConfig(condition=c_wait))
    assert not_wait.check(ConditionContext()).passed
