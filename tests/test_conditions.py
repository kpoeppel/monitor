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
    assert result.status == "pass"
    assert result.message == "always pass"

def test_max_attempts_condition():
    config = MaxAttemptsConditionConfig(max_attempts=2)
    condition = MaxAttemptsCondition(config)
    
    assert condition.check(ConditionContext(attempts=0)).status == "pass"
    assert condition.check(ConditionContext(attempts=1)).status == "pass"
    
    result = condition.check(ConditionContext(attempts=2))
    assert result.status == "fail"
    assert "attempts 2 >= limit 2" in result.message

def test_cooldown_condition(mock_event):
    # Event happened 100s ago
    
    # 60s cooldown -> Pass
    config_pass = CooldownConditionConfig(cooldown_seconds=60)
    cond_pass = CooldownCondition(config_pass)
    assert cond_pass.check(ConditionContext(event=mock_event)).status == "pass"
    
    # 200s cooldown -> Waiting
    config_wait = CooldownConditionConfig(cooldown_seconds=200)
    cond_wait = CooldownCondition(config_wait)
    result = cond_wait.check(ConditionContext(event=mock_event))
    assert result.status == "waiting"
    assert "cooldown" in result.message

def test_file_exists_condition(tmp_path):
    f = tmp_path / "exists.txt"
    config = FileExistsConditionConfig(path=str(f), blocking=False)
    condition = FileExistsCondition(config)
    ctx = ConditionContext()
    
    # Missing
    assert condition.check(ctx).status == "waiting"
    
    # Exists
    f.touch()
    assert condition.check(ctx).status == "pass"

    # Blocking timeout
    config_block = FileExistsConditionConfig(path=str(tmp_path / "missing"), blocking=True, timeout_seconds=0.1, poll_interval_seconds=0.01)
    condition_block = FileExistsCondition(config_block)
    assert condition_block.check(ctx).status == "fail"

def test_file_content_condition(tmp_path):
    f = tmp_path / "content.txt"
    config = FileContentConditionConfig(path=str(f), pattern="TARGET", mode="contains", blocking=False)
    condition = FileContentCondition(config)
    ctx = ConditionContext()
    
    # Missing file
    assert condition.check(ctx).status == "waiting"
    
    # Wrong content
    f.write_text("WRONG")
    assert condition.check(ctx).status == "waiting"
    
    # Correct content
    f.write_text("Found TARGET here")
    assert condition.check(ctx).status == "pass"
    
    # Regex mode
    config_regex = FileContentConditionConfig(path=str(f), pattern=r"T\w+T", mode="regex", blocking=False)
    condition_regex = FileContentCondition(config_regex)
    assert condition_regex.check(ctx).status == "pass"

def test_glob_exists_condition(tmp_path):
    config = GlobExistsConditionConfig(pattern=str(tmp_path / "*.txt"), min_matches=2, blocking=False)
    condition = GlobExistsCondition(config)
    ctx = ConditionContext()
    
    assert condition.check(ctx).status == "waiting"
    
    (tmp_path / "1.txt").touch()
    assert condition.check(ctx).status == "waiting"
    
    (tmp_path / "2.txt").touch()
    assert condition.check(ctx).status == "pass"

def test_command_condition():
    # Success
    config_ok = CommandConditionConfig(command=["echo", "hello"])
    cond_ok = CommandCondition(config_ok)
    assert cond_ok.check(ConditionContext()).status == "pass"
    
    # Failure
    config_fail = CommandConditionConfig(command=["false"])
    cond_fail = CommandCondition(config_fail)
    result = cond_fail.check(ConditionContext())
    assert result.status == "fail"
    assert "exited with 1" in result.message
    
    # No command
    config_empty = CommandConditionConfig(command=[])
    cond_empty = CommandCondition(config_empty)
    assert cond_empty.check(ConditionContext()).status == "fail"

def test_shell_command_condition():
    # Success
    config_ok = ShellCommandConditionConfig(command="echo hello")
    cond_ok = ShellCommandCondition(config_ok)
    assert cond_ok.check(ConditionContext()).status == "pass"
    
    # Failure
    config_fail = ShellCommandConditionConfig(command="exit 1")
    cond_fail = ShellCommandCondition(config_fail)
    result = cond_fail.check(ConditionContext())
    assert result.status == "fail"
    assert "exited with 1" in result.message

def test_metadata_condition(mock_event):
    # Key present
    config = MetadataConditionConfig(key="key", equals="value")
    cond = MetadataCondition(config)
    assert cond.check(ConditionContext(event=mock_event)).status == "pass"
    
    # Key missing
    config_missing = MetadataConditionConfig(key="missing")
    cond_missing = MetadataCondition(config_missing)
    assert cond_missing.check(ConditionContext(event=mock_event)).status == "fail"
    
    # Equals mismatch
    config_neq = MetadataConditionConfig(key="key", equals="other")
    cond_neq = MetadataCondition(config_neq)
    assert cond_neq.check(ConditionContext(event=mock_event)).status == "fail"
    
    # Within
    config_in = MetadataConditionConfig(key="key", within=["a", "value", "b"])
    cond_in = MetadataCondition(config_in)
    assert cond_in.check(ConditionContext(event=mock_event)).status == "pass"
    
    # Not within
    config_notin = MetadataConditionConfig(key="key", within=["a", "b"])
    cond_notin = MetadataCondition(config_notin)
    assert cond_notin.check(ConditionContext(event=mock_event)).status == "fail"
    
    # No key configured
    config_nokey = MetadataConditionConfig(key="")
    cond_nokey = MetadataCondition(config_nokey)
    assert cond_nokey.check(ConditionContext(event=mock_event)).status == "fail"

def test_composite_conditions():
    # Mocks
    pass_cond = MagicMock(spec=AlwaysTrueCondition)
    pass_cond.check.return_value = ConditionResult(status="pass")
    
    fail_cond = MagicMock(spec=AlwaysTrueCondition)
    fail_cond.check.return_value = ConditionResult(status="fail")
    
    wait_cond = MagicMock(spec=AlwaysTrueCondition)
    wait_cond.check.return_value = ConditionResult(status="waiting")
    
    # We can't easily inject mocks into CompositeCondition because it instantiates children from config.
    # So we use concrete classes wrapped in config.
    
    c_pass = AlwaysTrueConditionConfig(message="pass")
    c_fail = CommandConditionConfig(command=["false"]) # always fails
    c_wait = FileExistsConditionConfig(path="/non/existent/path", blocking=False) # always waits
    
    # AND: All pass -> Pass
    and_pass = AndCondition(AndConditionConfig(conditions=[c_pass, c_pass]))
    assert and_pass.check(ConditionContext()).status == "pass"
    
    # AND: One fail -> Fail
    and_fail = AndCondition(AndConditionConfig(conditions=[c_pass, c_fail]))
    assert and_fail.check(ConditionContext()).status == "fail"
    
    # AND: One wait -> Wait (if no fail)
    and_wait = AndCondition(AndConditionConfig(conditions=[c_pass, c_wait]))
    assert and_wait.check(ConditionContext()).status == "waiting"
    
    # OR: One pass -> Pass
    or_pass = OrCondition(OrConditionConfig(conditions=[c_fail, c_pass]))
    assert or_pass.check(ConditionContext()).status == "pass"
    
    # OR: All fail -> Fail
    or_fail = OrCondition(OrConditionConfig(conditions=[c_fail, c_fail]))
    assert or_fail.check(ConditionContext()).status == "fail"
    
    # OR: Wait (if no pass)
    or_wait = OrCondition(OrConditionConfig(conditions=[c_fail, c_wait]))
    assert or_wait.check(ConditionContext()).status == "waiting"

def test_not_condition():
    c_pass = AlwaysTrueConditionConfig(message="pass")
    c_fail = CommandConditionConfig(command=["false"])
    c_wait = FileExistsConditionConfig(path="/non/existent/path", blocking=False)
    
    # Not Pass -> Fail
    not_pass = NotCondition(NotConditionConfig(condition=c_pass))
    assert not_pass.check(ConditionContext()).status == "fail"
    
    # Not Fail -> Pass
    not_fail = NotCondition(NotConditionConfig(condition=c_fail))
    assert not_fail.check(ConditionContext()).status == "pass"
    
    # Not Wait -> Wait
    not_wait = NotCondition(NotConditionConfig(condition=c_wait))
    assert not_wait.check(ConditionContext()).status == "waiting"