from __future__ import annotations

import time

from monitor.conditions import (
    AlwaysTrueConditionConfig,
    AndCondition,
    AndConditionConfig,
    ConditionContext,
    CommandCondition,
    CommandConditionConfig,
    CompositeCondition,
    CompositeConditionConfig,
    FileContentCondition,
    FileContentConditionConfig,
    FileExistsCondition,
    FileExistsConditionConfig,
    GlobExistsCondition,
    GlobExistsConditionConfig,
    MaxAttemptsCondition,
    MaxAttemptsConditionConfig,
    MetadataCondition,
    MetadataConditionConfig,
    NotCondition,
    NotConditionConfig,
    OrCondition,
    OrConditionConfig,
    ShellCommandCondition,
    ShellCommandConditionConfig,
    TimeoutCondition,
    TimeoutConditionConfig,
)


def test_timeout_condition_pass_then_fail(monkeypatch) -> None:
    start = 1000.0
    monkeypatch.setattr(time, "time", lambda: start)
    condition = TimeoutCondition(TimeoutConditionConfig(timeout_seconds=5.0))
    ctx = ConditionContext()

    result = condition.check(ctx)
    assert result.passed is True

    monkeypatch.setattr(time, "time", lambda: start + 6.0)
    result = condition.check(ctx)
    assert result.passed is False


def test_file_exists_condition(tmp_path) -> None:
    target = tmp_path / "exists.txt"
    condition = FileExistsCondition(FileExistsConditionConfig(path=str(target)))
    ctx = ConditionContext()
    assert condition.check(ctx).passed is False
    target.write_text("ok", encoding="utf-8")
    assert condition.check(ctx).passed is True


def test_metadata_condition_equals() -> None:
    ctx = ConditionContext(event=None, job_metadata={}, attempts=0, state={}, started_ts=None)
    ctx.event = type("E", (), {"metadata": {"status": "ok"}})()
    condition = MetadataCondition(MetadataConditionConfig(key="status", equals="ok"))
    assert condition.check(ctx).passed is True


def test_or_condition() -> None:
    condition = OrCondition(
        OrConditionConfig(
            conditions=[
                MetadataConditionConfig(key="status", equals="missing"),
                MetadataConditionConfig(key="status", equals="ok"),
            ]
        )
    )
    ctx = ConditionContext(event=type("E", (), {"metadata": {"status": "ok"}})())
    assert condition.check(ctx).passed is True


def test_not_condition() -> None:
    condition = NotCondition(NotConditionConfig(condition=MetadataConditionConfig(key="status", equals="ok")))
    ctx = ConditionContext(event=type("E", (), {"metadata": {"status": "bad"}})())
    assert condition.check(ctx).passed is True


def test_command_condition_success() -> None:
    condition = CommandCondition(CommandConditionConfig(command=["bash", "-c", "exit 0"]))
    assert condition.check(ConditionContext()).passed is True


def test_shell_command_condition_failure() -> None:
    condition = ShellCommandCondition(ShellCommandConditionConfig(command="exit 1"))
    assert condition.check(ConditionContext()).passed is False


def test_max_attempts_condition_below_limit() -> None:
    condition = MaxAttemptsCondition(MaxAttemptsConditionConfig(max_attempts=3))
    ctx = ConditionContext(attempts=2)
    assert condition.check(ctx).passed is True


def test_max_attempts_condition_at_limit() -> None:
    condition = MaxAttemptsCondition(MaxAttemptsConditionConfig(max_attempts=3))
    ctx = ConditionContext(attempts=3)
    result = condition.check(ctx)
    assert result.passed is False
    assert "3" in result.message


def test_cooldown_condition_no_event() -> None:
    condition = MaxAttemptsCondition(MaxAttemptsConditionConfig(max_attempts=1))
    # Use CooldownCondition specifically for the no-event path
    from monitor.conditions import CooldownCondition, CooldownConditionConfig
    cond = CooldownCondition(CooldownConditionConfig(cooldown_seconds=60))
    ctx = ConditionContext(event=None)
    assert cond.check(ctx).passed is True


def test_glob_exists_condition_pass(tmp_path) -> None:
    (tmp_path / "a.txt").write_text("x")
    (tmp_path / "b.txt").write_text("x")
    condition = GlobExistsCondition(GlobExistsConditionConfig(pattern=str(tmp_path / "*.txt"), min_matches=2))
    assert condition.check(ConditionContext()).passed is True


def test_glob_exists_condition_fail(tmp_path) -> None:
    (tmp_path / "a.txt").write_text("x")
    condition = GlobExistsCondition(GlobExistsConditionConfig(pattern=str(tmp_path / "*.txt"), min_matches=2))
    result = condition.check(ConditionContext())
    assert result.passed is False
    assert "missing" in result.message


def test_file_content_condition_regex_pass(tmp_path) -> None:
    f = tmp_path / "out.log"
    f.write_text("step=42\n")
    condition = FileContentCondition(FileContentConditionConfig(path=str(f), pattern=r"step=\d+", mode="regex"))
    assert condition.check(ConditionContext()).passed is True


def test_file_content_condition_regex_fail(tmp_path) -> None:
    f = tmp_path / "out.log"
    f.write_text("no match here\n")
    condition = FileContentCondition(FileContentConditionConfig(path=str(f), pattern=r"step=\d+", mode="regex"))
    assert condition.check(ConditionContext()).passed is False


def test_command_condition_failure() -> None:
    condition = CommandCondition(CommandConditionConfig(command=["bash", "-c", "exit 2"]))
    result = condition.check(ConditionContext())
    assert result.passed is False
    assert "2" in result.message


def test_shell_command_condition_failure_message() -> None:
    condition = ShellCommandCondition(ShellCommandConditionConfig(command="exit 3"))
    result = condition.check(ConditionContext())
    assert result.passed is False
    assert "3" in result.message


def test_composite_condition_any_all_fail() -> None:
    condition = CompositeCondition(CompositeConditionConfig(
        mode="any",
        conditions=[
            MetadataConditionConfig(key="status", equals="missing"),
            MetadataConditionConfig(key="status", equals="also_missing"),
        ],
    ))
    ctx = ConditionContext(event=type("E", (), {"metadata": {"status": "other"}})())
    result = condition.check(ctx)
    assert result.passed is False


def test_and_condition() -> None:
    condition = AndCondition(AndConditionConfig(
        conditions=[AlwaysTrueConditionConfig(), AlwaysTrueConditionConfig()],
    ))
    assert condition.check(ConditionContext()).passed is True


def test_not_condition_child_passes_returns_failure_message() -> None:
    condition = NotCondition(NotConditionConfig(condition=AlwaysTrueConditionConfig()))
    result = condition.check(ConditionContext())
    assert result.passed is False
    assert "NOT condition failed" in result.message


def test_metadata_condition_within_pass() -> None:
    ctx = ConditionContext(event=type("E", (), {"metadata": {"status": "ready"}})())
    condition = MetadataCondition(MetadataConditionConfig(key="status", within=["ready", "running"]))
    assert condition.check(ctx).passed is True


def test_metadata_condition_within_fail() -> None:
    ctx = ConditionContext(event=type("E", (), {"metadata": {"status": "error"}})())
    condition = MetadataCondition(MetadataConditionConfig(key="status", within=["ready", "running"]))
    result = condition.check(ctx)
    assert result.passed is False
    assert "not in" in result.message
