from __future__ import annotations

import time

from monitor.conditions import (
    ConditionContext,
    CommandCondition,
    CommandConditionConfig,
    FileExistsCondition,
    FileExistsConditionConfig,
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
