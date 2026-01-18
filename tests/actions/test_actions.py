from __future__ import annotations

import subprocess

from monitor.actions import (
    ActionContext,
    DuplicateAction,
    DuplicateActionConfig,
    ActionBackendConfig,
    LogAction,
    LogActionConfig,
    LocalActionBackendConfig,
    RestartAction,
    RestartActionConfig,
    RunCommandAction,
    RunCommandActionConfig,
    CancelAction,
    CancelActionConfig,
    SlurmActionBackendConfig,
    EventRecord,
)


def _context() -> ActionContext:
    event = EventRecord(event_id="evt1", name="evt", source="log")
    return ActionContext(event=event, job_metadata={"job_kind": "local", "job_name": "job1"})


def test_restart_action_adjustments() -> None:
    action = RestartAction(
        RestartActionConfig(
            reason="oom",
            extra_args_append=["--retry={attempts}"],
            backend_config=LocalActionBackendConfig(),
        )
    )
    context = _context()
    context.attempts = 2
    result = action.execute(context)
    assert result.status == "retry"
    assert result.metadata["adjustments"]["extra_args_append"] == ["--retry=2"]


def test_duplicate_action_payload() -> None:
    action = DuplicateAction(
        DuplicateActionConfig(
            name_suffix="-copy",
            backend_config=LocalActionBackendConfig(),
        )
    )
    result = action.execute(_context())
    assert result.status == "success"
    duplicate = result.metadata["duplicate_job"]
    assert duplicate["name_suffix"] == "-copy"


def test_log_action_renders_message() -> None:
    action = LogAction(LogActionConfig(message="event {event_name}"))
    result = action.execute(_context())
    assert result.status == "success"
    assert result.message == "event evt"


def test_run_command_action_success() -> None:
    action = RunCommandAction(RunCommandActionConfig(command=["bash", "-c", "echo ok"]))
    result = action.execute(_context())
    assert result.status == "success"


def test_run_command_action_failure() -> None:
    action = RunCommandAction(RunCommandActionConfig(command=["bash", "-c", "exit 1"]))
    result = action.execute(_context())
    assert result.status == "failed"


def test_restart_action_backend_mismatch() -> None:
    action = RestartAction(
        RestartActionConfig(
            reason="slurm-only",
            backend_config=SlurmActionBackendConfig(),
        )
    )
    result = action.execute(_context())
    assert result.status == "failed"


def test_ensure_backend_allow_any_kind() -> None:
    action = RestartAction(
        RestartActionConfig(
            reason="any",
            backend_config=ActionBackendConfig(),
        )
    )
    result = action.execute(_context())
    assert result.status == "retry"


def test_run_command_action_empty_command() -> None:
    action = RunCommandAction(RunCommandActionConfig(command=[]))
    result = action.execute(_context())
    assert result.status == "failed"


def test_run_command_action_exception(monkeypatch) -> None:
    def boom(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr(subprocess, "run", boom)
    action = RunCommandAction(RunCommandActionConfig(command=["bash", "-c", "echo ok"]))
    result = action.execute(_context())
    assert result.status == "failed"
    assert "Command execution error" in result.message


def test_cancel_action_backend_mismatch() -> None:
    action = CancelAction(CancelActionConfig(backend_config=SlurmActionBackendConfig()))
    result = action.execute(_context())
    assert result.status == "failed"
