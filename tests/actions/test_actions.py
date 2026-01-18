import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from monitor.actions import (
    ActionContext,
    RunCommandAction, RunCommandActionConfig,
    RestartAction, RestartActionConfig,
    DuplicateAction, DuplicateActionConfig,
    FinishAction, FinishActionConfig,
    CancelAction, CancelActionConfig,
    LogAction, LogActionConfig,
    ActionResult,
    replace_braced_keys,
)
from monitor.events import EventRecord, EventStatus

@pytest.fixture
def mock_event():
    return EventRecord(
        event_id="evt-1",
        name="test_event",
        source="test_source",
        metadata={"job_id": "123"},
        payload={"value": "foo"}
    )

def test_replace_braced_keys():
    values = {"a": 1, "b": "two"}
    assert replace_braced_keys("Val: {a}", values) == "Val: 1"
    assert replace_braced_keys("{a}-{b}", values) == "1-two"
    assert replace_braced_keys("{missing}", values) == "{missing}"
    assert replace_braced_keys("No braces", values) == "No braces"

def test_action_context_render(mock_event):
    context = ActionContext(
        event=mock_event,
        job_metadata={"cluster": "juwels"}
    )
    
    assert context.render("Job: {job_id}") == "Job: 123"
    assert context.render("Val: {value}") == "Val: foo"
    assert context.render("Cluster: {cluster}") == "Cluster: juwels"
    assert context.render("Event: {event_name}") == "Event: test_event"

@patch("monitor.actions.subprocess.run")
def test_run_command_action_success(mock_run, mock_event):
    mock_run.return_value = MagicMock(returncode=0, stdout="done", stderr="")
    
    config = RunCommandActionConfig(
        command=["echo", "{job_id}"],
        cwd="/tmp"
    )
    action = RunCommandAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    
    assert result.status == "success"
    assert result.message == "command completed"
    
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    assert args[0] == ["echo", "123"]
    assert kwargs["cwd"] == "/tmp"

@patch("monitor.actions.subprocess.run")
def test_run_command_action_failure(mock_run, mock_event):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
    
    config = RunCommandActionConfig(
        command=["ls", "nonexistent"]
    )
    action = RunCommandAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    
    assert result.status == "failed"
    assert "command exited 1" in result.message
    assert result.metadata["stderr"] == "error"

def test_run_command_action_empty(mock_event):
    config = RunCommandActionConfig(command=[])
    action = RunCommandAction(config)
    context = ActionContext(event=mock_event)
    result = action.execute(context)
    assert result.status == "failed"
    assert "command is empty" in result.message

def test_restart_action(mock_event):
    config = RestartActionConfig(reason="bad state", extra_args=["--lr=0.1"])
    action = RestartAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    assert result.status == "retry"
    assert result.message == "bad state"
    assert result.metadata["adjustments"]["extra_args"] == ["--lr=0.1"]

def test_log_action(mock_event):
    config = LogActionConfig(message="Job {job_id} encountered issue")
    action = LogAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    assert result.status == "success"
    assert result.message == "Job 123 encountered issue"


def test_duplicate_action(mock_event):
    config = DuplicateActionConfig(
        extra_args=["--tag={job_id}"],
        name_suffix="-copy",
        log_path_current="latest-{job_id}.log",
    )
    action = DuplicateAction(config)
    context = ActionContext(event=mock_event)

    result = action.execute(context)
    assert result.status == "success"
    duplicate = result.metadata["duplicate_job"]
    assert duplicate["name_suffix"] == "-copy"
    assert duplicate["adjustments"]["extra_args"] == ["--tag=123"]
    assert duplicate["adjustments"]["log_path_current"] == "latest-123.log"

def test_update_event_status(mock_event):
    action = RestartAction(RestartActionConfig())
    
    # Success
    action.update_event(mock_event, ActionResult(status="success", message="done"))
    assert mock_event.status == EventStatus.PROCESSED
    assert mock_event.history[-1]["note"] == "done"
    
    # Failed
    action.update_event(mock_event, ActionResult(status="failed", message="err"))
    assert mock_event.status == EventStatus.FAILED
    assert mock_event.history[-1]["note"] == "err"
    
    # Retry/Other
    action.update_event(mock_event, ActionResult(status="retry", message="try again"))
    assert mock_event.status == EventStatus.PENDING
    assert mock_event.history[-1]["note"] == "try again"


def test_finish_action(mock_event):
    action = FinishAction(FinishActionConfig(reason="done"))
    context = ActionContext(event=mock_event)
    result = action.execute(context)
    assert result.status == "success"
    assert result.metadata["finalize"] == "success"
    assert result.message == "done"


def test_cancel_action(mock_event):
    action = CancelAction(CancelActionConfig(reason="stop"))
    context = ActionContext(event=mock_event)
    result = action.execute(context)
    assert result.status == "success"
    assert result.metadata["finalize"] == "cancel"
    assert result.message == "stop"
