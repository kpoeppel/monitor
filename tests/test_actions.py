import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from monitor.actions import (
    ActionContext, 
    RunCommandAction, RunCommandActionConfig,
    RestartAction, RestartActionConfig,
    LogAction, LogActionConfig,
    PublishEventAction, PublishEventActionConfig,
    RunAutoexpAction, RunAutoexpActionConfig,
    ActionResult,
    replace_braced_keys
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
    config = RestartActionConfig(reason="bad state")
    action = RestartAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    assert result.status == "retry"
    assert result.message == "bad state"

def test_log_action(mock_event):
    config = LogActionConfig(message="Job {job_id} encountered issue")
    action = LogAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    assert result.status == "success"
    assert result.message == "Job 123 encountered issue"

def test_publish_event_action(mock_event):
    config = PublishEventActionConfig(
        event_name="custom_event",
        metadata={"source": "action"},
        payload={"data": 1}
    )
    action = PublishEventAction(config)
    context = ActionContext(event=mock_event)
    
    result = action.execute(context)
    assert result.status == "success"
    assert result.metadata["publish_event"]["name"] == "custom_event"
    assert result.metadata["publish_event"]["payload"]["data"] == 1

@patch("monitor.actions.subprocess.run")
def test_run_autoexp_action(mock_run, mock_event):
    mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
    
    config = RunAutoexpActionConfig(
        script="myscript.py",
        overrides=["param={job_id}"],
        config_path="conf.yaml"
    )
    action = RunAutoexpAction(config)
    context = ActionContext(
        event=mock_event, 
        job_metadata={"session_id": "sess-1"},
        workspace=Path("/work")
    )
    
    result = action.execute(context)
    
    assert result.status == "success"
    
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    cmd = args[0]
    
    assert cmd[0] == sys.executable
    assert cmd[1] == "myscript.py"
    assert "--config-ref" in cmd
    assert "conf.yaml" in cmd
    assert "param=123" in cmd
    assert "--no-monitor" in cmd
    assert "--plan-id" in cmd
    assert "sess-1" in cmd
    assert kwargs["cwd"] == str(Path("/work"))

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