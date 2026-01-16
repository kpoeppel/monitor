import pytest
from unittest.mock import MagicMock, patch
from monitor.actions import (
    ActionContext, 
    RunCommandAction, 
    RunCommandActionConfig, 
    ActionResult,
    replace_braced_keys
)
from monitor.events import EventRecord

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
