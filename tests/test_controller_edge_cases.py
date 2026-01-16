import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from monitor.controller import (
    MonitorController, 
    JobRuntimeState, 
    JobRegistration,
    MonitorOutcome,
    MonitorEvent
)
from monitor.watcher import BaseMonitor
from monitor.job_client_protocol import JobClientProtocol
from monitor.action_queue import ActionQueue
from monitor.actions import RestartAction, RestartActionConfig
from monitor.event_bindings import EventActionBinding

def test_controller_expand_log_path_array():
    """Test _expand_log_path with array templates."""
    controller = MonitorController(MagicMock(), MagicMock())
    
    # Array job
    path = controller._expand_log_path("100_5", "job_%A_%a.log")
    assert str(path) == "job_100_5.log"
    
    # Single job
    path_single = controller._expand_log_path("123", "job_%j.log")
    assert str(path_single) == "job_123.log"

def test_controller_classify_mode():
    """Test _classify_mode logic."""
    controller = MonitorController(MagicMock(), MagicMock())
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    
    # Success via monitor
    outcome_success = MonitorOutcome(job_id="1", status="complete", last_update_seconds=0, metadata={})
    mode, meta = controller._classify_mode(state, outcome_success, {})
    assert mode == "success"
    assert meta["reason"] == "termination_condition_met"
    
    # Stall via monitor
    outcome_stall = MonitorOutcome(job_id="1", status="stall", last_update_seconds=100, metadata={})
    mode, meta = controller._classify_mode(state, outcome_stall, {"1": "RUNNING"})
    assert mode == "stall"
    
    # Timeout via SLURM
    mode, meta = controller._classify_mode(state, None, {"1": "TIMEOUT"})
    assert mode == "timeout"
    
    # Cancelled via SLURM
    mode, meta = controller._classify_mode(state, None, {"1": "CANCELLED"})
    assert mode == "crash"
    assert meta["error_type"] == "cancelled"
    
    # Failed via SLURM
    mode, meta = controller._classify_mode(state, None, {"1": "FAILED"})
    assert mode == "crash"
    assert meta["error_type"] == "slurm_failure"
    
    # Completed via SLURM
    mode, meta = controller._classify_mode(state, None, {"1": "COMPLETED"})
    assert mode == "success"
    
    # Missing from SLURM
    mode, meta = controller._classify_mode(state, None, {})
    assert mode == "timeout"
    assert meta["reason"] == "job_not_in_queue"

def test_controller_handle_monitor_event_queue_mode(tmp_path):
    """Test handling events with queued actions."""
    monitor = MagicMock(spec=BaseMonitor)
    # Mock config.state_events to empty
    monitor.config = MagicMock()
    monitor.config.state_events = []
    
    slurm = MagicMock()
    
    # Mock state store to enable action queue
    state_store = MagicMock()
    state_store.session_path = tmp_path / "session.json"
    state_store.load_events.return_value = {}
    
    controller = MonitorController(monitor, slurm, state_store)
    
    # Manually initialize action queue since we mocked state_store
    controller._action_queue = MagicMock(spec=ActionQueue)
    controller._action_queue.enqueue.return_value = MagicMock(queue_id="q1")
    
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    state.submitted = True
    
    # Create event with queued action
    action = RestartAction(RestartActionConfig())
    binding = EventActionBinding(action=action, mode="queue", conditions=[])
    event = MonitorEvent(job_id="1", name="evt", metadata={}, actions=[binding])
    
    cycle = MagicMock()
    cycle.events = []
    cycle.decisions = {}
    
    controller._handle_monitor_event(state, event, cycle)
    
    # Check enqueue called
    controller._action_queue.enqueue.assert_called_once()
    assert controller._action_queue.enqueue.call_args[0][0] == "RestartAction"

def test_controller_handle_monitor_event_restart():
    """Test handling event that requests restart."""
    controller = MonitorController(MagicMock(), MagicMock())
    controller._executor = MagicMock()
    
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    
    event = MonitorEvent(job_id="1", name="evt", metadata={})
    
    # Action outcome with restart=True
    action_outcome = {"restart": True, "queued": [], "results": []}
    
    decision = controller._finalize_event(state, event, action_outcome)
    
    assert decision.action == "restart"
    controller._executor.restart_job.assert_called_once_with(state)

def test_controller_capture_slurm_transitions():
    """Test capturing SLURM state transitions."""
    controller = MonitorController(MagicMock(), MagicMock())
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    state.last_slurm_state = "PENDING"
    
    # Transition to RUNNING
    records = controller._capture_slurm_transitions(state, "RUNNING")
    assert len(records) == 1
    assert records[0].event == "slurm_state_transition"
    assert records[0].metadata["slurm_state"] == "RUNNING"
    assert state.last_slurm_state == "RUNNING"
    
    # Transition to COMPLETED
    records = controller._capture_slurm_transitions(state, "COMPLETED")
    assert len(records) == 1
    assert records[0].action == "run_ended"
