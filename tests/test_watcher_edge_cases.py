import pytest
import time
from unittest.mock import MagicMock, patch
from pathlib import Path
from monitor.watcher import (
    SlurmLogMonitor,
    SlurmLogMonitorConfig,
    MonitoredJob,
    LogEventConfig,
    _fallback_state_for,
    _JobSnapshot
)
from monitor.states import StalledStateConfig

def test_watcher_read_log_oserror(tmp_path):
    """Test OSError handling when reading log file."""
    log_path = tmp_path / "job.log"
    log_path.touch()
    
    config = SlurmLogMonitorConfig(log_path=str(log_path))
    monitor = SlurmLogMonitor(config)
    
    job = MonitoredJob(
        job_id="1", 
        name="test", 
        log_path=str(log_path), 
        check_interval_seconds=60, 
        state="RUNNING"
    )
    
    with patch.object(Path, "read_text", side_effect=OSError("Read failed")):
        outcome = monitor._evaluate_job(job, time.time())
        
    # Should treat log content as empty, status remains pending or active?
    # If updated=False (because read failed and log_previous="", so no change),
    # and no content, status -> pending.
    assert outcome.status == "pending"

def test_watcher_read_output_oserror(tmp_path):
    """Test OSError handling when reading output file."""
    log_path = tmp_path / "job.log"
    output_path = tmp_path / "out.txt"
    log_path.touch()
    output_path.touch()
    
    config = SlurmLogMonitorConfig(log_path=str(log_path))
    monitor = SlurmLogMonitor(config)
    
    job = MonitoredJob(
        job_id="1", 
        name="test", 
        log_path=str(log_path), 
        check_interval_seconds=60, 
        state="RUNNING",
        output_paths=[str(output_path)]
    )
    
    # Pre-populate snapshot so we have previous content to compare against
    # monitor._snapshots["1"] = _JobSnapshot(log_content="", last_update=time.time())
    
    with patch.object(Path, "read_text", side_effect=OSError("Read failed")):
        outcome = monitor._evaluate_job(job, time.time())
        
    assert outcome.status == "pending"

def test_effective_threshold_override():
    """Test inactivity threshold override from job metadata."""
    config = SlurmLogMonitorConfig(log_path="l", inactivity_threshold_seconds=100)
    monitor = SlurmLogMonitor(config)
    
    job_default = MonitoredJob(job_id="1", name="n", log_path="l", check_interval_seconds=1, state="r")
    assert monitor._effective_threshold(job_default) == 100
    
    job_override = MonitoredJob(
        job_id="2", 
        name="n", 
        log_path="l", 
        check_interval_seconds=1, 
        state="r",
        metadata={"inactivity_threshold_seconds": 50}
    )
    assert monitor._effective_threshold(job_override) == 50

def test_fallback_state_for():
    """Test _fallback_state_for mapping."""
    from monitor.states import StalledState, TimeoutState, CrashState, SuccessState
    
    assert isinstance(_fallback_state_for("stall"), StalledState)
    assert isinstance(_fallback_state_for("timeout"), TimeoutState)
    assert isinstance(_fallback_state_for("crash"), CrashState)
    assert isinstance(_fallback_state_for("success"), SuccessState)
    assert _fallback_state_for("unknown") is None

def test_extract_events_edge_cases(tmp_path):
    """Test extract events with previous content matching."""
    log_path = tmp_path / "job.log"
    config = SlurmLogMonitorConfig(
        log_path=str(log_path),
        log_events=[
            LogEventConfig(name="evt", pattern="EVENT", state=StalledStateConfig())
        ]
    )
    monitor = SlurmLogMonitor(config)
    job = MonitoredJob(job_id="1", name="n", log_path=str(log_path), check_interval_seconds=1, state="r")
    
    # 1. Previous content prefix matches
    content = "PREVIOUS\nEVENT\n"
    previous = "PREVIOUS\n"
    
    events = monitor._extract_events(job, content, previous, source="log")
    assert len(events) == 1
    
    # 2. Previous content not prefix (log rotation? or partial read?)
    # Implementation: if previous and content.startswith(previous):
    # new_text = slice
    # Else: new_text = content
    
    content_new = "NEW START\nEVENT\n"
    previous_old = "OLD START\n"
    
    events_reset = monitor._extract_events(job, content_new, previous_old, source="log")
    assert len(events_reset) == 1
    
    # 3. No new text
    events_empty = monitor._extract_events(job, "SAME", "SAME", source="log")
    assert len(events_empty) == 0

def test_build_state_event_fallback():
    """Test _build_state_event falling back to default states."""
    config = SlurmLogMonitorConfig(log_path="l")
    monitor = SlurmLogMonitor(config)
    job = MonitoredJob(job_id="1", name="n", log_path="l", check_interval_seconds=1, state="r")
    
    # Known state name
    event = monitor._build_state_event(job, "stall", {})
    assert event is not None
    assert event.name == "stall"
    
    # Unknown state name -> None
    event_none = monitor._build_state_event(job, "mystery", {})
    assert event_none is None
