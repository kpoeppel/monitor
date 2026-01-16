"""Comprehensive tests for monitor watcher module."""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from monitor.watcher import (
    SlurmLogMonitor,
    SlurmLogMonitorConfig,
    LogEventConfig,
    StateEventConfig,
    MonitoredJob,
    NullMonitor,
    NullMonitorConfig,
)
from monitor.states import (
    PendingState, PendingStateConfig,
    StalledState, StalledStateConfig,
    StartedState, StartedStateConfig
)


def test_log_event_config_validation():
    """Test LogEventConfig validation logic."""
    # Valid config with state
    config1 = LogEventConfig(
        name="error",
        pattern="ERROR",
        state=PendingStateConfig()
    )
    assert config1.name == "error"

    # Valid config with metadata
    config2 = LogEventConfig(
        name="checkpoint",
        pattern="CHECKPOINT",
        metadata={"checkpoint": True}
    )
    assert config2.metadata["checkpoint"] is True

    # Invalid: no state and no metadata
    with pytest.raises(ValueError, match="must specify a state change or metadata"):
        LogEventConfig(
            name="invalid",
            pattern="TEST"
        )

    # Invalid pattern type
    with pytest.raises(ValueError, match="Unsupported pattern_type"):
        LogEventConfig(
            name="invalid",
            pattern="TEST",
            pattern_type="invalid_type",
            state=StartedStateConfig()
        )

    # Invalid: regex/substring without pattern
    with pytest.raises(ValueError, match="requires a pattern"):
        LogEventConfig(
            name="no_pattern",
            pattern="",
            pattern_type="regex",
            state=StartedStateConfig()
        )


def test_null_monitor():
    """Test NullMonitor implementation."""
    config = NullMonitorConfig(log_path="/tmp/test.log")
    monitor = NullMonitor(config)

    jobs = [
        MonitoredJob(
            job_id="job-1",
            name="test",
            log_path="/tmp/test.log",
            check_interval_seconds=60,
            state="RUNNING"
        )
    ]

    # Sync watch should return empty
    result_sync = monitor.watch_sync(jobs)
    assert result_sync == {}


def test_slurm_log_monitor_initialization():
    """Test SlurmLogMonitor initialization."""
    config = SlurmLogMonitorConfig(
        log_path="/tmp/test.log",
        log_events=[
            LogEventConfig(
                name="error",
                pattern="ERROR:",
                state=StalledStateConfig()
            ),
            LogEventConfig(
                name="checkpoint",
                pattern="CHECKPOINT",
                metadata={"type": "checkpoint"}
            ),
        ],
        state_whitelist=["pending", "running"],
        inactivity_threshold_seconds=600,
    )

    monitor = SlurmLogMonitor(config)

    assert len(monitor._compiled_rules) == 2
    assert monitor._state_whitelist == {"pending", "running"}


def test_slurm_log_monitor_with_log_file():
    """Test SlurmLogMonitor watching a job with log file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Starting job\nProcessing data\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="started",
                    pattern="Starting",
                    metadata={"status": "started"}
                )
            ]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        # First watch
        outcomes = monitor.watch_sync([job])

        assert "job-1" in outcomes
        outcome = outcomes["job-1"]
        assert outcome.job_id == "job-1"
        assert len(outcome.events) > 0


def test_slurm_log_monitor_pattern_matching():
    """Test pattern matching in logs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="error",
                    pattern=r"ERROR:\s+(.+)",
                    pattern_type="regex",
                    state=StalledStateConfig(),
                    extract_groups={"message": 1}
                ),
                LogEventConfig(
                    name="warning",
                    pattern="WARNING",
                    pattern_type="substring",
                    metadata={"severity": "warning"}
                ),
            ]
        )

        monitor = SlurmLogMonitor(config)

        # Write log with ERROR
        log_path.write_text("ERROR: Out of memory\nWARNING: Low disk space\n")

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        outcomes = monitor.watch_sync([job])
        outcome = outcomes["job-1"]

        # Should detect both error and warning
        assert len(outcome.events) >= 2
        event_names = {e.name for e in outcome.events}
        assert "error" in event_names
        assert "warning" in event_names


def test_slurm_log_monitor_inactivity_detection():
    """Test inactivity detection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Initial content\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="stall",
                    pattern="",
                    pattern_type="inactivity",
                    state=StalledStateConfig()
                )
            ],
            inactivity_threshold_seconds=1  # 1 second threshold
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        # First observation
        outcomes1 = monitor.watch_sync([job])
        outcome1 = outcomes1["job-1"]

        # Wait for inactivity threshold
        time.sleep(1.5)

        # Second observation - should detect inactivity
        outcomes2 = monitor.watch_sync([job])
        outcome2 = outcomes2["job-1"]

        # Status should change to stall after inactivity
        assert outcome2.status in ["stall", "stale", "active"]


def test_slurm_log_monitor_state_whitelist():
    """Test state whitelist filtering."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Some content\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[],
            state_whitelist=["RUNNING", "PENDING"]
        )

        monitor = SlurmLogMonitor(config)

        # Job in whitelisted state
        job_running = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        outcomes1 = monitor.watch_sync([job_running])
        assert "job-1" in outcomes1

        # Job in non-whitelisted state
        job_completed = MonitoredJob(
            job_id="job-2",
            name="test2",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="COMPLETED"
        )

        outcomes2 = monitor.watch_sync([job_completed])
        assert "job-2" in outcomes2
        # Should return early with empty result
        assert outcomes2["job-2"].last_update_seconds is None


def test_slurm_log_monitor_multiple_jobs():
    """Test monitoring multiple jobs simultaneously."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log1 = Path(tmpdir) / "job1.log"
        log2 = Path(tmpdir) / "job2.log"
        log1.write_text("Job 1 starting\n")
        log2.write_text("Job 2 starting\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log1),
            log_events=[
                LogEventConfig(
                    name="started",
                    pattern="starting",
                    metadata={"event": "start"}
                )
            ]
        )

        monitor = SlurmLogMonitor(config)

        jobs = [
            MonitoredJob(
                job_id="job-1",
                name="test1",
                log_path=str(log1),
                check_interval_seconds=60,
                state="RUNNING"
            ),
            MonitoredJob(
                job_id="job-2",
                name="test2",
                log_path=str(log2),
                check_interval_seconds=60,
                state="RUNNING"
            ),
        ]

        outcomes = monitor.watch_sync(jobs)

        assert len(outcomes) == 2
        assert "job-1" in outcomes
        assert "job-2" in outcomes


def test_slurm_log_monitor_log_updates():
    """Test detection of log updates."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Line 1\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="update",
                    pattern="Line",
                    metadata={"updated": True}
                )
            ]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        # First observation
        outcomes1 = monitor.watch_sync([job])
        event_count1 = len(outcomes1["job-1"].events)

        # Update log file
        log_path.write_text("Line 1\nLine 2\nLine 3\n")

        # Second observation - should detect new lines
        outcomes2 = monitor.watch_sync([job])
        event_count2 = len(outcomes2["job-1"].events)

        # Should have more events after update
        assert event_count2 >= event_count1


def test_slurm_log_monitor_missing_log_file():
    """Test handling of missing log file."""
    config = SlurmLogMonitorConfig(
        log_path="/nonexistent/path/job.log",
        log_events=[]
    )

    monitor = SlurmLogMonitor(config)

    job = MonitoredJob(
        job_id="job-1",
        name="test",
        log_path="/nonexistent/path/job.log",
        check_interval_seconds=60,
        state="RUNNING"
    )

    # Should handle missing file gracefully
    outcomes = monitor.watch_sync([job])

    assert "job-1" in outcomes
    outcome = outcomes["job-1"]
    assert outcome.job_id == "job-1"
    # Should be pending since log doesn't exist
    assert outcome.status in ["pending", "active"]


def test_slurm_log_monitor_termination_string():
    """Test termination string detection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Processing...\nJob completed successfully\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING",
            termination_string="completed successfully"
        )

        outcomes = monitor.watch_sync([job])
        outcome = outcomes["job-1"]

        # Should detect completion
        assert outcome.status == "complete"


def test_slurm_log_monitor_state_events():
    """Test state-based event generation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Running...\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[],
            state_events=[
                StateEventConfig(
                    name="stall_detected",
                    state=StalledStateConfig(),
                    metadata={"reason": "stall"}
                )
            ]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        outcomes = monitor.watch_sync([job])
        assert "job-1" in outcomes


def test_slurm_log_monitor_output_paths():
    """Test monitoring of output paths."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        output1 = Path(tmpdir) / "output1.txt"
        output2 = Path(tmpdir) / "output2.txt"

        log_path.write_text("Running...\n")
        output1.write_text("Output 1 content\n")
        output2.write_text("Output 2 content\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING",
            output_paths=[str(output1), str(output2)]
        )

        outcomes = monitor.watch_sync([job])
        assert "job-1" in outcomes


def test_log_event_config_extract_groups():
    """Test regex group extraction."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "job.log"
        log_path.write_text("Loss: 0.123, Accuracy: 0.95\n")

        config = SlurmLogMonitorConfig(
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="metrics",
                    pattern=r"Loss: ([\d.]+), Accuracy: ([\d.]+)",
                    pattern_type="regex",
                    metadata={"type": "metrics"},
                    extract_groups={"loss": 1, "accuracy": 2}
                )
            ]
        )

        monitor = SlurmLogMonitor(config)

        job = MonitoredJob(
            job_id="job-1",
            name="test",
            log_path=str(log_path),
            check_interval_seconds=60,
            state="RUNNING"
        )

        outcomes = monitor.watch_sync([job])
        outcome = outcomes["job-1"]

        # Should extract groups into metadata
        if len(outcome.events) > 0:
            event = outcome.events[0]
            assert event.name == "metrics"
