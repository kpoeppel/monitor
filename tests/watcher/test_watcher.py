"Consolidated and modernized watcher tests."

from __future__ import annotations

import os
from pathlib import Path
import time
import pytest

from monitor.watcher import (
    SlurmLogMonitor,
    SlurmLogMonitorConfig,
    LogEventConfig,
    InactivityRuleConfig,
    MonitoredJob,
    NullMonitor,
    NullMonitorConfig,
)
from monitor.states import StalledStateConfig, SuccessStateConfig


@pytest.fixture
def log_file(tmp_path):
    p = tmp_path / "job.log"
    p.write_text("initial\n")
    return p


def test_slurm_log_monitor_basic_events(log_file):
    config = SlurmLogMonitorConfig()
    monitor = SlurmLogMonitor(config)
    job = MonitoredJob(
        job_id="1",
        name="j",
        log_path=str(log_file),
        check_interval_seconds=1,
        state="running",
        log_events=[
            LogEventConfig(name="error", pattern="ERROR", state=StalledStateConfig()),
            LogEventConfig(
                name="progress",
                pattern=r"Iteration (\d+)",
                pattern_type="regex",
                extract_groups={"it": 1},
            ),
        ],
    )
    
    # First check
    outcomes = monitor.watch_sync([job])
    assert outcomes["1"].status == "active"
    assert len(outcomes["1"].events) == 0
    
    # Append events
    log_file.write_text("initial\nERROR detected\nIteration 10\n")
    outcomes = monitor.watch_sync([job])
    assert len(outcomes["1"].events) == 2
    assert outcomes["1"].events[0].name == "error"
    assert outcomes["1"].events[1].metadata["it"] == "10"


def test_slurm_log_monitor_inactivity(log_file):
    config = SlurmLogMonitorConfig()
    monitor = SlurmLogMonitor(config)
    job = MonitoredJob(
        job_id="1",
        name="j",
        log_path=str(log_file),
        check_interval_seconds=1,
        state="running",
        inactivity_threshold_seconds=0.1,
        inactivity_rules=[InactivityRuleConfig(name="stall_event", threshold_seconds=0.05)],
    )
    
    # Backdate the log file to trigger stall
    past = time.time() - 1.0
    os.utime(log_file, (past, past))
    
    outcomes = monitor.watch_sync([job])
    assert outcomes["1"].status == "stall"
    assert any(e.name == "stall_event" for e in outcomes["1"].events)


def test_null_monitor():
    config = NullMonitorConfig()
    monitor = NullMonitor(config)
    job = MonitoredJob(job_id="1", name="j", log_path="none", check_interval_seconds=1, state="running")
    
    outcomes = monitor.watch_sync([job])
    assert outcomes["1"].status == "pending"


def test_slurm_log_monitor_missing_log():
    config = SlurmLogMonitorConfig()
    monitor = SlurmLogMonitor(config)
    job = MonitoredJob(job_id="1", name="j", log_path="nonexistent.log", check_interval_seconds=1, state="running")
    
    outcomes = monitor.watch_sync([job])
    assert outcomes["1"].status == "pending"

def test_extract_metadata_groups():
    from monitor.watcher import _extract_metadata
    import re
    
    pattern = re.compile(r"Val: (\d+)")
    match = pattern.search("Val: 42")
    rule = LogEventConfig(extract_groups={"v": 1})
    
    meta = _extract_metadata(match, rule)
    assert meta["v"] == "42"
