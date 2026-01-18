"""Consolidated and modernized controller tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from monitor.controller import MonitorController, MonitorDecision
from monitor.watcher import MonitorOutcome, MonitorEvent
from monitor.submission import JobRegistration
from monitor.states import get_state
from monitor.utils.paths import expand_log_path
from monitor.conditions import AlwaysTrueConditionConfig


@pytest.fixture
def mocks():
    return {
        "monitor": MagicMock(),
        "slurm": MagicMock(),
        "store": MagicMock()
    }


@pytest.fixture
def controller(mocks):
    mocks["slurm"].squeue.return_value = {}
    return MonitorController(mocks["monitor"], mocks["slurm"], mocks["store"])


def test_controller_register_job(controller):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    
    jobs = list(controller.jobs())
    assert len(jobs) == 1
    assert jobs[0].job_id == "job-1"


def test_controller_observe_cycle(controller, mocks):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    
    # Mock submission
    state = controller._submission_manager.get_job("job-1")
    state.submitted = True
    
    mocks["slurm"].squeue.return_value = {"job-1": "RUNNING"}
    mocks["monitor"].watch_sync.return_value = {
        "job-1": MonitorOutcome(job_id="job-1", status="active", events=[
            MonitorEvent(job_id="job-1", name="progress", metadata={"iter": 1})
        ])
    }
    
    result = controller.observe_once_sync()
    assert len(result.events) > 0
    assert any(e.event == "progress" for e in result.events)


def test_controller_slurm_transition(controller, mocks):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")
    state.submitted = True
    state.last_slurm_state = "PENDING"
    
    mocks["slurm"].squeue.return_value = {"job-1": "RUNNING"}
    mocks["monitor"].watch_sync.return_value = {"job-1": MonitorOutcome(job_id="job-1", status="active")}
    
    result = controller.observe_once_sync()
    assert state.last_slurm_state == "RUNNING"
    assert state.state.key == "running"


def test_controller_job_completion(controller, mocks):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")
    state.submitted = True
    
    # Mock SLURM COMPLETED
    mocks["slurm"].squeue.return_value = {"job-1": "COMPLETED"}
    mocks["monitor"].watch_sync.return_value = {"job-1": MonitorOutcome(job_id="job-1", status="complete")}
    
    result = controller.observe_once_sync()
    assert result.decisions["job-1"].action == "success"


def test_controller_handle_state_change(controller):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    
    decision = controller.handle_state_change("job-1", "crash")
    assert decision.action == "stop"
    assert decision.reason == "crash detected"


def test_controller_expand_log_path():
    path = expand_log_path("job_%A_%a.log", "100_5")
    assert str(path) == "job_100_5.log"


def test_controller_event_record_reuse(controller):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")
    state.submitted = True
    controller._event_records = {}
    controller._event_index = {}

    event = MonitorEvent(
        job_id="job-1",
        name="checkpoint",
        metadata={"checkpoint_iteration": 1},
    )
    record1 = controller._get_or_create_event_record(state, event)
    assert controller._event_records[record1.event_id] is record1
    assert record1.count == 1

    record2 = controller._get_or_create_event_record(state, event)
    assert record1.event_id == record2.event_id
    assert record2.count == 2


def test_controller_build_job_metadata_outputs(controller):
    reg = JobRegistration(
        name="test",
        command=["s.sh"],
        log_path="/tmp/test.log",
        output_paths=["/tmp/out1.log", "/tmp/out2.log"],
        metadata={"custom": "value"},
    )
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")
    metadata = controller._build_job_metadata(state)
    assert metadata["custom"] == "value"
    assert metadata["job_name"] == "test"
    assert metadata["job_id"] == "job-1"
    assert metadata["output_dir"] == "/tmp"
    assert metadata["output_paths"] == ["/tmp/out1.log", "/tmp/out2.log"]


def test_controller_condition_started_ts_per_label(controller, monkeypatch):
    reg = JobRegistration(name="test", command=["s.sh"], log_path="test.log")
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")

    monkeypatch.setattr("monitor.condition_evaluator.time.time", lambda: 123.0)
    controller._condition_evaluator.evaluate(state, AlwaysTrueConditionConfig(), label="start")
    controller._condition_evaluator.evaluate(state, AlwaysTrueConditionConfig(), label="cancel")

    assert state.condition_data["start"]["started_ts"] == 123.0
    assert state.condition_data["cancel"]["started_ts"] == 123.0


def test_controller_prefers_log_path_current(controller, mocks):
    reg = JobRegistration(
        name="test",
        command=["s.sh"],
        log_path="job_%j.log",
        log_path_current="job_latest.log",
    )
    controller.register_job("job-1", reg)
    state = controller._submission_manager.get_job("job-1")
    state.submitted = True

    mocks["slurm"].squeue.return_value = {"job-1": "RUNNING"}
    mocks["monitor"].watch_sync.return_value = {"job-1": MonitorOutcome(job_id="job-1", status="active")}

    controller.observe_once_sync()

    ((monitored_jobs,), _) = mocks["monitor"].watch_sync.call_args
    assert monitored_jobs[0].log_path == "job_latest.log"
