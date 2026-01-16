import pytest
from unittest.mock import MagicMock
from monitor.controller import MonitorController, MonitorOutcome
from monitor.submission import JobRegistration
from monitor.watcher import BaseMonitor, MonitorEvent

# Define a simple class to use as a spec
class BaseSlurmClient:
    def squeue(self):
        pass
    
    def submit(self, name, script, log):
        return "job-123"

    def cancel(self, job_id):
        pass

    def remove(self, job_id):
        pass

def test_controller_register_job():
    monitor = MagicMock(spec=BaseMonitor)
    slurm = MagicMock(spec=BaseSlurmClient)
    # Mock config
    monitor.config = MagicMock()
    
    controller = MonitorController(monitor, slurm)
    
    registration = JobRegistration(
        name="test-job",
        script_path="script.sh",
        log_path="log.out"
    )
    
    controller.register_job("job-123", registration)
    
    jobs = list(controller.jobs())
    assert len(jobs) == 1
    assert jobs[0].job_id == "job-123"
    assert jobs[0].name == "test-job"

def test_controller_observe_once():
    monitor = MagicMock(spec=BaseMonitor)
    monitor.config = MagicMock()
    monitor.config.check_interval_seconds = 60
    
    # Mock watch_sync to return an outcome for job-123
    monitor.watch_sync.return_value = {
        "job-123": MonitorOutcome(
            job_id="job-123",
            status="active",
            last_update_seconds=10.0,
            metadata={}
        )
    }
    
    slurm = MagicMock(spec=BaseSlurmClient)
    slurm.squeue.return_value = {"job-123": "RUNNING"}
    slurm.submit.return_value = "job-123"
    
    controller = MonitorController(monitor, slurm)
    registration = JobRegistration(name="test", script_path="s", log_path="l")
    controller.register_job("job-123", registration)
    
    result = controller.observe_once_sync()
    
    jobs_map = {j.job_id: j for j in controller.jobs()}
    assert "job-123" in jobs_map
    monitor.watch_sync.assert_called_once()
    slurm.squeue.assert_called_once()
    
    # Check if slurm state transition event was recorded
    events = result.events
    # We expect a transition from NONE to RUNNING
    assert any(e.event == "slurm_state_transition" and e.metadata.get("slurm_state") == "RUNNING" for e in events)