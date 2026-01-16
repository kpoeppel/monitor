from unittest.mock import MagicMock
import pytest
from monitor.executor import Executor
from monitor.submission import SubmissionManager, JobRegistration, JobRuntimeState
from monitor.job_client_protocol import JobClientProtocol
from monitor.watcher import BaseMonitor

class MockSlurmClient(JobClientProtocol):
    def submit(self, name, script_path, log_path):
        return "123"
    
    def submit_array(self, array_name, script_path, log_paths, task_names, start_index):
        return ["100_1"]
        
    def cancel(self, job_id):
        pass
    
    def remove(self, job_id):
        pass
    
    def squeue(self):
        return {}

def test_executor_stop_job():
    sub_mgr = MagicMock(spec=SubmissionManager)
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    reg = JobRegistration(name="test", script_path="s", log_path="l")
    state = JobRuntimeState(job_id="123", registration=reg, submitted=True)
    
    executor.stop_job(state)
    
    slurm.cancel.assert_called_with("123")
    slurm.remove.assert_called_with("123")
    sub_mgr.remove_job.assert_called_with("123")

def test_executor_restart_job_with_adjustments():
    sub_mgr = MagicMock(spec=SubmissionManager)
    # Configure update_job to mimic saving the state updates
    def update_mock(s):
        pass
    sub_mgr.update_job.side_effect = update_mock
    
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    reg = JobRegistration(name="test", script_path="s", log_path="l", metadata={"a": 1})
    state = JobRuntimeState(job_id="123", registration=reg, submitted=True)
    
    adjustments = {
        "script_path": "new_s",
        "log_path": "new_l",
        "metadata": {"b": 2}
    }
    
    executor.restart_job(state, adjustments)
    
    slurm.cancel.assert_called_with("123")
    slurm.remove.assert_called_with("123")
    
    assert state.registration.script_path == "new_s"
    assert state.registration.log_path == "new_l"
    assert state.registration.metadata["a"] == 1
    assert state.registration.metadata["b"] == 2
    assert state.submitted is False
    assert "pending" in state.job_id

def test_executor_finalize_job():
    sub_mgr = MagicMock(spec=SubmissionManager)
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    executor.finalize_job("123")
    
    sub_mgr.remove_job.assert_called_with("123")
    slurm.remove.assert_called_with("123")

def test_executor_submit_array_job():
    sub_mgr = MagicMock(spec=SubmissionManager)
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    slurm.submit_array.return_value = ["100_1"]
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    reg = JobRegistration(name="array_test", script_path="s", log_path="l")
    # Simulate a job ID that looks like an array ID (from previous submission or config)
    # The logic checks if "_" in job_id to trigger array submission logic
    state = JobRuntimeState(job_id="100_1", registration=reg)
    
    job_id = executor.start_job(state)
    
    slurm.submit_array.assert_called()
    assert job_id == "100_1"

def test_executor_submit_array_job_failure():
    sub_mgr = MagicMock(spec=SubmissionManager)
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    # Return empty list indicating failure
    slurm.submit_array.return_value = []
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    reg = JobRegistration(name="array_test", script_path="s", log_path="l")
    state = JobRuntimeState(job_id="100_1", registration=reg)
    
    with pytest.raises(RuntimeError, match="Array job submission failed"):
        executor.start_job(state)

def test_executor_start_job_id_change():
    sub_mgr = MagicMock(spec=SubmissionManager)
    slurm = MagicMock(spec=MockSlurmClient)
    monitor = MagicMock(spec=BaseMonitor)
    
    # Slurm returns new ID
    slurm.submit.return_value = "new_123"
    
    executor = Executor(sub_mgr, slurm, monitor)
    
    reg = JobRegistration(name="test", script_path="s", log_path="l")
    # Initial state has old ID
    state = JobRuntimeState(job_id="old123", registration=reg)
    
    executor.start_job(state)
    
    # Verify old ID removed
    sub_mgr.remove_job.assert_called_with("old123")
    # Verify state updated
    sub_mgr.update_job.assert_called_with(state)
    assert state.job_id == "new_123"
