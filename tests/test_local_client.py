"""Tests for LocalCommandClient."""

import pytest
import tempfile
from pathlib import Path
from monitor.local_client import LocalCommandClient, LocalJob
from monitor.job_client_protocol import JobClientProtocol


def test_local_client_implements_protocol():
    """Test that LocalCommandClient implements JobClientProtocol."""
    client = LocalCommandClient()
    assert isinstance(client, JobClientProtocol)


def test_submit_single_job():
    """Test submitting a single local job."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        log_path = Path(tmpdir) / "test.log"

        # Create a simple script
        script_path.write_text("#!/bin/bash\necho 'Hello World'\nsleep 0.1\n")

        client = LocalCommandClient()
        job_id = client.submit("test-job", str(script_path), str(log_path))

        assert job_id == "0"
        assert log_path.exists()

        # Wait for job to complete
        import time
        time.sleep(0.5)

        statuses = client.squeue()
        assert job_id in statuses
        assert statuses[job_id] in ["RUNNING", "COMPLETED"]


def test_submit_array_jobs():
    """Test submitting array jobs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"

        # Script that prints task info
        script_path.write_text("#!/bin/bash\necho \"Task $TASK_NAME (ID: $TASK_ID)\"")

        client = LocalCommandClient()
        log_paths = [str(Path(tmpdir) / f"task{i}.log") for i in range(3)]
        task_names = ["task0", "task1", "task2"]

        job_ids = client.submit_array(
            "array-job", str(script_path), log_paths, task_names
        )

        assert len(job_ids) == 3
        assert job_ids == ["0", "1", "2"]

        # All log files should exist
        for log_path in log_paths:
            assert Path(log_path).exists()


def test_cancel_job():
    """Test canceling a running job."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        log_path = Path(tmpdir) / "test.log"

        # Long-running script
        script_path.write_text("#!/bin/bash\nsleep 10\n")

        client = LocalCommandClient()
        job_id = client.submit("test-job", str(script_path), str(log_path))

        import time
        time.sleep(0.2)  # Let it start

        # Should be running
        statuses = client.squeue()
        assert statuses[job_id] == "RUNNING"

        # Cancel it
        client.cancel(job_id)
        time.sleep(0.2)

        # Should be cancelled
        statuses = client.squeue()
        assert statuses[job_id] in ["CANCELLED", "FAILED"]


def test_remove_job():
    """Test removing a job from tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        log_path = Path(tmpdir) / "test.log"
        script_path.write_text("#!/bin/bash\necho 'done'\n")

        client = LocalCommandClient()
        job_id = client.submit("test-job", str(script_path), str(log_path))

        assert job_id in client.squeue()

        client.remove(job_id)

        assert job_id not in client.squeue()


def test_job_ids_by_name():
    """Test retrieving job IDs by name."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        script_path.write_text("#!/bin/bash\necho 'done'\n")

        client = LocalCommandClient()

        job_id1 = client.submit("job-a", str(script_path), str(Path(tmpdir) / "1.log"))
        job_id2 = client.submit("job-a", str(script_path), str(Path(tmpdir) / "2.log"))
        job_id3 = client.submit("job-b", str(script_path), str(Path(tmpdir) / "3.log"))

        job_ids = client.job_ids_by_name("job-a")
        assert set(job_ids) == {job_id1, job_id2}
        assert job_id3 not in job_ids


def test_get_job():
    """Test getting job details."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        log_path = Path(tmpdir) / "test.log"
        script_path.write_text("#!/bin/bash\necho 'done'\n")

        client = LocalCommandClient()
        job_id = client.submit("test-job", str(script_path), str(log_path))

        job = client.get_job(job_id)

        assert isinstance(job, LocalJob)
        assert job.job_id == job_id
        assert job.name == "test-job"
        assert job.script_path == str(script_path)
        assert job.log_path == str(log_path)


def test_get_job_not_found():
    """Test getting non-existent job raises KeyError."""
    client = LocalCommandClient()

    with pytest.raises(KeyError):
        client.get_job("nonexistent")


def test_failed_job_status():
    """Test that failed jobs are detected."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        log_path = Path(tmpdir) / "test.log"

        # Script that exits with error
        script_path.write_text("#!/bin/bash\nexit 1\n")

        client = LocalCommandClient()
        job_id = client.submit("failing-job", str(script_path), str(log_path))

        import time
        time.sleep(0.5)  # Wait for it to fail

        statuses = client.squeue()
        assert statuses[job_id] == "FAILED"

        job = client.get_job(job_id)
        assert job.return_code == 1


def test_cleanup():
    """Test cleanup terminates all jobs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "test.sh"
        script_path.write_text("#!/bin/bash\nsleep 10\n")

        client = LocalCommandClient()

        # Submit multiple jobs
        for i in range(3):
            client.submit(f"job-{i}", str(script_path), str(Path(tmpdir) / f"{i}.log"))

        import time
        time.sleep(0.2)

        # All should be running
        statuses = client.squeue()
        assert len(statuses) == 3

        # Cleanup
        client.cleanup()

        # All should be removed
        assert len(client.squeue()) == 0
