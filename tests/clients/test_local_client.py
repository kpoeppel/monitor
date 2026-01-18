from __future__ import annotations

import time
from pathlib import Path

from monitor.local_client import LocalCommandClient


def test_local_client_submit_and_complete(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = tmp_path / "job_%t.log"
    job_id = client.submit(
        name="job",
        command=["bash", "-c", "echo hello"],
        log_path=str(log_path),
    )
    statuses = client.squeue()
    assert statuses[job_id] in {"RUNNING", "COMPLETED"}
    time.sleep(0.1)
    statuses = client.squeue()
    assert statuses[job_id] == "COMPLETED"
    assert list(tmp_path.glob("job_*.log"))


def test_local_client_running_then_cancel(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = tmp_path / "sleep_%t.log"
    job_id = client.submit(
        name="sleep",
        command=["bash", "-c", "sleep 1"],
        log_path=str(log_path),
    )
    statuses = client.squeue()
    assert statuses[job_id] == "RUNNING"
    client.cancel(job_id)
    job = client._jobs[job_id]
    assert job.process is not None
    assert job.process.poll() is not None


def test_local_client_log_path_current(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = tmp_path / "current_%t.log"
    log_current = tmp_path / "latest.log"
    job_id = client.submit(
        name="log",
        command=["bash", "-c", "echo log"],
        log_path=str(log_path),
        log_path_current=str(log_current),
    )
    time.sleep(0.1)
    assert log_current.exists()
    resolved = Path(log_current.readlink())
    assert resolved.exists()
    statuses = client.squeue()
    assert statuses[job_id] == "COMPLETED"


def test_local_client_log_to_file_false(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = tmp_path / "no_log_%t.log"
    job_id = client.submit(
        name="nolog",
        command=["bash", "-c", "echo skip"],
        log_path=str(log_path),
        log_to_file=False,
    )
    time.sleep(0.1)
    assert not list(tmp_path.glob("no_log_*.log"))
    statuses = client.squeue()
    assert statuses[job_id] == "COMPLETED"


def test_local_client_submit_array(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_paths = [str(tmp_path / "arr1_%t.log"), str(tmp_path / "arr2_%t.log")]
    job_ids = client.submit_array(
        array_name="arr",
        command=["bash", "-c", "echo $TASK_ID $TASK_NAME"],
        log_paths=log_paths,
        task_names=["a", "b"],
    )
    assert len(job_ids) == 2
    time.sleep(0.1)
    statuses = client.squeue()
    assert all(statuses[job_id] == "COMPLETED" for job_id in job_ids)
    assert list(tmp_path.glob("arr1_*.log"))
    assert list(tmp_path.glob("arr2_*.log"))
