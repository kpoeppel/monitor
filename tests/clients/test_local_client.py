from __future__ import annotations

import time
from pathlib import Path

from monitor.local_client import LocalCommandClient, LocalJobConfig


def test_local_client_submit_and_complete(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = tmp_path / "job_%t.log"
    job_id = client.submit(
        LocalJobConfig(
            name="job",
            command=["bash", "-c", "echo hello"],
            log_path=str(log_path),
        )
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
        LocalJobConfig(
            name="sleep",
            command=["bash", "-c", "sleep 1"],
            log_path=str(log_path),
        )
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
        LocalJobConfig(
            name="log",
            command=["bash", "-c", "echo log"],
            log_path=str(log_path),
            log_path_current=str(log_current),
        )
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
        LocalJobConfig(
            name="nolog",
            command=["bash", "-c", "echo skip"],
            log_path=str(log_path),
            log_to_file=False,
        )
    )
    time.sleep(0.1)
    assert not list(tmp_path.glob("no_log_*.log"))
    statuses = client.squeue()
    assert statuses[job_id] == "COMPLETED"


def test_local_client_submit_array(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = str(tmp_path / "arr1_var_%t_%a.log")
    job_ids = client.submit_array(
        LocalJobConfig(
            name="arr",
            command=["bash", "-c", "echo $TASK_ID $TASK_NAME"],
            log_path=log_path,
            log_path_current=str(tmp_path / "arr1_cur_%a.log"),
            array_args=["0", "1"],
        ),
        indices=[0, 1],
    )
    assert len(job_ids) == 2
    time.sleep(0.1)
    statuses = client.squeue()
    assert all(statuses[job_id] == "COMPLETED" for job_id in job_ids)
    assert len(list(tmp_path.glob("arr1_var_*.log"))) == 2
    assert len(list(tmp_path.glob("arr1_cur_*.log"))) == 2
