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


def test_local_client_cancel_nonexistent() -> None:
    """Cancel() on unknown job ID should not raise."""
    client = LocalCommandClient()
    client.cancel("nonexistent-job-id")  # should be a no-op


def test_local_client_remove_nonexistent() -> None:
    """Remove() on unknown job ID should not raise."""
    client = LocalCommandClient()
    client.remove("nonexistent-job-id")  # should be a no-op


def test_local_client_failed_status(tmp_path: Path) -> None:
    """Job that exits non-zero should appear as FAILED in squeue."""
    client = LocalCommandClient()
    log_path = tmp_path / "fail_%t.log"
    job_id = client.submit(
        LocalJobConfig(
            name="fail",
            command=["bash", "-c", "exit 1"],
            log_path=str(log_path),
        )
    )
    time.sleep(0.1)
    statuses = client.squeue()
    assert statuses[job_id] == "FAILED"


def test_local_client_cleanup(tmp_path: Path) -> None:
    """Cleanup() cancels and removes all tracked jobs."""
    client = LocalCommandClient()
    log_path = tmp_path / "sleep_%t.log"
    job_id = client.submit(
        LocalJobConfig(
            name="sleep",
            command=["bash", "-c", "sleep 10"],
            log_path=str(log_path),
        )
    )
    assert job_id in client._jobs
    client.cleanup()
    assert job_id not in client._jobs


def test_local_client_submit_array(tmp_path: Path) -> None:
    client = LocalCommandClient()
    log_path = str(tmp_path / "arr1_var_%t_%a.log")
    job_ids = client.submit_array(
        LocalJobConfig(
            name="arr",
            command=["bash", "-c", "echo task:$TASK_ID"],
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
    for idx in range(2):
        symlink = tmp_path / f"arr1_cur_{idx}.log"
        assert symlink.is_symlink()
        target = symlink.resolve()
        assert target.exists()
        contents = target.read_text(encoding="utf-8")
        assert f"task:{idx}" in contents


def test_local_client_dumps_the_resolved_config(tmp_path: Path) -> None:
    """``config_path`` records what a job was actually submitted with."""
    import yaml

    client = LocalCommandClient()
    job_id = client.submit(
        LocalJobConfig(
            name="job",
            command=["bash", "-c", "true"],
            log_path=str(tmp_path / "job_%j.log"),
            config_path=str(tmp_path / "cfg" / "job_%j.yaml"),
            config_path_current=str(tmp_path / "cfg" / "current.yaml"),
            base_config={"stage": "stable", "lr": 0.001},
        )
    )

    dumped = tmp_path / "cfg" / f"job_{job_id}.yaml"
    assert yaml.safe_load(dumped.read_text()) == {"stage": "stable", "lr": 0.001}
    current = tmp_path / "cfg" / "current.yaml"
    assert current.is_symlink()
    assert current.resolve() == dumped.resolve()


def test_local_client_register_job_is_a_no_op_that_warns(tmp_path: Path, caplog) -> None:
    """A local process cannot be re-adopted: its Popen died with the monitor.

    Registering it anyway would be worse than skipping it -- the stored state
    would be reported forever and the job could never be seen to finish.
    """
    import logging

    client = LocalCommandClient()
    job = LocalJobConfig(name="orphan", command=["true"], log_path=str(tmp_path / "j.log"))

    with caplog.at_level(logging.WARNING, logger="monitor.local_client"):
        client.register_job(job, "17", state="RUNNING")

    assert client.squeue() == {}
    assert "Cannot re-adopt local job 17" in caplog.text
    assert "orphan" in caplog.text
