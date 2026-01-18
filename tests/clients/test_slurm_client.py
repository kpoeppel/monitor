from __future__ import annotations

from pathlib import Path

from monitor.slurm_client import FakeSlurmClient, FakeSlurmClientConfig


def test_fake_slurm_submit_and_squeue(tmp_path: Path) -> None:
    client = FakeSlurmClient(FakeSlurmClientConfig())
    job_id = client.submit("job", "script.sbatch", str(tmp_path / "job.log"))
    statuses = client.squeue()
    assert statuses[job_id] == "PENDING"
    client.set_state(job_id, "RUNNING")
    statuses = client.squeue()
    assert statuses[job_id] == "RUNNING"


def test_fake_slurm_submit_array(tmp_path: Path) -> None:
    client = FakeSlurmClient(FakeSlurmClientConfig())
    job_ids = client.submit_array(
        array_name="arr",
        script_path="array.sbatch",
        log_paths=[str(tmp_path / "a1.log"), str(tmp_path / "a2.log")],
        task_names=["t1", "t2"],
        start_index=1,
    )
    assert job_ids == ["1_1", "1_2"]
    statuses = client.squeue()
    assert statuses["1_1"] == "PENDING"


def test_fake_slurm_cancel_remove() -> None:
    client = FakeSlurmClient(FakeSlurmClientConfig())
    job_id = client.submit("job", "script.sbatch", "job.log")
    client.cancel(job_id)
    assert client.squeue()[job_id] == "CANCELLED"
    client.remove(job_id)
    assert job_id not in client.squeue()


def test_fake_slurm_lookup_and_register() -> None:
    client = FakeSlurmClient(FakeSlurmClientConfig())
    job_id = client.register_job("42", "name", "script.sbatch", "log.log", state="RUNNING")
    assert job_id == "42"
    assert client.job_ids_by_name("name") == ["42"]
    job = client.get_job("42")
    assert job.state == "RUNNING"
