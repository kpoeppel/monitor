from __future__ import annotations

from types import SimpleNamespace

import monitor.slurm_client as slurm_client


def test_slurm_client_submit_parses_job_id(monkeypatch) -> None:
    def fake_run_command(args):
        return SimpleNamespace(returncode=0, stdout="Submitted batch job 12345\n", stderr="")

    monkeypatch.setattr(slurm_client, "run_command", fake_run_command)
    client = slurm_client.SlurmClient(slurm_client.SlurmClientConfig())
    client._jobs["12345"] = slurm_client.SlurmJob(
        job_id="12345",
        name="job",
        script_path="script.sbatch",
        log_path="job.log",
    )
    job_id = client.submit("job", "script.sbatch", "job.log")
    assert job_id == "12345"


def test_slurm_client_submit_failure(monkeypatch) -> None:
    def fake_run_command(args):
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr(slurm_client, "run_command", fake_run_command)
    client = slurm_client.SlurmClient(slurm_client.SlurmClientConfig())
    try:
        client.submit("job", "script.sbatch", "job.log")
    except RuntimeError as exc:
        assert "sbatch failed" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_slurm_client_squeue_parses(monkeypatch) -> None:
    output = "12345 RUNNING\n67890 PENDING\n"

    def fake_run_command(args):
        return SimpleNamespace(returncode=0, stdout=output, stderr="")

    monkeypatch.setattr(slurm_client, "run_command", fake_run_command)
    client = slurm_client.SlurmClient(slurm_client.SlurmClientConfig())
    client._jobs["12345"] = slurm_client.SlurmJob(
        job_id="12345",
        name="job",
        script_path="script.sbatch",
        log_path="job.log",
    )
    client._jobs["67890"] = slurm_client.SlurmJob(
        job_id="67890",
        name="job2",
        script_path="script2.sbatch",
        log_path="job2.log",
    )
    assert client.squeue() == {"12345": "RUNNING", "67890": "PENDING"}
