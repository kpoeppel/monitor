from __future__ import annotations

from pathlib import Path

import yaml
from compoconf import parse_config

import monitor.slurm_client  # noqa: F401
from monitor.job_client_protocol import JobClientInterface
from monitor.submission import SlurmJobConfig


def test_slurm_job_client_submit_with_fake_client(tmp_path: Path) -> None:
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n{command}\n",
        encoding="utf-8",
    )
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmClient",
            "base_client": {"class_name": "FakeSlurmClient"},
        },
    )
    job_config = parse_config(
        SlurmJobConfig,
        {
            "slurm": {
                "template_path": str(template_path),
                "script_dir": str(tmp_path / "scripts"),
                "log_dir": str(tmp_path / "logs"),
                "command": ["echo", "Hello"],
                "name": "test",
            },
            "log_path": str(tmp_path / "logs" / "test_%j.log"),
            "log_path_current": str(tmp_path / "logs" / "test_latest.log"),
            "name": "test",
        },
    )
    client = client_config.instantiate(JobClientInterface)
    job_id = client.submit(job_config)
    assert job_id == "1"
    statuses = client.squeue()
    assert statuses[job_id] == "PENDING"

    # sbatch does not create the --output directory, so the client must: a job
    # name that has never run would otherwise die at launch with
    # "Unable to open file".
    assert (tmp_path / "logs").is_dir()

    # The current.* symlinks are NOT created here any more. They are re-pointed
    # by the monitor when a job enters RUNNING, so a symlink shared by a
    # dependency chain tracks the running job rather than the last submitted one
    # (see tests/loop/test_current_symlinks.py).
    assert not (tmp_path / "logs" / "test_latest.log").exists()


def test_slurm_job_client_submit_dumps_the_resolved_config(tmp_path: Path) -> None:
    """``config_path`` records what the job was actually submitted with."""
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n{command}\n",
        encoding="utf-8",
    )
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmClient",
            "base_client": {"class_name": "FakeSlurmClient"},
        },
    )
    job_config = parse_config(
        SlurmJobConfig,
        {
            "slurm": {
                "template_path": str(template_path),
                "script_dir": str(tmp_path / "scripts"),
                "log_dir": str(tmp_path / "logs"),
                "command": ["echo", "Hello"],
                "name": "test",
            },
            "log_path": str(tmp_path / "logs" / "test_%j.log"),
            "config_path": str(tmp_path / "logs" / "test_%j.yaml"),
            "base_config": {"learning_rate": 0.001, "stage": "stable"},
            "name": "test",
        },
    )
    client = client_config.instantiate(JobClientInterface)
    job_id = client.submit(job_config)

    dumped = tmp_path / "logs" / f"test_{job_id}.yaml"
    assert dumped.is_file()
    assert yaml.safe_load(dumped.read_text()) == {"learning_rate": 0.001, "stage": "stable"}


def test_slurm_job_client_submit_array(tmp_path: Path) -> None:
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n{command}\n",
        encoding="utf-8",
    )
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmClient",
            "base_client": {"class_name": "FakeSlurmClient"},
        },
    )
    job_config = parse_config(
        SlurmJobConfig,
        {
            "slurm": {
                "template_path": str(template_path),
                "script_dir": str(tmp_path / "scripts"),
                "log_dir": str(tmp_path / "logs"),
                "command": ["echo", "Hello"],
                "name": "test-array",
                "array": True,
            },
            "log_path": str(tmp_path / "logs" / "test_%A_%a.log"),
            "log_path_current": str(tmp_path / "logs" / "test_latest_%a.log"),
            "name": "test-array",
        },
    )
    client = client_config.instantiate(JobClientInterface)
    job_ids = client.submit_array(job_config, indices=[0, 1])
    assert len(job_ids) == 2
    assert client.squeue().keys() >= set(job_ids)

    # As for a single job, the per-task current.* symlinks are the monitor's
    # business now (on the RUNNING transition), not the client's.
    for job_id in job_ids:
        array_idx = job_id.split("_")[-1]
        assert not (tmp_path / "logs" / f"test_latest_{array_idx}.log").exists()
