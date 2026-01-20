from __future__ import annotations

from pathlib import Path

from compoconf import parse_config

import monitor.slurm_job_client  # noqa: F401
from monitor.job_client_protocol import JobClientInterface
from slurm_gen import SlurmConfig


def test_slurm_gen_client_submits_and_updates_symlink(tmp_path: Path):
    template = """#!/bin/bash
{sbatch_directives}
#SBATCH --job-name={job_name}
#SBATCH --output={log_path}

{command}
"""
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(template)

    slurm_config = {
        "template_path": str(template_path),
        "script_dir": str(tmp_path / "scripts"),
        "log_dir": str(tmp_path / "logs"),
    }
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmJobClient",
            "slurm": slurm_config,
            "slurm_client": {"class_name": "FakeSlurmClient"},
        },
    )
    client = client_config.instantiate(JobClientInterface)

    log_path = str(tmp_path / "logs" / "train_%j.log")
    latest_path = tmp_path / "logs" / "latest.log"

    job_id = client.submit(
        "train",
        ["python", "train.py"],
        log_path,
        extra_args=["--lr=0.1"],
        log_path_current=str(latest_path),
    )

    assert job_id == "1"
    script_dir = tmp_path / "scripts"
    scripts = list(script_dir.glob("train_*.sbatch"))
    assert scripts, "expected generated sbatch script"
    rendered = scripts[0].read_text()
    assert "python train.py --lr=0.1" in rendered

    assert latest_path.is_symlink()
    assert latest_path.resolve().name == "train_1.log"


def test_slurm_gen_client_accepts_slurm_config(tmp_path: Path):
    template = """#!/bin/bash
{sbatch_directives}
{command}
"""
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(template)

    slurm_config = SlurmConfig(
        template_path=str(template_path),
        script_dir=str(tmp_path / "scripts"),
        log_dir=str(tmp_path / "logs"),
        command=["python", "train.py"],
    )
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmJobClient",
            "slurm": slurm_config,
            "slurm_client": {"class_name": "FakeSlurmClient"},
        },
    )
    client = client_config.instantiate(JobClientInterface)

    log_path = str(tmp_path / "logs" / "train_%j.log")
    job_id = client.submit(
        "train",
        ["python", "train.py"],
        log_path,
        slurm=slurm_config,
    )
    assert job_id == "1"
