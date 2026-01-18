from __future__ import annotations

from pathlib import Path

from slurm_gen.generator import generate_script
from slurm_gen.schema import SlurmConfig, SbatchConfig


def test_generate_script_renders_template(tmp_path: Path):
    template = """#!/bin/bash
{sbatch_directives}
#SBATCH --job-name={job_name}
#SBATCH --output={log_path}

{env_exports}
{command}
"""
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(template)

    slurm_config = SlurmConfig(
        template_path=str(template_path),
        script_dir=str(tmp_path / "scripts"),
        log_dir=str(tmp_path / "logs"),
        sbatch=SbatchConfig(nodes=2, time="0-00:10:00"),
    )

    script_path = generate_script(
        slurm_config,
        job_name="demo",
        log_path="logs/demo_%j.log",
        command=["python", "train.py"],
        extra_args=["--lr=0.1"],
        now_ms=123,
    )

    rendered = script_path.read_text()
    assert "#SBATCH --nodes=2" in rendered
    assert "#SBATCH --time=0-00:10:00" in rendered
    assert "#SBATCH --job-name=demo" in rendered
    assert "#SBATCH --output=logs/demo_%j.log" in rendered
    assert "python train.py --lr=0.1" in rendered
