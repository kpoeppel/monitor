from __future__ import annotations

from pathlib import Path

from compoconf import parse_config

import monitor.slurm_job_client  # noqa: F401
from monitor.job_client_protocol import JobClientInterface


def test_slurm_job_client_submit_with_fake_client(tmp_path: Path) -> None:
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n{command}\n",
        encoding="utf-8",
    )
    client_config = parse_config(
        JobClientInterface.cfgtype,
        {
            "class_name": "SlurmJobClient",
            "slurm": {
                "template_path": str(template_path),
                "script_dir": str(tmp_path / "scripts"),
                "log_dir": str(tmp_path / "logs"),
            },
            "slurm_client": {"class_name": "FakeSlurmClient"},
        },
    )
    client = client_config.instantiate(JobClientInterface)
    job_id = client.submit(
        name="job",
        command=["echo", "hi"],
        log_path=str(tmp_path / "logs" / "job_%j.log"),
    )
    assert job_id == "1"
    statuses = client.squeue()
    assert statuses[job_id] == "PENDING"
