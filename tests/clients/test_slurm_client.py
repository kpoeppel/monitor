from __future__ import annotations

from pathlib import Path

from compoconf import parse_config

import monitor.slurm_client  # noqa: F401
from monitor.job_client_protocol import JobClientInterface
from monitor.submission import SlurmJobConfig
from monitor.utils.paths import expand_log_path


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
    symlink = tmp_path / "logs" / "test_latest.log"
    assert symlink.is_symlink()
    assert symlink.resolve() == expand_log_path(str(tmp_path / "logs" / "test_%j.log"), job_id)


def test_slurm_job_client_submit_array_updates_log_symlinks(tmp_path: Path) -> None:
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

    for job_id in job_ids:
        array_idx = job_id.split("_")[-1]
        symlink = tmp_path / "logs" / f"test_latest_{array_idx}.log"
        assert symlink.is_symlink()
        expected = expand_log_path(str(tmp_path / "logs" / "test_%A_%a.log"), job_id)
        assert symlink.resolve() == expected
