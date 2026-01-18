from __future__ import annotations

import sys
from pathlib import Path

def test_monitor_loop_slurm_gen_submission(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    slurm_gen_path = root / "slurm_gen" / "src"
    if str(slurm_gen_path) not in sys.path:
        sys.path.insert(0, str(slurm_gen_path))

    from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop
    from monitor.slurm_gen_client import SlurmGenClient, SlurmGenClientConfig
    from monitor.submission import SlurmJobRegistrationConfig
    from slurm_gen import SlurmConfig

    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n{command}\n",
        encoding="utf-8",
    )

    slurm_config = {
        "template_path": str(template_path),
        "script_dir": str(tmp_path / "scripts"),
        "log_dir": str(tmp_path / "logs"),
        "command": ["python", "train.py", "--profile=fast"],
    }
    client_config = SlurmGenClientConfig(
        slurm=slurm_config,
        slurm_client={"class_name": "FakeSlurmClient"},
    )
    client = SlurmGenClient(client_config)
    store = JobFileStore(tmp_path / "state")
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    record = JobRecordConfig(
        job_id="slurm-job",
        registration=SlurmJobRegistrationConfig(
            name="slurm-job",
            command=["python", "train.py"],
            log_path=str(tmp_path / "logs" / "train_%j.log"),
            slurm=SlurmConfig(**slurm_config),
        ),
    )
    store.upsert(record)

    loop.observe_once()
    loaded = store.load("slurm-job")
    assert loaded is not None
    assert loaded.runtime.submitted is True
    assert loaded.runtime.runtime_job_id is not None
