from __future__ import annotations

import time
from pathlib import Path

from monitor.app import build_controller, load_app_config


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_yaml_config_local_job(tmp_path: Path) -> None:
    script_path = tmp_path / "job.sh"
    script_path.write_text("#!/bin/bash\necho READY\n", encoding="utf-8")
    script_path.chmod(0o755)

    log_path = tmp_path / "job.log"
    config_path = tmp_path / "config.yaml"
    _write_yaml(
        config_path,
        f"""monitor:
  class_name: SlurmLogMonitor
  poll_interval_seconds: 0.1
jobs:
  - job_id: job1
    registration:
      class_name: LocalJobRegistration
      name: job1
      command: ["bash", "{script_path}"]
      log_path: "{log_path}"
      log_events:
        - class_name: LogEvent
          name: ready
          pattern: READY
          pattern_type: substring
""",
    )

    app_config = load_app_config(config_path)
    controller = build_controller(app_config)

    start = time.time()
    seen_ready = False
    while time.time() - start < 2:
        result = controller.observe_once_sync()
        if any(event.event == "ready" for event in result.events):
            seen_ready = True
            break
        time.sleep(0.05)

    assert seen_ready, "expected READY event from YAML-defined job"


def test_yaml_config_slurm_fake(tmp_path: Path) -> None:
    template_path = tmp_path / "job.sbatch"
    template_path.write_text(
        "#!/bin/bash\n{sbatch_directives}\n#SBATCH --job-name={job_name}\n#SBATCH --output={log_path}\n\n{command}\n",
        encoding="utf-8",
    )
    log_path = tmp_path / "logs" / "train_%j.log"
    config_path = tmp_path / "config.yaml"
    _write_yaml(
        config_path,
        f"""monitor:
  class_name: SlurmLogMonitor
  poll_interval_seconds: 0.1
client:
  class_name: SlurmGenClient
  slurm:
    template_path: "{template_path}"
    script_dir: "{tmp_path / "scripts"}"
    log_dir: "{tmp_path / "logs"}"
  slurm_client:
    class_name: FakeSlurmClient
    persist_artifacts: true
jobs:
  - job_id: train
    registration:
      class_name: SlurmJobRegistration
      name: train
      command: ["python", "train.py"]
      log_path: "{log_path}"
""",
    )

    app_config = load_app_config(config_path)
    controller = build_controller(app_config)

    controller.observe_once_sync()

    slurm_client = controller._slurm
    statuses = slurm_client.squeue()
    assert statuses, "expected fake slurm job to be submitted"
