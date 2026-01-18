from __future__ import annotations

import json
import sys
from pathlib import Path

from monitor.persistence import MonitorStateStore

ROOT = Path(__file__).resolve().parents[2]


def _write_config(state_dir: Path) -> None:
    store = MonitorStateStore(state_dir)
    store.save_config({"monitor": {"class_name": "NullMonitor"}, "jobs": []})


def test_monitor_control_submit_json(tmp_path, monkeypatch):
    _write_config(tmp_path)
    job_payload = {
        "job_id": "job1",
        "registration": {
            "name": "job1",
            "command": ["bash", "./run.sh"],
            "log_path": "job1.log",
        },
    }
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(job_payload))

    sys_path = str(ROOT)
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    import scripts.monitor_control as monitor_control

    monkeypatch.setattr(
        "sys.argv",
        [
            "monitor_control.py",
            "--state-dir",
            str(tmp_path),
            "submit",
            "--job-json",
            str(job_path),
        ],
    )
    monitor_control.main()

    store = MonitorStateStore(tmp_path)
    config = store.load_config()
    assert config is not None
    assert config["jobs"][0]["job_id"] == "job1"


def test_monitor_control_submit_yaml(tmp_path, monkeypatch):
    _write_config(tmp_path)
    job_payload = {
        "job_id": "job2",
        "registration": {
            "name": "job2",
            "command": ["bash", "./run.sh"],
            "log_path": "job2.log",
        },
    }
    job_path = tmp_path / "job.yaml"
    job_path.write_text(
        "job_id: job2\n"
        "registration:\n"
        "  name: job2\n"
        "  command: [\"bash\", \"./run.sh\"]\n"
        "  log_path: job2.log\n"
    )

    sys_path = str(ROOT)
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    import scripts.monitor_control as monitor_control

    monkeypatch.setattr(
        "sys.argv",
        [
            "monitor_control.py",
            "--state-dir",
            str(tmp_path),
            "submit",
            "--job-yaml",
            str(job_path),
        ],
    )
    monitor_control.main()

    store = MonitorStateStore(tmp_path)
    config = store.load_config()
    assert config is not None
    assert config["jobs"][0]["job_id"] == "job2"


def test_monitor_control_cancel(tmp_path, monkeypatch):
    store = MonitorStateStore(tmp_path)
    store.save_config(
        {
            "monitor": {"class_name": "NullMonitor"},
            "jobs": [
                {
                    "job_id": "job3",
                    "registration": {"name": "job3", "command": ["echo", "hi"], "log_path": "job3.log"},
                }
            ],
        }
    )

    sys_path = str(ROOT)
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    import scripts.monitor_control as monitor_control

    monkeypatch.setattr(
        "sys.argv",
        [
            "monitor_control.py",
            "--state-dir",
            str(tmp_path),
            "cancel",
            "--job-id",
            "job3",
        ],
    )
    monitor_control.main()

    config = store.load_config()
    assert config is not None
    assert config["jobs"] == []
