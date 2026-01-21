from __future__ import annotations

from pathlib import Path

from monitor.app import build_loop, parse_app_config


def test_build_loop_syncs_jobs(tmp_path) -> None:
    payload = {
        "monitor": {"class_name": "MonitorLoop", "poll_interval_seconds": 1},
        "state_store_dir": str(tmp_path / "state"),
        "client": {"class_name": "LocalCommandClient"},
        "jobs": [
            {
                "job_id": "job1",
                "registration": {
                    "class_name": "LocalJob",
                    "name": "job1",
                    "command": ["echo", "hi"],
                    "log_path": str(tmp_path / "job1_%j.log"),
                    "log_events": [
                        {
                            "class_name": "LogEvent",
                            "name": "ready",
                            "pattern": "READY",
                            "action": {"class_name": "LogAction", "message": "ok"},
                        }
                    ],
                },
            }
        ],
    }
    app_config = parse_app_config(payload)
    loop = build_loop(app_config)
    assert app_config.jobs
    assert Path(app_config.state_store_dir).exists()
    assert loop.poll_interval_seconds == 1
