from __future__ import annotations

from monitor.app import build_loop, parse_app_config
from monitor.loop import JobFileStore


def test_build_loop_syncs_jobs(tmp_path) -> None:
    payload = {
        "monitor": {"class_name": "MonitorLoop", "poll_interval_seconds": 1},
        "state_store_dir": str(tmp_path / "state"),
        "client": {"class_name": "LocalCommandClient"},
        "jobs": [
            {
                "job_id": "job1",
                "registration": {
                    "class_name": "LocalJobRegistration",
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
    store = JobFileStore(app_config.state_store_dir)
    record = store.load("job1")
    assert record is not None
    assert loop.poll_interval_seconds == 1
