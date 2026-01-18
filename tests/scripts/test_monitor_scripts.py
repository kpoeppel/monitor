from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from monitor.loop import JobFileStore, JobRecordConfig
from monitor.submission import LocalJobRegistrationConfig


def test_monitor_control_submit_and_cancel(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    job_payload = {
        "class_name": "JobRecord",
        "job_id": "job1",
        "registration": {
            "class_name": "LocalJobRegistration",
            "name": "job1",
            "command": ["echo", "hi"],
            "log_path": str(tmp_path / "job1_%j.log"),
        },
    }
    job_json = tmp_path / "job.json"
    job_json.write_text(json.dumps(job_payload), encoding="utf-8")

    script = Path("scripts/monitor_control.py")
    subprocess.run(
        [sys.executable, str(script), "--state-dir", str(state_dir), "submit", "--job-json", str(job_json)],
        check=True,
    )
    assert (state_dir / "job1.job.json").exists()

    subprocess.run(
        [sys.executable, str(script), "--state-dir", str(state_dir), "cancel", "--job-id", "job1"],
        check=True,
    )
    assert not (state_dir / "job1.job.json").exists()


def test_monitor_status_json(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    store = JobFileStore(state_dir)
    store.upsert(
        JobRecordConfig(
            job_id="job2",
            registration=LocalJobRegistrationConfig(
                name="job2",
                command=["echo", "ok"],
                log_path=str(tmp_path / "job2_%j.log"),
            ),
        )
    )

    script = Path("scripts/monitor_status.py")
    result = subprocess.run(
        [sys.executable, str(script), "--state-dir", str(state_dir), "--json"],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["jobs"][0]["job_id"] == "job2"


def test_monitor_cleanup_done_only(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    store = JobFileStore(state_dir)
    record = JobRecordConfig(
        job_id="job3",
        registration=LocalJobRegistrationConfig(
            name="job3",
            command=["echo", "ok"],
            log_path=str(tmp_path / "job3_%j.log"),
        ),
    )
    record.runtime.last_status = "COMPLETED"
    store.upsert(record)

    script = Path("scripts/monitor_cleanup.py")
    subprocess.run(
        [sys.executable, str(script), "--state-dir", str(state_dir), "--done-only"],
        check=True,
    )
    assert store.load("job3") is None
