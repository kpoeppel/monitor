#!/usr/bin/env python3
"""Submit/cancel jobs by updating the monitor config state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
slurm_gen_path = ROOT / "slurm_gen" / "src"
if slurm_gen_path.exists():
    sys.path.insert(0, str(slurm_gen_path))

from monitor.persistence import MonitorStateStore


def _load_config(store: MonitorStateStore) -> dict[str, Any]:
    payload = store.load_config()
    if payload is None:
        raise FileNotFoundError("No config.json found in state dir")
    return payload


def _save_config(store: MonitorStateStore, payload: dict[str, Any]) -> None:
    store.save_config(payload)


def _ensure_jobs_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    jobs = payload.get("jobs")
    if jobs is None:
        jobs = []
        payload["jobs"] = jobs
    if not isinstance(jobs, list):
        raise ValueError("config.jobs is not a list")
    return jobs


def submit_job(store: MonitorStateStore, payload: dict[str, Any]) -> None:
    config = _load_config(store)
    jobs = _ensure_jobs_list(config)
    job_id = payload.get("job_id")
    if not job_id:
        raise ValueError("payload missing job_id")
    jobs = [job for job in jobs if str(job.get("job_id")) != str(job_id)]
    jobs.append(payload)
    config["jobs"] = jobs
    _save_config(store, config)


def cancel_job(store: MonitorStateStore, job_id: str) -> None:
    config = _load_config(store)
    jobs = _ensure_jobs_list(config)
    jobs = [job for job in jobs if str(job.get("job_id")) != str(job_id)]
    config["jobs"] = jobs
    _save_config(store, config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit/cancel monitor jobs.")
    parser.add_argument("--state-dir", required=True, help="Path to monitor state dir.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    submit_parser = subparsers.add_parser("submit", help="Submit or replace a job")
    submit_parser.add_argument("--job-json", help="Path to job JSON payload.")
    submit_parser.add_argument("--job-yaml", help="Path to job YAML payload.")

    cancel_parser = subparsers.add_parser("cancel", help="Cancel/remove a job")
    cancel_parser.add_argument("--job-id", required=True, help="Job ID to cancel/remove.")

    args = parser.parse_args()

    store = MonitorStateStore(Path(args.state_dir))

    if args.command == "submit":
        job_path = args.job_json or args.job_yaml
        if not job_path:
            raise ValueError("submit requires --job-json or --job-yaml")
        job_text = Path(job_path).read_text(encoding="utf-8")
        if job_path.endswith((".yaml", ".yml")):
            import yaml
            payload = yaml.safe_load(job_text)
        else:
            payload = json.loads(job_text)
        submit_job(store, payload)
        return

    if args.command == "cancel":
        cancel_job(store, args.job_id)
        return


if __name__ == "__main__":
    main()
