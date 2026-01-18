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

from compoconf import parse_config

from monitor.loop import JobFileStore, JobRecordConfig
from monitor import actions, conditions, submission  # noqa: F401


def submit_job(store: JobFileStore, payload: dict[str, Any]) -> None:
    record = parse_config(JobRecordConfig, payload)
    store.upsert(record)


def cancel_job(store: JobFileStore, job_id: str) -> None:
    store.remove(job_id)


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

    store = JobFileStore(Path(args.state_dir))

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
