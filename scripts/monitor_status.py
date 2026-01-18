#!/usr/bin/env python3
"""Print a summary of jobs/events from a monitor state directory."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
slurm_gen_path = ROOT / "slurm_gen" / "src"
if slurm_gen_path.exists():
    sys.path.insert(0, str(slurm_gen_path))

from monitor.loop import JobFileStore
from monitor import actions, conditions, submission  # noqa: F401


def main() -> None:
    parser = argparse.ArgumentParser(description="Show monitor state summary.")
    parser.add_argument("--state-dir", required=True, help="Path to monitor state dir.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    args = parser.parse_args()

    store = JobFileStore(Path(args.state_dir))
    jobs = store.load_all()

    payload = {
        "jobs": [
            {
                "job_id": job.job_id,
                "name": job.registration.name if job.registration else "",
                "attempts": job.runtime.attempts,
                "submitted": job.runtime.submitted,
                "runtime_job_id": job.runtime.runtime_job_id,
                "last_status": job.runtime.last_status,
                "log_path": job.registration.log_path if job.registration else "",
            }
            for job in jobs
        ],
    }

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print(f"Jobs: {len(jobs)}")
    for job in jobs:
        name = job.registration.name if job.registration else ""
        print(
            f"- {job.job_id} {name} status={job.runtime.last_status} "
            f"attempts={job.runtime.attempts} submitted={job.runtime.submitted}"
        )


if __name__ == "__main__":
    main()
