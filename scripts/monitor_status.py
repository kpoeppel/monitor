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

from monitor.persistence import MonitorStateStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Show monitor state summary.")
    parser.add_argument("--state-dir", required=True, help="Path to monitor state dir.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    args = parser.parse_args()

    store = MonitorStateStore(Path(args.state_dir))
    jobs = store.load_jobs()
    events = store.load_events()

    payload = {
        "jobs": [
            {
                "job_id": job.job_id,
                "name": job.name,
                "attempts": job.attempts,
                "submitted": job.submitted,
                "slurm_state": job.slurm_state,
                "monitor_state": job.monitor_state,
                "log_path": job.log_path,
            }
            for job in jobs
        ],
        "events": [
            {
                "event_id": event.event_id,
                "name": event.name,
                "status": event.status.value,
                "count": event.count,
            }
            for event in events.values()
        ],
    }

    if args.json:
        print(json.dumps(payload, indent=2))
        return

    print(f"Jobs: {len(jobs)}")
    for job in jobs:
        print(
            f"- {job.job_id} {job.name} state={job.monitor_state} "
            f"slurm={job.slurm_state} attempts={job.attempts}"
        )
    print(f"Events: {len(events)}")
    for event in events.values():
        print(f"- {event.event_id} {event.name} status={event.status.value} count={event.count}")


if __name__ == "__main__":
    main()
