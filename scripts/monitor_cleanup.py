#!/usr/bin/env python3
"""Cleanup monitor state directory."""

from __future__ import annotations

import argparse
from pathlib import Path
from monitor.loop import JobFileStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup monitor state files.")
    parser.add_argument("--state-dir", required=True, help="Path to monitor state dir.")
    parser.add_argument("--done-only", action="store_true", help="Remove finished jobs only.")
    args = parser.parse_args()

    store = JobFileStore(Path(args.state_dir))
    removed = 0

    for record in store.load_all():
        if not args.done_only:
            store.remove(record.job_id)
            removed += 1
            continue
        status = record.runtime.last_status
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            store.remove(record.job_id)
            removed += 1

    print(f"removed={removed}")


if __name__ == "__main__":
    main()
