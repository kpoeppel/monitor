#!/usr/bin/env python3
"""Run a monitor loop from a YAML config."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from monitor.app import build_loop, load_app_config, sync_loop


def main() -> None:
    parser = argparse.ArgumentParser(description="Run monitor from YAML config.")
    parser.add_argument("--config", required=True, help="Path to YAML config.")
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Override state store directory from config.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single observation cycle and exit.",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Stop after this many cycles.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    app_config = load_app_config(config_path)
    if args.state_dir:
        app_config.state_store_dir = args.state_dir

    loop = build_loop(app_config)

    poll_seconds = app_config.run.sleep_seconds
    if poll_seconds is None:
        poll_seconds = loop.poll_interval_seconds

    last_mtime = config_path.stat().st_mtime
    cycles = 0
    while True:
        try:
            current_mtime = config_path.stat().st_mtime
        except FileNotFoundError:
            current_mtime = last_mtime
        if current_mtime != last_mtime:
            app_config = load_app_config(config_path)
            if args.state_dir:
                app_config.state_store_dir = args.state_dir
            sync_loop(loop, app_config)
            last_mtime = current_mtime
        loop.observe_once()
        cycles += 1
        if args.once or app_config.run.max_cycles == 1:
            break
        if args.max_cycles is not None and cycles >= args.max_cycles:
            break
        if app_config.run.max_cycles is not None and cycles >= app_config.run.max_cycles:
            break
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
