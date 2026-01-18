#!/usr/bin/env python3
"""Validate a monitor YAML config by parsing and instantiating it."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
slurm_gen_path = ROOT / "slurm_gen" / "src"
if slurm_gen_path.exists():
    sys.path.insert(0, str(slurm_gen_path))

from monitor.app import build_loop, load_app_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a monitor YAML config.")
    parser.add_argument("--config", required=True, help="Path to YAML config.")
    parser.add_argument("--state-dir", default=None, help="Optional state dir override.")
    args = parser.parse_args()

    config = load_app_config(args.config)
    if args.state_dir:
        config.state_store_dir = args.state_dir
    build_loop(config)
    print("ok")


if __name__ == "__main__":
    main()
