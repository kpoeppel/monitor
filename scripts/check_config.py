#!/usr/bin/env python3
"""Validate a monitor YAML config by parsing and instantiating it."""

from __future__ import annotations

import argparse
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
