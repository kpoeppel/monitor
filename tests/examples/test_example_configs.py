from __future__ import annotations

from pathlib import Path

from monitor.app import build_loop, load_app_config
from monitor.loop import JobFileStore


def _load_with_state(path: Path, state_dir: Path):
    config = load_app_config(str(path))
    config.state_store_dir = str(state_dir)
    return config


def test_example_monitor_app_yaml(tmp_path: Path) -> None:
    config_path = Path("examples/monitor_app.yaml")
    config = _load_with_state(config_path, tmp_path / "state_app")
    loop = build_loop(config)
    store = JobFileStore(config.state_store_dir)
    assert loop.poll_interval_seconds > 0
    assert len(list(store.list_paths())) == len(config.jobs)


def test_example_monitor_config_yaml(tmp_path: Path) -> None:
    config_path = Path("examples/monitor_config.yaml")
    config = _load_with_state(config_path, tmp_path / "state_cfg")
    loop = build_loop(config)
    store = JobFileStore(config.state_store_dir)
    assert loop.poll_interval_seconds > 0
    assert len(list(store.list_paths())) == len(config.jobs)


def test_example_monitor_slurmgen_yaml(tmp_path: Path) -> None:
    config_path = Path("examples/monitor_slurmgen.yaml")
    config = _load_with_state(config_path, tmp_path / "state_slurm")
    loop = build_loop(config)
    store = JobFileStore(config.state_store_dir)
    assert loop.poll_interval_seconds > 0
    assert len(list(store.list_paths())) == len(config.jobs)
