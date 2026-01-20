"""Top-level config and runner helpers for monitor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from compoconf import parse_config

from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop, MonitorLoopConfig
from monitor.job_client_protocol import JobClientInterface


@dataclass(kw_only=True)
class JobConfig:
    job_id: str
    registration: dict[str, Any]


@dataclass(kw_only=True)
class RunConfig:
    max_cycles: int | None = None
    sleep_seconds: float | None = None


@dataclass(kw_only=True)
class MonitorAppConfig:
    monitor: Any
    jobs: list[JobConfig] = field(default_factory=list)
    client: Any = None
    state_store_dir: str | None = None
    run: RunConfig = field(default_factory=RunConfig)
    raw: dict[str, Any] | None = None


def load_app_config(path: str | Path) -> MonitorAppConfig:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return parse_app_config(payload)


def parse_app_config(payload: dict[str, Any]) -> MonitorAppConfig:
    _import_registry()
    jobs_payload = payload.get("jobs", [])
    jobs: list[JobConfig] = []
    for job in jobs_payload:
        reg_payload = job.get("registration", {})
        jobs.append(JobConfig(job_id=str(job["job_id"]), registration=dict(reg_payload)))
    client_payload = payload.get("client", {})
    if not client_payload:
        client_payload = {"class_name": "LocalCommandClient"}
    client_config = parse_config(JobClientInterface.cfgtype, client_payload)
    run_payload = payload.get("run", {})
    return MonitorAppConfig(
        monitor=payload["monitor"],
        jobs=jobs,
        client=client_config,
        state_store_dir=payload.get("state_store_dir"),
        run=RunConfig(**run_payload) if run_payload else RunConfig(),
        raw=payload,
    )


def build_loop(config: MonitorAppConfig) -> MonitorLoop:
    _import_registry()
    monitor_cfg = config.monitor
    poll_interval = 60.0
    if isinstance(monitor_cfg, dict):
        monitor_cfg = parse_config(MonitorLoopConfig, monitor_cfg)
    if isinstance(monitor_cfg, MonitorLoopConfig):
        poll_interval = monitor_cfg.poll_interval_seconds
    if config.client is None:
        config.client = parse_config(JobClientInterface.cfgtype, {"class_name": "LocalCommandClient"})
    client = config.client.instantiate(JobClientInterface)
    if not config.state_store_dir:
        raise ValueError("state_store_dir is required for monitor loop")
    store = JobFileStore(config.state_store_dir)
    _sync_jobs(store, config.jobs)
    return MonitorLoop(store, client, poll_interval_seconds=poll_interval)


def sync_loop(loop: MonitorLoop, config: MonitorAppConfig) -> None:
    if not config.state_store_dir:
        raise ValueError("state_store_dir is required for monitor loop")
    store = JobFileStore(config.state_store_dir)
    # _sync_jobs(store, config.jobs)


# def _build_registration(payload: dict[str, Any]) -> Any:
#     prepared = dict(payload)
#     if "log_path_latest" in prepared and "log_path_current" not in prepared:
#         prepared["log_path_current"] = prepared.pop("log_path_latest")
#     for legacy_key in ("state_events", "inactivity_rules", "output_paths", "inactivity_threshold_seconds"):
#         prepared.pop(legacy_key, None)
#     return parse_job_registration(prepared)


# def _sync_jobs(store: JobFileStore, jobs: list[JobConfig]) -> None:
#     for job in jobs:
#         registration = _build_registration(job.registration)
#         existing = store.load(str(job.job_id))
#         runtime = existing.runtime if existing else None
#         record = JobRecordConfig(job_id=str(job.job_id), registration=registration)
#         if runtime is not None:
#             record.runtime = runtime
#         store.upsert(record)


def _import_registry() -> None:
    import monitor.actions  # noqa: F401
    import monitor.conditions  # noqa: F401
    import monitor.local_client  # noqa: F401

    try:  # slurm_gen is optional
        import monitor.slurm_job_client  # noqa: F401
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional slurm_gen install
        if exc.name != "slurm_gen":
            raise
    import monitor.submission  # noqa: F401


__all__ = [
    "MonitorAppConfig",
    "load_app_config",
    "parse_app_config",
    "build_loop",
    "sync_loop",
]
