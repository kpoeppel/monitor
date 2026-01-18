"""Top-level config and runner helpers for monitor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from compoconf import parse_config

from monitor.local_client import LocalCommandClient
from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop, MonitorLoopConfig
from monitor.submission import parse_job_registration


@dataclass(kw_only=True)
class JobConfig:
    job_id: str
    registration: dict[str, Any]


@dataclass(kw_only=True)
class ClientConfig:
    class_name: Literal["LocalCommandClient", "SlurmGenClient"] = "LocalCommandClient"
    slurm: dict[str, Any] = field(default_factory=dict)
    slurm_client: dict[str, Any] = field(default_factory=dict)
    output_dir: str | None = None


@dataclass(kw_only=True)
class RunConfig:
    max_cycles: int | None = None
    sleep_seconds: float | None = None


@dataclass(kw_only=True)
class MonitorAppConfig:
    monitor: Any
    jobs: list[JobConfig] = field(default_factory=list)
    client: ClientConfig = field(default_factory=ClientConfig)
    state_store_dir: str | None = None
    run: RunConfig = field(default_factory=RunConfig)
    raw: dict[str, Any] | None = None


def load_app_config(path: str | Path) -> MonitorAppConfig:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return parse_app_config(payload)


def parse_app_config(payload: dict[str, Any]) -> MonitorAppConfig:
    jobs_payload = payload.get("jobs", [])
    jobs: list[JobConfig] = []
    for job in jobs_payload:
        reg_payload = job.get("registration", {})
        jobs.append(JobConfig(job_id=str(job["job_id"]), registration=dict(reg_payload)))
    client_payload = payload.get("client", {})
    run_payload = payload.get("run", {})
    return MonitorAppConfig(
        monitor=payload["monitor"],
        jobs=jobs,
        client=ClientConfig(**client_payload) if client_payload else ClientConfig(),
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
    client = _build_client(config.client)
    if not config.state_store_dir:
        raise ValueError("state_store_dir is required for monitor loop")
    store = JobFileStore(config.state_store_dir)
    _sync_jobs(store, config.jobs)
    return MonitorLoop(store, client, poll_interval_seconds=poll_interval)


def sync_loop(loop: MonitorLoop, config: MonitorAppConfig) -> None:
    if not config.state_store_dir:
        raise ValueError("state_store_dir is required for monitor loop")
    store = JobFileStore(config.state_store_dir)
    _sync_jobs(store, config.jobs)


def _build_client(config: ClientConfig) -> LocalCommandClient:
    if config.class_name == "LocalCommandClient":
        return LocalCommandClient()
    if config.class_name == "SlurmGenClient":
        from monitor.slurm_gen_client import SlurmGenClient, SlurmGenClientConfig

        slurm_config = SlurmGenClientConfig(
            slurm=config.slurm,
            slurm_client=config.slurm_client,
            output_dir=config.output_dir,
        )
        return SlurmGenClient(slurm_config)
    raise ValueError(f"Unsupported client: {config.class_name}")


def _build_registration(payload: dict[str, Any]) -> Any:
    prepared = dict(payload)
    if "log_path_latest" in prepared and "log_path_current" not in prepared:
        prepared["log_path_current"] = prepared.pop("log_path_latest")
    for legacy_key in ("state_events", "inactivity_rules", "output_paths", "inactivity_threshold_seconds"):
        prepared.pop(legacy_key, None)
    return parse_job_registration(prepared)


def _sync_jobs(store: JobFileStore, jobs: list[JobConfig]) -> None:
    for job in jobs:
        registration = _build_registration(job.registration)
        existing = store.load(str(job.job_id))
        runtime = existing.runtime if existing else None
        record = JobRecordConfig(job_id=str(job.job_id), registration=registration)
        if runtime is not None:
            record.runtime = runtime
        store.upsert(record)


def _import_registry() -> None:
    import monitor.actions  # noqa: F401
    import monitor.conditions  # noqa: F401
    import monitor.submission  # noqa: F401


__all__ = [
    "MonitorAppConfig",
    "load_app_config",
    "parse_app_config",
    "build_loop",
    "sync_loop",
]
