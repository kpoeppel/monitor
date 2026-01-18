"""Top-level config and runner helpers for monitor."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from compoconf import parse_config

from monitor.conditions import MonitorConditionInterface
from monitor.controller import MonitorController
from monitor.local_client import LocalCommandClient
from monitor.persistence import MonitorStateStore
from monitor.submission import JobRegistration, parse_job_registration
from monitor.watcher import (
    BaseMonitor,
    MonitorConfigInterface,
    NullMonitor,
    NullMonitorConfig,
    SlurmLogMonitor,
    SlurmLogMonitorConfig,
)


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


def build_controller(config: MonitorAppConfig) -> MonitorController:
    monitor_cfg = config.monitor
    if isinstance(monitor_cfg, dict):
        class_name = monitor_cfg.get("class_name", "SlurmLogMonitor")
        if class_name == "NullMonitor":
            monitor_cfg = parse_config(NullMonitorConfig, monitor_cfg)
        else:
            monitor_cfg = parse_config(SlurmLogMonitorConfig, monitor_cfg)
    if isinstance(monitor_cfg, NullMonitorConfig):
        monitor = NullMonitor(monitor_cfg)
    else:
        monitor = SlurmLogMonitor(monitor_cfg)
    client = _build_client(config.client)
    state_store = (
        MonitorStateStore(config.state_store_dir)
        if config.state_store_dir
        else None
    )
    controller = MonitorController(monitor, client, state_store=state_store)
    _apply_jobs(controller, config.jobs)
    if state_store and config.raw:
        state_store.save_config(config.raw)
    return controller


def sync_controller(controller: MonitorController, config: MonitorAppConfig) -> None:
    _apply_jobs(controller, config.jobs)
    if controller._state_store and config.raw:
        controller._state_store.save_config(config.raw)


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


def _parse_condition(payload: Any | None) -> Any | None:
    if payload is None:
        return None
    if hasattr(payload, "instantiate"):
        return payload
    if isinstance(payload, dict):
        return parse_config(MonitorConditionInterface.cfgtype, payload)
    raise TypeError(f"Unsupported condition payload: {payload!r}")


def _build_registration(payload: dict[str, Any]) -> JobRegistration:
    from monitor.watcher import LogEventConfig, InactivityRuleConfig, StateEventConfig
    from monitor.actions import ActionBackendInterface, BaseMonitorAction

    prepared = dict(payload)
    if "log_path_latest" in prepared and "log_path_current" not in prepared:
        prepared["log_path_current"] = prepared.pop("log_path_latest")
    prepared["start_condition"] = _parse_condition(prepared.get("start_condition"))
    prepared["cancel_condition"] = _parse_condition(prepared.get("cancel_condition"))
    prepared["finish_condition"] = _parse_condition(prepared.get("finish_condition"))

    def parse_action_payload(action_payload: Any) -> Any:
        if isinstance(action_payload, dict):
            payload = dict(action_payload)
            backend_config = payload.get("backend_config")
            if isinstance(backend_config, dict):
                payload["backend_config"] = parse_config(ActionBackendInterface.cfgtype, backend_config)
            return parse_config(BaseMonitorAction.cfgtype, payload)
        return action_payload

    def parse_event_config(cfg_type: Any, items: list[Any]) -> list[Any]:
        parsed: list[Any] = []
        for item in items:
            if isinstance(item, dict):
                payload = dict(item)
                if "action" in payload:
                    payload["action"] = parse_action_payload(payload["action"])
                parsed.append(parse_config(cfg_type, payload))
            else:
                parsed.append(item)
        return parsed

    prepared["log_events"] = parse_event_config(LogEventConfig, prepared.get("log_events", []))
    prepared["inactivity_rules"] = parse_event_config(InactivityRuleConfig, prepared.get("inactivity_rules", []))
    prepared["state_events"] = parse_event_config(StateEventConfig, prepared.get("state_events", []))
    return parse_job_registration(prepared)


def _apply_jobs(controller: MonitorController, jobs: list[JobConfig]) -> None:
    for job in jobs:
        registration = _build_registration(job.registration)
        existing = controller._submission_manager.get_job(job.job_id)
        if existing is None:
            controller.register_job(job.job_id, registration)
        else:
            existing.registration = registration
            controller._submission_manager.update_job(existing)


__all__ = [
    "MonitorAppConfig",
    "load_app_config",
    "parse_app_config",
    "build_controller",
    "sync_controller",
]
