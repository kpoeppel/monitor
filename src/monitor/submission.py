"""Job registration and state management for submission."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    parse_config,
    register,
    register_interface,
)
from monitor.utils.paths import expand_log_path

if TYPE_CHECKING:
    from monitor.persistence import MonitorStateStore
    from monitor.states import BaseMonitorState
    from monitor.watcher import MonitorOutcome


LOGGER = logging.getLogger(__name__)


@register_interface
class JobRegistrationInterface(RegistrableConfigInterface):
    """Registrable interface for job registrations."""


@dataclass(kw_only=True)
class JobRegistration:
    """Configuration for a job to be monitored."""

    name: str
    command: list[str]
    log_path: str
    log_path_current: str | None = None
    extra_args: list[str] = field(default_factory=list)
    log_to_file: bool = True
    inactivity_threshold_seconds: float | None = None
    output_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    slurm: dict[str, Any] | None = None
    log_events: list[Any] = field(default_factory=list)
    inactivity_rules: list[Any] = field(default_factory=list)
    state_events: list[Any] = field(default_factory=list)
    start_condition: ConfigInterface | None = None
    cancel_condition: ConfigInterface | None = None
    finish_condition: ConfigInterface | None = None
    job_kind: str | None = None

    def __post_init__(self) -> None:
        if self.job_kind is None:
            self.job_kind = "slurm" if self.slurm else "local"


@dataclass(kw_only=True)
class LocalJobRegistrationConfig(ConfigInterface):
    class_name: str = "LocalJobRegistration"
    name: str = ""
    command: list[str] = field(default_factory=list)
    log_path: str = ""
    log_path_current: str | None = None
    extra_args: list[str] = field(default_factory=list)
    log_to_file: bool = True
    inactivity_threshold_seconds: float | None = None
    output_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    log_events: list[Any] = field(default_factory=list)
    inactivity_rules: list[Any] = field(default_factory=list)
    state_events: list[Any] = field(default_factory=list)
    start_condition: ConfigInterface | None = None
    cancel_condition: ConfigInterface | None = None
    finish_condition: ConfigInterface | None = None


@dataclass(kw_only=True)
class SlurmJobRegistrationConfig(LocalJobRegistrationConfig):
    class_name: str = "SlurmJobRegistration"
    slurm: dict[str, Any] = field(default_factory=dict)


@register
class LocalJobRegistration(JobRegistrationInterface):
    config: LocalJobRegistrationConfig

    def __init__(self, config: LocalJobRegistrationConfig) -> None:
        self.config = config

    def to_registration(self) -> JobRegistration:
        return JobRegistration(
            name=self.config.name,
            command=list(self.config.command),
            log_path=self.config.log_path,
            log_path_current=self.config.log_path_current,
            extra_args=list(self.config.extra_args),
            log_to_file=self.config.log_to_file,
            inactivity_threshold_seconds=self.config.inactivity_threshold_seconds,
            output_paths=list(self.config.output_paths),
            metadata=dict(self.config.metadata),
            log_events=list(self.config.log_events),
            inactivity_rules=list(self.config.inactivity_rules),
            state_events=list(self.config.state_events),
            start_condition=self.config.start_condition,
            cancel_condition=self.config.cancel_condition,
            finish_condition=self.config.finish_condition,
            job_kind="local",
        )


@register
class SlurmJobRegistration(JobRegistrationInterface):
    config: SlurmJobRegistrationConfig

    def __init__(self, config: SlurmJobRegistrationConfig) -> None:
        self.config = config

    def to_registration(self) -> JobRegistration:
        return JobRegistration(
            name=self.config.name,
            command=list(self.config.command),
            log_path=self.config.log_path,
            log_path_current=self.config.log_path_current,
            extra_args=list(self.config.extra_args),
            log_to_file=self.config.log_to_file,
            inactivity_threshold_seconds=self.config.inactivity_threshold_seconds,
            output_paths=list(self.config.output_paths),
            metadata=dict(self.config.metadata),
            slurm=dict(self.config.slurm),
            log_events=list(self.config.log_events),
            inactivity_rules=list(self.config.inactivity_rules),
            state_events=list(self.config.state_events),
            start_condition=self.config.start_condition,
            cancel_condition=self.config.cancel_condition,
            finish_condition=self.config.finish_condition,
            job_kind="slurm",
        )


def parse_job_registration(payload: dict[str, Any]) -> JobRegistration:
    if "class_name" not in payload:
        payload = dict(payload)
        payload["class_name"] = "SlurmJobRegistration" if "slurm" in payload else "LocalJobRegistration"
    config = parse_config(JobRegistrationInterface.cfgtype, payload)
    registration = config.instantiate(JobRegistrationInterface)
    return registration.to_registration()


@dataclass(kw_only=True)
class JobRuntimeState:
    """Mutable runtime state of a registered job."""

    job_id: str
    registration: JobRegistration
    attempts: int = 1
    submitted: bool = False
    state: BaseMonitorState | None = None
    last_outcome: MonitorOutcome | None = None
    last_slurm_state: str | None = None
    condition_data: dict[str, Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.registration.name


class SubmissionManager:
    """Manages the lifecycle and persistence of monitored jobs."""

    def __init__(self, state_store: MonitorStateStore | None = None) -> None:
        self._state_store = state_store
        self._jobs: dict[str, JobRuntimeState] = {}
        if self._state_store:
            for stored in self._state_store.load_jobs():
                self._jobs[stored.job_id] = stored.to_runtime_state()

    def register_job(
        self,
        job_id: str,
        registration: JobRegistration,
        attempts: int = 1,
        state: BaseMonitorState | None = None,
    ) -> None:
        if job_id in self._jobs:
            return
        runtime_state = JobRuntimeState(
            job_id=job_id,
            registration=registration,
            attempts=attempts,
            state=state,
        )
        self._jobs[job_id] = runtime_state
        self._persist_job(runtime_state)

    def remove_job(self, job_id: str) -> None:
        if job_id in self._jobs:
            del self._jobs[job_id]
            if self._state_store:
                self._state_store.remove_job(job_id)

    def get_job(self, job_id: str) -> JobRuntimeState | None:
        return self._jobs.get(job_id)

    def jobs(self) -> list[JobRuntimeState]:
        return list(self._jobs.values())

    def clear_state(self) -> None:
        if self._state_store:
            self._state_store.clear()  # pragma: no cover

    def update_job(self, state: JobRuntimeState) -> None:
        self._jobs[state.job_id] = state # pragma: no cover
        self._persist_job(state) # pragma: no cover

    def _persist_job(self, state: JobRuntimeState) -> None:
        if not self._state_store:
            return  # pragma: no cover
        from monitor.persistence import StoredJob

        resolved_log_path = expand_log_path(state.registration.log_path, state.job_id)
        monitor_state = state.state.key if state.state else None
        stored = StoredJob.from_registration(
            state.job_id,
            state.attempts,
            state.registration,
            resolved_log_path=str(resolved_log_path),
            monitor_state=monitor_state,
            slurm_state=state.last_slurm_state,
            condition_data=state.condition_data,
        )
        self._state_store.upsert_job(stored)
