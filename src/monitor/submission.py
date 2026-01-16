"""Job registration and state management for submission."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from compoconf import ConfigInterface

if TYPE_CHECKING:
    from monitor.persistence import MonitorStateStore
    from monitor.watcher import MonitorOutcome

from monitor.states import UndefinedStateConfig, MonitorStateInterface

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class JobRegistration:
    """Configuration for a job to be monitored."""

    name: str
    script_path: str
    log_path: str
    inactivity_threshold_seconds: float | None = None
    output_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    start_condition: ConfigInterface | None = None
    cancel_condition: ConfigInterface | None = None
    finish_condition: ConfigInterface | None = None


@dataclass(kw_only=True)
class JobRuntimeState:
    """Mutable runtime state of a registered job."""

    job_id: str
    registration: JobRegistration
    attempts: int = 1
    submitted: bool = False
    state: MonitorStateInterface = field(
        default_factory=lambda: UndefinedStateConfig().instantiate(MonitorStateInterface)
    )
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
        state: MonitorStateInterface,
        attempts: int = 1,
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
        self._jobs[state.job_id] = state  # pragma: no cover
        self._persist_job(state)  # pragma: no cover

    def _persist_job(self, state: JobRuntimeState) -> None:
        if not self._state_store:
            return  # pragma: no cover
        from monitor.persistence import StoredJob

        resolved_log_path = self._expand_log_path(state.job_id, state.registration.log_path)
        monitor_state = state.state.key if state.state else None
        stored = StoredJob.from_registration(
            state.job_id,
            state.attempts,
            state.registration,
            resolved_log_path=str(resolved_log_path),
            monitor_state=monitor_state,
            slurm_state=state.last_slurm_state,
        )
        self._state_store.upsert_job(stored)

    def _expand_log_path(self, job_id: str, log_path: str) -> Path:
        log_str = str(log_path)
        if "_" in job_id:
            base_id, array_idx = job_id.split("_")
            log_str = log_str.replace("%A", str(base_id)).replace("%a", str(array_idx))
        log_str = log_str.replace("%j", str(job_id))
        return Path(log_str)
