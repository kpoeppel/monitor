"""SLURM transition helpers for the monitor controller."""

from __future__ import annotations

from typing import Any, Callable

from monitor.submission import JobRuntimeState
from monitor.watcher import MonitorOutcome


class TransitionManager:
    """Encapsulate SLURM state transition handling and classification."""

    def __init__(
        self,
        *,
        set_state: Callable[[JobRuntimeState, str], None],
        queue_event: Callable[..., Any],
        persist_job: Callable[[JobRuntimeState], None],
    ) -> None:
        self._set_state = set_state
        self._queue_event = queue_event
        self._persist_job = persist_job

    def capture_slurm_transitions(
        self,
        state: JobRuntimeState,
        current_state: str | None,
    ) -> list[Any]:
        previous = state.last_slurm_state
        records: list[Any] = []
        if current_state != previous:
            metadata = {
                "slurm_state": current_state or "NOT_FOUND",
                "previous_state": previous or "NONE",
            }
            if current_state == "RUNNING":
                self._set_state(state, "running")
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key if state.state else None,
                    metadata=metadata,
                    action_name="run_started",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
            if current_state in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}:
                self._set_state(state, current_state.lower())
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key if state.state else None,
                    metadata=metadata,
                    action_name="run_ended",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
            if current_state is None and previous is not None:
                self._set_state(state, "timeout")
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key if state.state else None,
                    metadata=metadata,
                    action_name="run_disappeared",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
        state.last_slurm_state = current_state
        self._persist_job(state)
        return records  # pragma: no cover

    @staticmethod
    def classify_mode(
        state: JobRuntimeState,
        outcome: MonitorOutcome | None,
        slurm_snapshot: dict[str, str],
    ) -> tuple[str, dict[str, Any]] | None:
        if outcome and outcome.status == "complete":
            return "success", {"reason": "termination_condition_met"}

        slurm_state = slurm_snapshot.get(state.job_id)
        if outcome and outcome.status == "stall":
            return "stall", {"reason": "inactivity_timeout"}
        if slurm_state is None:
            return "timeout", {"reason": "job_not_in_queue", "error_type": "timeout"}
        if slurm_state == "CANCELLED":
            return "crash", {"reason": "job_cancelled", "slurm_state": "CANCELLED", "error_type": "cancelled", "subsystem": "slurm"}
        if slurm_state == "FAILED":
            return "crash", {"reason": "job_failed", "slurm_state": "FAILED", "error_type": "slurm_failure"}
        if slurm_state == "COMPLETED":
            return "success", {"reason": "slurm_completed", "slurm_state": "COMPLETED"}
        if slurm_state == "TIMEOUT":
            return "timeout", {"reason": "slurm_timeout", "slurm_state": "TIMEOUT", "error_type": "timeout"}
        return None  # pragma: no cover


__all__ = ["TransitionManager"]
