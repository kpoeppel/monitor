"""Handles the execution of job-related actions like starting, stopping, and restarting."""

from __future__ import annotations
import logging
import time
from pathlib import Path
from typing import Any

from monitor.job_client_protocol import JobClientProtocol
from monitor.submission import JobRegistration
from monitor.submission import JobRuntimeState, SubmissionManager
from monitor.states import PendingState, PendingStateConfig
from monitor.watcher import BaseMonitor

LOGGER = logging.getLogger(__name__)


class Executor:
    def __init__(
        self,
        submission_manager: SubmissionManager,
        slurm_client: JobClientProtocol,
        monitor: BaseMonitor,
    ) -> None:
        self._submission_manager = submission_manager
        self._slurm = slurm_client
        self._monitor = monitor

    def start_job(self, state: JobRuntimeState) -> str:
        """Submits a job to the execution backend."""
        old_id = state.job_id
        new_job_id = self._submit_job(state)
        
        if old_id != new_job_id:
            self._submission_manager.remove_job(old_id)

        state.job_id = new_job_id
        state.attempts += 1
        state.last_slurm_state = None
        state.state = PendingState(PendingStateConfig())
        state.submitted = True
        self._submission_manager.update_job(state)
        LOGGER.info(f"[job {state.name}] submitted with ID {new_job_id}")
        return new_job_id

    def stop_job(self, state: JobRuntimeState) -> None:
        """Cancels and removes a job."""
        if state.job_id:
            self._slurm.cancel(state.job_id)
            self._slurm.remove(state.job_id)
            self._submission_manager.remove_job(state.job_id)

    def restart_job(
        self,
        state: JobRuntimeState,
        adjustments: dict[str, Any] | None = None,
    ) -> None:
        """Prepares a job for restart by resetting its state. 
        Actual submission happens in the main loop via start_job."""
        old_job_id = state.job_id
        if adjustments:
            self._apply_adjustments(state, adjustments)

        # Cancel the running instance
        self._slurm.cancel(old_job_id)
        self._slurm.remove(old_job_id)
        
        # We don't remove from manager here, we just update the state
        # to be ready for re-submission
        state.job_id = f"{state.name}_pending_{state.attempts}" # Temporary ID until submission
        state.last_slurm_state = None
        state.state = PendingState(PendingStateConfig())
        state.submitted = False # This flags it for the controller to pick up and check conditions
        self._submission_manager.update_job(state)
        
        # Clean up the old ID map if necessary, though update_job handles keying by job_id.
        # If job_id changed, we might have a ghost entry. 
        if old_job_id != state.job_id:
             self._submission_manager.remove_job(old_job_id)
             self._submission_manager.update_job(state)

    def duplicate_job(
        self,
        state: JobRuntimeState,
        *,
        adjustments: dict[str, Any] | None = None,
        name_suffix: str | None = None,
    ) -> str:
        registration = state.registration
        suffix = name_suffix or "_dup"
        timestamp = int(time.time() * 1000)
        log_path = self._duplicate_log_path(registration.log_path, suffix, timestamp)
        cloned = JobRuntimeState(
            job_id=f"{state.job_id}_dup_{int(time.time() * 1000)}",
            registration=JobRegistration(
                name=registration.name,
                command=list(registration.command),
                log_path=log_path,
                metadata=dict(registration.metadata),
                slurm=registration.slurm,
                inactivity_threshold_seconds=registration.inactivity_threshold_seconds,
                output_paths=list(registration.output_paths),
                start_condition=registration.start_condition,
                cancel_condition=registration.cancel_condition,
                finish_condition=registration.finish_condition,
                extra_args=list(registration.extra_args),
                log_to_file=registration.log_to_file,
                log_path_current=registration.log_path_current,
            ),
            attempts=1,
            submitted=False,
        )
        if suffix:
            cloned.registration.name = f"{cloned.registration.name}{suffix}"
        if adjustments:
            self._apply_adjustments(cloned, adjustments)
        if adjustments is None or "log_path" not in adjustments:
            cloned.registration.log_path = log_path
        if adjustments is None or "log_path_current" not in adjustments:
            cloned.registration.log_path_current = registration.log_path_current
        new_job_id = self.start_job(cloned)
        return new_job_id

    @staticmethod
    def _duplicate_log_path(log_path: str, suffix: str, timestamp: int) -> str:
        path = Path(log_path)
        stem = f"{path.stem}{suffix}_{timestamp}"
        return str(path.with_name(f"{stem}{path.suffix}"))

    def finalize_job(self, job_id: str) -> None:
        self._submission_manager.remove_job(job_id)
        self._slurm.remove(job_id)

    def _apply_adjustments(self, state: JobRuntimeState, adjustments: dict[str, Any]) -> None:
        command_override = adjustments.get("command")
        if isinstance(command_override, list):
            state.registration.command = [str(arg) for arg in command_override]
        log_override = adjustments.get("log_path")
        if log_override:
            state.registration.log_path = str(log_override)
        log_current = adjustments.get("log_path_current")
        if log_current is not None:
            state.registration.log_path_current = str(log_current)
        extra_args = adjustments.get("extra_args")
        if isinstance(extra_args, list):
            state.registration.extra_args = [str(arg) for arg in extra_args]
        extra_args_append = adjustments.get("extra_args_append")
        if isinstance(extra_args_append, list):
            state.registration.extra_args.extend([str(arg) for arg in extra_args_append])
        metadata_patch = adjustments.get("metadata")
        if isinstance(metadata_patch, dict):
            state.registration.metadata.update(metadata_patch)
        output_paths = adjustments.get("output_paths")
        if isinstance(output_paths, list):
            state.registration.output_paths = [str(path) for path in output_paths]
        inactivity_threshold = adjustments.get("inactivity_threshold_seconds")
        if inactivity_threshold is not None:
            state.registration.inactivity_threshold_seconds = float(inactivity_threshold)
        log_to_file = adjustments.get("log_to_file")
        if isinstance(log_to_file, bool):
            state.registration.log_to_file = log_to_file
        slurm_payload = adjustments.get("slurm")
        if isinstance(slurm_payload, dict):
            if state.registration.job_kind != "slurm":
                LOGGER.warning("Ignoring slurm adjustments for non-slurm job '%s'", state.name)
            else:
                state.registration.slurm = _merge_dicts(state.registration.slurm, slurm_payload)

    def _submit_job(self, state: JobRuntimeState) -> str:
        if "_" in str(state.job_id) and hasattr(self._slurm, "submit_array"):
            parts = str(state.job_id).split("_")
            if len(parts) == 2 and parts[1].isdigit():
                array_idx = int(parts[1])
                job_ids = self._slurm.submit_array(
                    array_name=state.name,
                    command=state.registration.command,
                    log_paths=[state.registration.log_path],
                    task_names=[state.name],
                    extra_args=state.registration.extra_args,
                    start_index=array_idx,
                    log_to_file=state.registration.log_to_file,
                    log_path_current=state.registration.log_path_current,
                    slurm=state.registration.slurm,
                )
                new_job_id = job_ids[0] if job_ids else None
                if new_job_id is None:
                    raise RuntimeError(f"Array job submission failed for {state.name}")
                return new_job_id
        
        return self._slurm.submit(
            state.name,
            state.registration.command,
            state.registration.log_path,
            state.registration.extra_args,
            state.registration.log_to_file,
            state.registration.log_path_current,
            state.registration.slurm,
        )


def _merge_dicts(base: dict[str, Any] | None, override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base or {})
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged
