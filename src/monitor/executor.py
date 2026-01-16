"""Handles the execution of job-related actions like starting, stopping, and restarting."""

from __future__ import annotations
import logging
from typing import Any

from monitor.job_client_protocol import JobClientProtocol
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

    def finalize_job(self, job_id: str) -> None:
        self._submission_manager.remove_job(job_id)
        self._slurm.remove(job_id)

    def _apply_adjustments(self, state: JobRuntimeState, adjustments: dict[str, Any]) -> None:
        script_override = adjustments.get("script_path")
        if script_override:
            state.registration.script_path = str(script_override)
        log_override = adjustments.get("log_path")
        if log_override:
            state.registration.log_path = str(log_override)
        metadata_patch = adjustments.get("metadata")
        if isinstance(metadata_patch, dict):
            state.registration.metadata.update(metadata_patch)

    def _submit_job(self, state: JobRuntimeState) -> str:
        if "_" in str(state.job_id) and hasattr(self._slurm, "submit_array"):
            parts = str(state.job_id).split("_")
            if len(parts) == 2 and parts[1].isdigit():
                array_idx = int(parts[1])
                job_ids = self._slurm.submit_array(
                    array_name=state.name,
                    script_path=state.registration.script_path,
                    log_paths=[state.registration.log_path],
                    task_names=[state.name],
                    start_index=array_idx,
                )
                new_job_id = job_ids[0] if job_ids else None
                if new_job_id is None:
                    raise RuntimeError(f"Array job submission failed for {state.name}")
                return new_job_id
        
        return self._slurm.submit(
            state.name, state.registration.script_path, state.registration.log_path
        )
