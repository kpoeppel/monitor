"""Protocol defining the job client interface required by monitor.

This protocol allows monitor to work with different job execution backends:
- SLURM (via slurm_gen.client.BaseSlurmClient)
- Local processes (via LocalCommandClient)
- Other batch systems (PBS, LSF, etc.)
"""

from __future__ import annotations
from typing import Protocol, runtime_checkable


@runtime_checkable
class JobClientProtocol(Protocol):
    """Protocol for job submission and management.

    Any class implementing these methods can be used with MonitorController,
    allowing monitor to work with SLURM, local processes, or other batch systems.
    """

    def submit(self, name: str, script_path: str, log_path: str) -> str:  # pragma: no cover
        """Submit a single job.

        Args:
            name: Human-readable job name
            script_path: Path to the script/command to execute
            log_path: Path where job output should be logged

        Returns:
            job_id: Unique identifier for the submitted job

        Raises:
            RuntimeError: If submission fails
        """
        ...

    def submit_array(
        self,
        array_name: str,
        script_path: str,
        log_paths: list[str],
        task_names: list[str],
    ) -> list[str]:  # pragma: no cover
        """Submit an array of jobs (multiple instances of same script).

        Args:
            array_name: Base name for the job array
            script_path: Path to the script to execute for each task
            log_paths: List of log paths, one per task
            task_names: List of task names, one per task

        Returns:
            List of job_ids, one per submitted task

        Raises:
            RuntimeError: If submission fails
        """
        ...

    def cancel(self, job_id: str) -> None:  # pragma: no cover
        """Cancel/terminate a running or pending job.

        Args:
            job_id: Job identifier returned by submit()
        """
        ...

    def remove(self, job_id: str) -> None:  # pragma: no cover
        """Remove a job from tracking.

        This is used to clean up finished jobs from the client's internal state.

        Args:
            job_id: Job identifier to remove
        """
        ...

    def squeue(self) -> dict[str, str]:  # pragma: no cover
        """Query the status of all tracked jobs.

        Returns:
            Dictionary mapping job_id -> status string
            Common statuses: "PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"
        """
        ...

    def job_ids_by_name(self, name: str) -> list[str]:  # pragma: no cover
        """Get all job IDs matching a given name.

        Args:
            name: Job name to search for

        Returns:
            List of job_ids with matching name
        """
        ...

    def get_job(self, job_id: str):  # pragma: no cover
        """Get detailed information about a specific job.

        Args:
            job_id: Job identifier

        Returns:
            Job object with details (implementation-specific)

        Raises:
            KeyError: If job_id not found
        """
        ...


__all__ = ["JobClientProtocol"]
