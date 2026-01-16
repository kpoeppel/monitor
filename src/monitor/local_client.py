"""Local command execution client for monitor.

Allows monitoring of local bash scripts/commands without SLURM.
Useful for:
- Local development and testing
- Single-machine workflows with complex monitoring needs
- Chaining local commands with event-driven logic
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from monitor.job_client_protocol import JobClientProtocol


@dataclass
class LocalJob:
    """Metadata for a locally executed job."""

    job_id: str
    name: str
    script_path: str
    log_path: str
    process: subprocess.Popen | None = None
    state: str = "PENDING"
    return_code: int | None = None
    submitted_at: float = field(default_factory=time.time)


class LocalCommandClient(JobClientProtocol):
    """Execute and monitor local bash commands as background processes.

    This client implements the JobClientProtocol to allow monitor to track
    local scripts/commands without requiring SLURM. Jobs are started as
    background processes with output redirected to log files.

    Example:
        >>> client = LocalCommandClient()
        >>> job_id = client.submit("my-job", "./train.sh", "./train.log")
        >>> statuses = client.squeue()
        >>> print(statuses[job_id])  # "RUNNING", "COMPLETED", etc.

    Note:
        - Processes are started in a new session (detached from terminal)
        - Job IDs are simple incrementing integers
        - No array job support for start_index parameter
    """

    def __init__(self) -> None:
        self._jobs: dict[str, LocalJob] = {}
        self._job_counter = 0

    def submit(self, name: str, script_path: str, log_path: str) -> str:
        """Submit a local script as a background process.

        Args:
            name: Human-readable job name
            script_path: Path to bash script to execute
            log_path: Path where stdout/stderr will be written

        Returns:
            job_id: String identifier for this job

        Raises:
            FileNotFoundError: If script_path doesn't exist
            OSError: If process creation fails
        """
        job_id = str(self._job_counter)
        self._job_counter += 1

        # Ensure log directory exists
        log_path_obj = Path(log_path)
        log_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Start process with output redirected to log file
        log_file = open(log_path, "w")
        try:
            proc = subprocess.Popen(
                ["bash", script_path],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # Detach from terminal
                cwd=Path(script_path).parent,  # Run in script's directory
            )

            job = LocalJob(
                job_id=job_id,
                name=name,
                script_path=script_path,
                log_path=log_path,
                process=proc,
                state="RUNNING",
            )
            self._jobs[job_id] = job

        except Exception:
            log_file.close()
            raise

        # Note: log_file will remain open for the lifetime of the process
        # This is intentional - closing it would break output redirection

        return job_id

    def submit_array(
        self,
        array_name: str,
        script_path: str,
        log_paths: list[str],
        task_names: list[str],
    ) -> list[str]:
        """Submit multiple instances of a script.

        Each task is submitted as an independent process. The script receives
        environment variables TASK_ID (0-indexed) and TASK_NAME.

        Args:
            array_name: Base name for the job array
            script_path: Path to script to execute for each task
            log_paths: Log paths, one per task
            task_names: Task names, one per task

        Returns:
            List of job_ids for submitted tasks
        """
        job_ids = []

        for task_idx, (log_path, task_name) in enumerate(zip(log_paths, task_names)):
            job_id = str(self._job_counter)
            self._job_counter += 1

            # Ensure log directory exists
            log_path_obj = Path(log_path)
            log_path_obj.parent.mkdir(parents=True, exist_ok=True)

            # Set environment variables for the task
            env = {
                **subprocess.os.environ,
                "TASK_ID": str(task_idx),
                "TASK_NAME": task_name,
            }

            log_file = open(log_path, "w")
            try:
                proc = subprocess.Popen(
                    ["bash", script_path],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env=env,
                    cwd=Path(script_path).parent,
                )

                job = LocalJob(
                    job_id=job_id,
                    name=f"{array_name}_{task_name}",
                    script_path=script_path,
                    log_path=log_path,
                    process=proc,
                    state="RUNNING",
                )
                self._jobs[job_id] = job
                job_ids.append(job_id)

            except Exception:
                log_file.close()
                raise

        return job_ids

    def cancel(self, job_id: str) -> None:
        """Terminate a running process.

        Sends SIGTERM, waits up to 5 seconds, then sends SIGKILL if needed.

        Args:
            job_id: Job to cancel
        """
        job = self._jobs.get(job_id)
        if not job or not job.process:
            return

        if job.process.poll() is None:  # Still running
            job.process.terminate()  # SIGTERM
            try:
                job.process.wait(timeout=5)
                job.state = "CANCELLED"
            except subprocess.TimeoutExpired:
                job.process.kill()  # SIGKILL
                job.process.wait()
                job.state = "CANCELLED"

    def remove(self, job_id: str) -> None:
        """Remove a job from tracking.

        Args:
            job_id: Job to remove
        """
        self._jobs.pop(job_id, None)

    def squeue(self) -> dict[str, str]:
        """Get current status of all tracked jobs.

        Returns:
            Dictionary of job_id -> status
            Statuses: "RUNNING", "COMPLETED", "FAILED", "CANCELLED"
        """
        statuses = {}

        for job_id, job in self._jobs.items():
            if not job.process:
                statuses[job_id] = job.state
                continue

            return_code = job.process.poll()

            if return_code is None:
                # Still running
                statuses[job_id] = "RUNNING"
                job.state = "RUNNING"
            elif return_code == 0:
                statuses[job_id] = "COMPLETED"
                job.state = "COMPLETED"
                job.return_code = return_code
            else:
                # Non-zero exit = failure
                statuses[job_id] = "FAILED"
                job.state = "FAILED"
                job.return_code = return_code

        return statuses

    def job_ids_by_name(self, name: str) -> list[str]:
        """Get job IDs with matching name.

        Args:
            name: Job name to search for

        Returns:
            List of matching job_ids
        """
        return [job_id for job_id, job in self._jobs.items() if job.name == name]

    def get_job(self, job_id: str) -> LocalJob:
        """Get job details.

        Args:
            job_id: Job identifier

        Returns:
            LocalJob object with details

        Raises:
            KeyError: If job_id not found
        """
        return self._jobs[job_id]

    def cleanup(self) -> None:
        """Clean up all tracked jobs.

        Terminates any running processes and clears job tracking.
        Useful for graceful shutdown.
        """
        for job_id in list(self._jobs.keys()):
            self.cancel(job_id)
            self.remove(job_id)


__all__ = ["LocalCommandClient", "LocalJob"]
