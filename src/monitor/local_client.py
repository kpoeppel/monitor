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
from monitor.utils.paths import resolve_log_path, update_log_symlink


@dataclass
class LocalJob:
    """Metadata for a locally executed job."""

    job_id: str
    name: str
    command: list[str]
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

    def submit(
        self,
        name: str,
        command: list[str],
        log_path: str,
        extra_args: list[str] | None = None,
        log_to_file: bool | None = None,
        log_path_current: str | None = None,
        slurm: dict[str, Any] | None = None,
    ) -> str:
        """Submit a local script as a background process.

        Args:
            name: Human-readable job name
            command: Command to execute
            log_path: Path where stdout/stderr will be written

        Returns:
            job_id: String identifier for this job

        Raises:
            FileNotFoundError: If command[0] doesn't exist
            OSError: If process creation fails
        """
        job_id = str(self._job_counter)
        self._job_counter += 1

        log_file = None
        stdout_target = subprocess.DEVNULL
        if log_to_file is None or log_to_file:
            timestamp = int(time.time())
            resolved_log_path = resolve_log_path(log_path, job_id=job_id, timestamp=timestamp)
            log_path_obj = Path(resolved_log_path)
            log_path_obj.parent.mkdir(parents=True, exist_ok=True)
            log_file = open(log_path_obj, "w")
            stdout_target = log_file
            if log_path_current:
                update_log_symlink(log_path_obj, Path(log_path_current))
        try:
            proc = subprocess.Popen(
                [*command, *(extra_args or [])],
                stdout=stdout_target,
                stderr=subprocess.STDOUT if log_file else subprocess.DEVNULL,
                start_new_session=True,  # Detach from terminal
                cwd=self._command_cwd(command),  # Run in command's directory if possible
            )

            job = LocalJob(
                job_id=job_id,
                name=name,
                command=list(command),
                log_path=str(log_path_obj) if log_to_file is None or log_to_file else log_path,
                process=proc,
                state="RUNNING",
            )
            self._jobs[job_id] = job

        except Exception:
            if log_file:
                log_file.close()
            raise

        # Note: log_file will remain open for the lifetime of the process
        # This is intentional - closing it would break output redirection

        return job_id

    def submit_array(
        self,
        array_name: str,
        command: list[str],
        log_paths: list[str],
        task_names: list[str],
        extra_args: list[str] | None = None,
        start_index: int | None = None,
        log_to_file: bool | None = None,
        log_path_current: str | None = None,
        slurm: dict[str, Any] | None = None,
    ) -> list[str]:
        """Submit multiple instances of a script.

        Each task is submitted as an independent process. The script receives
        environment variables TASK_ID (0-indexed) and TASK_NAME.

        Args:
            array_name: Base name for the job array
            command: Command to execute for each task
            log_paths: Log paths, one per task
            task_names: Task names, one per task

        Returns:
            List of job_ids for submitted tasks
        """
        job_ids = []

        for task_idx, (log_path, task_name) in enumerate(zip(log_paths, task_names)):
            job_id = str(self._job_counter)
            self._job_counter += 1

            # Set environment variables for the task
            env = {
                **subprocess.os.environ,
                "TASK_ID": str(task_idx),
                "TASK_NAME": task_name,
            }

            log_file = None
            stdout_target = subprocess.DEVNULL
            if log_to_file is None or log_to_file:
                timestamp = int(time.time())
                resolved_log_path = resolve_log_path(log_path, job_id=job_id, timestamp=timestamp)
                log_path_obj = Path(resolved_log_path)
                log_path_obj.parent.mkdir(parents=True, exist_ok=True)
                log_file = open(log_path_obj, "w")
                stdout_target = log_file
                if log_path_current:
                    update_log_symlink(log_path_obj, Path(log_path_current))
            try:
                proc = subprocess.Popen(
                    [*command, *(extra_args or [])],
                    stdout=stdout_target,
                    stderr=subprocess.STDOUT if log_file else subprocess.DEVNULL,
                    start_new_session=True,
                    env=env,
                    cwd=self._command_cwd(command),
                )

                job = LocalJob(
                    job_id=job_id,
                    name=f"{array_name}_{task_name}",
                    command=list(command),
                    log_path=str(log_path_obj) if log_to_file is None or log_to_file else log_path,
                    process=proc,
                    state="RUNNING",
                )
                self._jobs[job_id] = job
                job_ids.append(job_id)

            except Exception:
                if log_file:
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

    @staticmethod
    def _command_cwd(command: list[str]) -> str | None:
        if not command:
            return None
        candidate = command[0]
        if candidate in {"bash", "sh"} and len(command) > 1:
            candidate = command[1]
        return str(Path(candidate).parent)

    def cleanup(self) -> None:
        """Clean up all tracked jobs.

        Terminates any running processes and clears job tracking.
        Useful for graceful shutdown.
        """
        for job_id in list(self._jobs.keys()):
            self.cancel(job_id)
            self.remove(job_id)


__all__ = ["LocalCommandClient", "LocalJob"]
