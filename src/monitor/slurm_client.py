"""SLURM client implementations for job submission and management."""

from __future__ import annotations

import logging
import re
import shlex
import time
from dataclasses import MISSING, dataclass, field
from pathlib import Path

from compoconf import ConfigInterface, RegistrableConfigInterface, register, register_interface

from slurm_gen.shell import run_command

LOGGER = logging.getLogger(__name__)


@register_interface
class SlurmClientInterface(RegistrableConfigInterface):
    """Abstraction layer over SLURM command interactions."""


@dataclass(kw_only=True)
class SlurmJob:
    """Metadata tracked for submitted SLURM jobs."""

    job_id: str = field(default_factory=MISSING)
    name: str = field(default_factory=MISSING)
    script_path: str = field(default_factory=MISSING)
    log_path: str = field(default_factory=MISSING)
    state: str = "PENDING"
    return_code: int | None = None
    submitted_at: float = field(default_factory=time.time)


class BaseSlurmClient(SlurmClientInterface):
    """Base functionality shared by SLURM client implementations."""

    config: ConfigInterface
    supports_array: bool = False

    def __init__(self, config: ConfigInterface) -> None:
        self.config = config

    def submit(self, name: str, script_path: str, log_path: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def submit_array(
        self,
        array_name: str,
        script_path: str,
        log_paths: list[str],
        task_names: list[str],
        start_index: int = 0,
    ) -> list[str]:  # pragma: no cover
        raise NotImplementedError

    def cancel(self, job_id: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def remove(self, job_id: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def squeue(self) -> dict[str, str]:  # pragma: no cover
        raise NotImplementedError

    def job_ids_by_name(self, name: str) -> list[str]:  # pragma: no cover
        raise NotImplementedError

    def get_job(self, job_id: str) -> SlurmJob:  # pragma: no cover
        raise NotImplementedError


@dataclass(kw_only=True)
class BaseSlurmClientConfig(ConfigInterface):
    """Shared configuration fields for SLURM clients."""

    class_name: str = "BaseSlurmClient"
    persist_artifacts: bool = False


@dataclass(kw_only=True)
class FakeSlurmClientConfig(BaseSlurmClientConfig):
    """Configuration for the fake SLURM client."""

    class_name: str = "FakeSlurmClient"
    persist_artifacts: bool = False


@register
class FakeSlurmClient(BaseSlurmClient):
    """In-memory SLURM simulator for testing."""

    config: FakeSlurmClientConfig
    supports_array = True

    def __init__(self, config: FakeSlurmClientConfig) -> None:
        super().__init__(config)
        self._jobs: dict[str, SlurmJob] = {}
        self._next_id = 1

    def submit(self, name: str, script_path: str, log_path: str) -> str:
        job_id = str(self._next_id)
        self._next_id += 1
        job = SlurmJob(job_id=job_id, name=name, script_path=script_path, log_path=log_path)
        job.state = "PENDING"
        self._jobs[job_id] = job
        if self.config.persist_artifacts:
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            Path(log_path).touch(exist_ok=True)
        return job_id

    def submit_array(
        self,
        array_name: str,
        script_path: str,
        log_paths: list[str],
        task_names: list[str],
        start_index: int = 0,
    ) -> list[str]:
        job_ids: list[str] = []
        base_id = str(self._next_id)
        self._next_id += 1

        for offset, (log_path, task_name) in enumerate(zip(log_paths, task_names)):
            array_idx = start_index + offset
            task_job_id = f"{base_id}_{array_idx}"
            job_name = f"{array_name}_{task_name}"

            job = SlurmJob(
                job_id=task_job_id,
                name=job_name,
                script_path=script_path,
                log_path=log_path,
            )
            job.state = "PENDING"
            self._jobs[task_job_id] = job

            if self.config.persist_artifacts:
                Path(log_path).parent.mkdir(parents=True, exist_ok=True)
                Path(log_path).touch(exist_ok=True)

            job_ids.append(task_job_id)

        return job_ids

    def cancel(self, job_id: str) -> None:
        if job_id in self._jobs:
            self._jobs[job_id].state = "CANCELLED"

    def remove(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)

    def squeue(self) -> dict[str, str]:
        return {job_id: job.state for job_id, job in self._jobs.items()}

    def job_ids_by_name(self, name: str) -> list[str]:
        return [job_id for job_id, job in self._jobs.items() if job.name == name]

    def get_job(self, job_id: str) -> SlurmJob:
        return self._jobs[job_id]

    def set_state(self, job_id: str, state: str, return_code: int | None = None) -> None:
        """Set the state of a job (for testing)."""
        job = self._jobs[job_id]
        job.state = state
        if return_code is not None:
            job.return_code = return_code

    def register_job(
        self,
        job_id: str,
        name: str,
        script_path: str,
        log_path: str,
        state: str = "PENDING",
    ) -> str:
        """Register an externally submitted job for tracking."""
        job = SlurmJob(
            job_id=job_id,
            name=name,
            script_path=script_path,
            log_path=log_path,
            state=state,
        )
        self._jobs[job_id] = job
        try:
            base_id = int(str(job_id).split("_")[0])
        except ValueError:
            base_id = self._next_id
        self._next_id = max(self._next_id, base_id + 1)
        return job_id


@dataclass(kw_only=True)
class SlurmClientConfig(BaseSlurmClientConfig):
    """Configuration for the real SLURM client."""

    class_name: str = "SlurmClient"
    submit_cmd: str = "sbatch"
    squeue_cmd: str = "squeue"
    cancel_cmd: str = "scancel"
    sacct_cmd: str = "sacct"


@register
class SlurmClient(BaseSlurmClient):
    """SLURM client that executes real SLURM commands."""

    config: SlurmClientConfig
    supports_array = True

    def __init__(self, config: SlurmClientConfig) -> None:
        super().__init__(config)
        self._jobs: dict[str, SlurmJob] = {}

    def submit(self, name: str, script_path: str, log_path: str) -> str:
        submit_cmd = shlex.split(self.config.submit_cmd)
        proc = run_command([*submit_cmd, str(script_path)])
        if proc.returncode != 0:
            raise RuntimeError(f"sbatch failed for {script_path}: {proc.stderr.strip()}")
        job_id = self._parse_job_id(proc.stdout)
        if job_id is None:
            raise RuntimeError(f"Unable to parse job id from sbatch output: {proc.stdout.strip()}")
        if self.config.persist_artifacts:
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            Path(log_path).touch(exist_ok=True)
        self._jobs[job_id] = SlurmJob(job_id=job_id, name=name, script_path=script_path, log_path=log_path)
        return job_id

    def submit_array(
        self,
        array_name: str,
        script_path: str,
        log_paths: list[str],
        task_names: list[str],
        start_index: int = 0,
    ) -> list[str]:
        num_tasks = len(task_names)
        submit_cmd = shlex.split(self.config.submit_cmd)
        array_range = f"{start_index}-{start_index + num_tasks - 1}"
        proc = run_command([*submit_cmd, f"--array={array_range}", str(script_path)])

        if proc.returncode != 0:
            raise RuntimeError(f"sbatch failed for array {script_path}: {proc.stderr.strip()}")

        base_job_id = self._parse_job_id(proc.stdout)
        if base_job_id is None:
            raise RuntimeError(f"Unable to parse job id from sbatch output: {proc.stdout.strip()}")

        job_ids: list[str] = []
        for offset, (log_path, task_name) in enumerate(zip(log_paths, task_names)):
            array_idx = start_index + offset
            task_job_id = f"{base_job_id}_{array_idx}"
            job_name = f"{array_name}_{task_name}"

            if self.config.persist_artifacts:
                Path(log_path).parent.mkdir(parents=True, exist_ok=True)
                Path(log_path).touch(exist_ok=True)

            job = SlurmJob(
                job_id=task_job_id,
                name=job_name,
                script_path=script_path,
                log_path=log_path,
            )
            self._jobs[task_job_id] = job
            job_ids.append(task_job_id)

        return job_ids

    def cancel(self, job_id: str) -> None:
        cmd = shlex.split(self.config.cancel_cmd)
        run_command([*cmd, str(job_id)])

    def remove(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)

    def squeue(self) -> dict[str, str]:
        if not self._jobs:
            return {}

        cmd = shlex.split(self.config.squeue_cmd)
        format_arg = ["--noheader", "--format", "%i %T"]

        job_ids = list(self._jobs.keys())
        job_id_to_key = {str(jid): jid for jid in job_ids}

        job_ids_str = ",".join(str(jid) for jid in job_ids)
        full_cmd = [*cmd, "--jobs", job_ids_str, *format_arg]

        proc = run_command(full_cmd)
        if proc.returncode != 0:
            return self._check_sacct_for_missing_jobs(job_ids, job_id_to_key)

        statuses: dict[str, str] = {}

        for line in proc.stdout.strip().splitlines():
            parts = line.strip().split(None, 1)
            if not parts:
                continue

            slurm_id = parts[0]
            state = parts[1] if len(parts) > 1 else "UNKNOWN"

            if slurm_id in job_id_to_key:
                statuses[job_id_to_key[slurm_id]] = state

        missing_jobs = [jid for jid in job_ids if jid not in statuses]
        if missing_jobs:
            missing_id_to_key = {str(jid): jid for jid in missing_jobs}
            sacct_statuses = self._check_sacct_for_missing_jobs(missing_jobs, missing_id_to_key)
            statuses.update(sacct_statuses)

        return statuses

    def _check_sacct_for_missing_jobs(
        self, job_ids: list[str], job_id_to_key: dict[str, str]
    ) -> dict[str, str]:
        if not job_ids:
            return {}

        sacct_cmd = shlex.split(self.config.sacct_cmd)

        job_ids_str = ",".join(str(jid) for jid in job_ids)
        full_cmd = [
            *sacct_cmd,
            "--jobs",
            job_ids_str,
            "--noheader",
            "--format",
            "JobID,State",
        ]

        proc = run_command(full_cmd)
        if proc.returncode != 0:
            return {}

        statuses: dict[str, str] = {}
        for line in proc.stdout.strip().splitlines():
            parts = line.strip().split(None, 1)
            if not parts:
                continue
            slurm_id = parts[0]
            state = parts[1] if len(parts) > 1 else "UNKNOWN"
            if slurm_id in job_id_to_key:
                statuses[job_id_to_key[slurm_id]] = state
        return statuses

    @staticmethod
    def _parse_job_id(stdout: str) -> str | None:
        match = re.search(r"Submitted batch job (\d+)", stdout)
        if match:
            return match.group(1)
        return None


FakeSlurm = FakeSlurmClient

__all__ = [
    "BaseSlurmClient",
    "BaseSlurmClientConfig",
    "FakeSlurmClient",
    "FakeSlurmClientConfig",
    "SlurmClient",
    "SlurmClientConfig",
    "SlurmJob",
    "SlurmClientInterface",
    "FakeSlurm",
]
