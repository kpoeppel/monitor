"""Slurm-gen backed client for JobClientProtocol."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from compoconf import ConfigInterface, parse_config

from monitor.job_client_protocol import JobClientProtocol
from monitor.slurm_client import (
    BaseSlurmClient,
    FakeSlurmClient,
    FakeSlurmClientConfig,
    SlurmClientInterface,
)
from monitor.utils.paths import expand_log_path, update_log_symlink
from slurm_gen.generator import generate_script, merge_slurm_config
from slurm_gen.schema import SlurmConfig

LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class SlurmGenClientConfig(ConfigInterface):
    class_name: str = "SlurmGenClient"
    slurm: dict[str, Any] = field(default_factory=dict)
    slurm_client: dict[str, Any] = field(default_factory=dict)
    output_dir: str | None = None


class SlurmGenClient(JobClientProtocol):
    """Generate sbatch scripts from slurm_gen config and submit via SLURM client."""

    def __init__(self, config: SlurmGenClientConfig) -> None:
        self.config = config
        self._slurm_client = self._build_client()

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
        slurm_config = self._parse_slurm_config(merge_slurm_config(self.config.slurm, slurm))
        command_to_run = list(slurm_config.command or command)
        script_path = generate_script(
            slurm_config,
            job_name=name,
            log_path=log_path,
            command=command_to_run,
            extra_args=extra_args,
            output_dir=self._output_dir(slurm_config, log_path),
        )
        job_id = self._slurm_client.submit(name, str(script_path), log_path)
        if log_path_current:
            resolved_log = expand_log_path(log_path, job_id)
            update_log_symlink(resolved_log, Path(log_path_current))
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
        if not log_paths:
            return []

        slurm_config = self._parse_slurm_config(merge_slurm_config(self.config.slurm, slurm))
        command_to_run = list(slurm_config.command or command)
        script_path = generate_script(
            slurm_config,
            job_name=array_name,
            log_path=log_paths[0],
            command=command_to_run,
            extra_args=extra_args,
            output_dir=self._output_dir(slurm_config, log_paths[0]),
        )
        job_ids = self._slurm_client.submit_array(
            array_name=array_name,
            script_path=str(script_path),
            log_paths=log_paths,
            task_names=task_names,
            start_index=start_index or 0,
        )
        if log_path_current and job_ids:
            resolved_log = expand_log_path(log_paths[0], job_ids[0])
            update_log_symlink(resolved_log, Path(log_path_current))
        return job_ids

    def cancel(self, job_id: str) -> None:
        self._slurm_client.cancel(job_id)

    def remove(self, job_id: str) -> None:
        self._slurm_client.remove(job_id)

    def squeue(self) -> dict[str, str]:
        return self._slurm_client.squeue()

    def _parse_slurm_config(self, payload: dict[str, Any]) -> SlurmConfig:
        merged = dict(payload or {})
        merged.setdefault("script_dir", self.config.output_dir or "")
        merged.setdefault("log_dir", self.config.output_dir or "")
        return parse_config(SlurmConfig, merged)

    def _build_client(self) -> BaseSlurmClient:
        client_payload = self.config.slurm_client
        if not client_payload:
            return FakeSlurmClient(FakeSlurmClientConfig())
        client_config = parse_config(SlurmClientInterface.cfgtype, client_payload)
        return client_config.instantiate(SlurmClientInterface)

    def _output_dir(self, slurm_config: SlurmConfig, log_path: str) -> str:
        if slurm_config.script_dir:
            return slurm_config.script_dir
        if slurm_config.log_dir:
            return slurm_config.log_dir
        return str(Path(log_path).expanduser().parent)


__all__ = ["SlurmGenClient", "SlurmGenClientConfig"]
