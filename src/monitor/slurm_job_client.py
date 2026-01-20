"""SLURM job client adapter that uses the external slurm_gen library."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from compoconf import ConfigInterface, RegistrableConfigInterface, parse_config, register, register_interface
from slurm_gen import SlurmConfig, generate_script, merge_slurm_config
from slurm_gen.client import (
    BaseSlurmClient,
    FakeSlurmClient as SlurmGenFakeSlurmClient,
    FakeSlurmClientConfig,
    SlurmClient as SlurmGenSlurmClient,
    SlurmClientConfig,
)

from monitor.job_client_protocol import JobClientInterface
from monitor.utils.paths import expand_log_path, update_log_symlink

LOGGER = logging.getLogger(__name__)


@register_interface
class SlurmClientInterface(RegistrableConfigInterface):
    """Registrable adapter interface for slurm_gen clients."""


@register
class SlurmClient(SlurmClientInterface):
    """Adapter for slurm_gen.SlurmClient to integrate with compoconf."""

    config_class = SlurmClientConfig

    def __init__(self, config: SlurmClientConfig) -> None:
        self.config = config
        self.client = SlurmGenSlurmClient(config)


@register
class FakeSlurmClient(SlurmClientInterface):
    """Adapter for slurm_gen.FakeSlurmClient to integrate with compoconf."""

    config_class = FakeSlurmClientConfig

    def __init__(self, config: FakeSlurmClientConfig) -> None:
        self.config = config
        self.client = SlurmGenFakeSlurmClient(config)


@dataclass(kw_only=True)
class SlurmJobClientConfig(ConfigInterface):
    class_name: str = "SlurmJobClient"
    slurm: SlurmConfig | None = None
    slurm_client: SlurmClientInterface.cfgtype | None = None
    output_dir: str | None = None


@register
class SlurmJobClient(JobClientInterface):
    """Generate sbatch scripts via slurm_gen and submit with slurm_gen clients."""

    config_class = SlurmJobClientConfig

    def __init__(self, config: SlurmJobClientConfig) -> None:
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
        slurm_config = self._parse_slurm_config(self._merge_slurm(self.config.slurm, slurm))
        command_to_run = list(slurm_config.command or command)
        script_path = generate_script(
            slurm_config,
            job_name=name,
            log_path=log_path,
            command=command_to_run,
            extra_args=extra_args,
            output_dir=self._output_dir(slurm_config, log_path),
        )
        prepared = replace(
            slurm_config,
            name=name,
            command=command_to_run,
            log_path=log_path,
            script_path=str(script_path),
        )
        job_id = self._slurm_client.submit(prepared)
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
        slurm_config = self._parse_slurm_config(self._merge_slurm(self.config.slurm, slurm))
        command_to_run = list(slurm_config.command or command)
        script_path = generate_script(
            slurm_config,
            job_name=array_name,
            log_path=log_paths[0],
            command=command_to_run,
            extra_args=extra_args,
            output_dir=self._output_dir(slurm_config, log_paths[0]),
        )
        indices = list(range(start_index or 0, (start_index or 0) + len(log_paths)))
        prepared = replace(
            slurm_config,
            name=array_name,
            command=command_to_run,
            log_path=log_paths[0],
            script_path=str(script_path),
        )
        job_ids = self._slurm_client.submit_array(prepared, indices)
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
        if self.config.output_dir:
            merged.setdefault("script_dir", self.config.output_dir)
            merged.setdefault("log_dir", self.config.output_dir)
        return parse_config(SlurmConfig, merged)

    def _merge_slurm(self, base: dict[str, Any] | SlurmConfig | None, override: dict[str, Any] | SlurmConfig | None) -> dict[str, Any]:
        base_dict = _slurm_to_dict(base)
        override_dict = _slurm_to_dict(override)
        return merge_slurm_config(base_dict, override_dict)

    def _build_client(self) -> BaseSlurmClient:
        if not self.config.slurm_client:
            return SlurmGenFakeSlurmClient(FakeSlurmClientConfig())
        wrapper = self.config.slurm_client.instantiate(SlurmClientInterface)
        return wrapper.client

    def _output_dir(self, slurm_config: SlurmConfig, log_path: str) -> str:
        if slurm_config.script_dir:
            return slurm_config.script_dir
        if slurm_config.log_dir:
            return slurm_config.log_dir
        return str(Path(log_path).expanduser().parent)


def _slurm_to_dict(value: dict[str, Any] | SlurmConfig | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, SlurmConfig):
        from dataclasses import asdict

        return asdict(value)
    return dict(value)


__all__ = [
    "SlurmJobClient",
    "SlurmJobClientConfig",
    "SlurmClient",
    "FakeSlurmClient",
    "SlurmClientInterface",
]
