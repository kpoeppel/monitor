"""SBATCH script generation helpers."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import time
from typing import Any

from slurm_gen.schema import SlurmConfig
from slurm_gen.template_renderer import render_template_file


def build_sbatch_directives(config: SlurmConfig) -> list[str]:
    directives: list[str] = []
    sbatch_values = asdict(config.sbatch)
    for key, value in sbatch_values.items():
        if value is None:
            continue
        flag = key if key.startswith("--") else f"--{key.replace('_', '-')}"
        if value is True:
            directives.append(f"#SBATCH {flag}")
        else:
            directives.append(f"#SBATCH {flag}={value}")
    for line in config.sbatch_extra_directives:
        directives.append(line if line.startswith("#SBATCH") else f"#SBATCH {line}")
    return directives


def build_replacements(
    config: SlurmConfig,
    *,
    job_name: str,
    log_path: str,
    command: list[str],
    extra_args: list[str] | None = None,
) -> dict[str, str]:
    full_command = [*command, *(extra_args or [])]
    env_exports = "\n".join(f"export {key}={value}" for key, value in (config.env or {}).items())
    return {
        "job_name": job_name,
        "log_path": log_path,
        "command": " ".join(full_command),
        "sbatch_directives": "\n".join(build_sbatch_directives(config)),
        "env_exports": env_exports,
        "launcher_cmd": config.launcher_cmd or "",
        "srun_opts": config.srun_opts or "",
        "launcher_env_passthrough": str(config.launcher_env_passthrough).lower(),
    }


def generate_script(
    config: SlurmConfig,
    *,
    job_name: str,
    log_path: str,
    command: list[str],
    extra_args: list[str] | None = None,
    output_dir: str | Path | None = None,
    script_name: str | None = None,
    now_ms: int | None = None,
) -> Path:
    if not config.template_path:
        raise ValueError("SlurmConfig.template_path is required")

    script_dir: Path
    if config.script_dir:
        script_dir = Path(config.script_dir)
    elif output_dir is not None:
        script_dir = Path(output_dir)
    else:
        script_dir = Path(log_path).expanduser().parent

    timestamp = int(time.time() * 1000) if now_ms is None else now_ms
    script_name = script_name or f"{job_name}_{timestamp}.sbatch"
    script_path = script_dir / script_name

    replacements = build_replacements(
        config,
        job_name=job_name,
        log_path=log_path,
        command=command,
        extra_args=extra_args,
    )
    render_template_file(config.template_path, script_path, replacements)
    return script_path


def merge_slurm_config(base: dict[str, Any] | None, override: dict[str, Any] | None) -> dict[str, Any]:
    if not base:
        return dict(override or {})
    if not override:
        return dict(base)
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_slurm_config(merged[key], value)
        else:
            merged[key] = value
    return merged


__all__ = [
    "build_sbatch_directives",
    "build_replacements",
    "generate_script",
    "merge_slurm_config",
]
