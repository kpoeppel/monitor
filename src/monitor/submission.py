"""Job registration and state management for submission."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, MISSING
from typing import Any

from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    parse_config,
    register,
    register_interface,
)
from slurm_gen import SlurmConfig
from monitor.conditions import MonitorConditionInterface
from monitor.actions import LogEventConfig

LOGGER = logging.getLogger(__name__)


@register_interface
class JobRegistrationInterface(RegistrableConfigInterface):
    """Registrable interface for job registrations."""


@dataclass(kw_only=True)
class JobRegistration:
    """Configuration for a job to be monitored."""

    name: str
    command: list[str]
    log_path: str
    log_path_current: str | None = None
    extra_args: list[str] = field(default_factory=list)
    log_to_file: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)
    slurm: SlurmConfig | None = None
    log_events: list[LogEventConfig] = field(default_factory=list)
    start_condition: MonitorConditionInterface.cfgtype | None = None
    cancel_condition: MonitorConditionInterface.cfgtype | None = None
    finish_condition: MonitorConditionInterface.cfgtype | None = None
    job_kind: str | None = None


@dataclass(kw_only=True)
class LocalJobRegistrationConfig(JobRegistration, ConfigInterface):
    class_name: str = "LocalJobRegistration"
    name: str = ""
    command: list[str] = field(default_factory=list)
    log_path: str = ""
    log_path_current: str | None = None
    extra_args: list[str] = field(default_factory=list)
    log_to_file: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)
    log_events: list[LogEventConfig] = field(default_factory=list)
    start_condition: MonitorConditionInterface.cfgtype | None = None
    cancel_condition: MonitorConditionInterface.cfgtype | None = None
    finish_condition: MonitorConditionInterface.cfgtype | None = None


@register
class LocalJobRegistration(JobRegistrationInterface):
    config: LocalJobRegistrationConfig

    def __init__(self, config: LocalJobRegistrationConfig) -> None:
        self.config = config

    def to_registration(self) -> LocalJobRegistrationConfig:
        return self.config


@dataclass(kw_only=True)
class SlurmJobRegistrationConfig(LocalJobRegistrationConfig):
    class_name: str = "SlurmJobRegistration"
    slurm: SlurmConfig = field(default_factory=MISSING)


@register
class SlurmJobRegistration(JobRegistrationInterface):
    config: SlurmJobRegistrationConfig

    def __init__(self, config: SlurmJobRegistrationConfig) -> None:
        self.config = config

    def to_registration(self) -> SlurmJobRegistrationConfig:
        return self.config


def parse_job_registration(payload: dict[str, Any] | JobRegistration) -> JobRegistration:
    if isinstance(payload, JobRegistration):
        return payload
    data = dict(payload)
    config = parse_config(JobRegistrationInterface.cfgtype, data)
    registration = config.instantiate(JobRegistrationInterface)
    return registration.to_registration()
