"""Monitor state definitions used by controller and monitors."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from compoconf import ConfigInterface, RegistrableConfigInterface, register, register_interface, parse_config

LOGGER = logging.getLogger(__name__)


@register_interface
class MonitorStateInterface(RegistrableConfigInterface):
    """Interface representing a monitor-produced state change."""


class BaseMonitorState(MonitorStateInterface):
    config: ConfigInterface

    def __init__(self, config: ConfigInterface) -> None:
        self.config = config

    @property
    def key(self) -> str:  # pragma: no cover - overridden by subclasses
        if hasattr(self.config, "key"):
            return self.config.key
        else:
            raise NotImplementedError


@dataclass
class SuccessStateConfig(ConfigInterface):
    class_name: str = "SuccessState"
    key: str = "success"


@register
class SuccessState(BaseMonitorState):
    config: SuccessStateConfig


@dataclass
class CrashStateConfig(ConfigInterface):
    class_name: str = "CrashState"
    key: str = "crash"


@register
class CrashState(BaseMonitorState):
    config: CrashStateConfig


@dataclass
class StalledStateConfig(ConfigInterface):
    class_name: str = "StalledState"
    key: str = "stall"


@register
class StalledState(BaseMonitorState):
    config: StalledStateConfig


@dataclass
class TimeoutStateConfig(ConfigInterface):
    class_name: str = "TimeoutState"
    key: str = "timeout"


@register
class TimeoutState(BaseMonitorState):
    config: TimeoutStateConfig


@dataclass
class StartedStateConfig(ConfigInterface):
    class_name: str = "StartedState"
    key: str = "running"


@register
class StartedState(BaseMonitorState):
    config: StartedStateConfig


@dataclass
class UndefinedStateConfig(ConfigInterface):
    class_name: str = "UndefinedState"
    key: str = "undefined"


@register
class UndefinedState(BaseMonitorState):
    config: UndefinedStateConfig


@dataclass
class PendingStateConfig(ConfigInterface):
    class_name: str = "PendingState"
    key: str = "pending"


@register
class PendingState(BaseMonitorState):
    config: PendingStateConfig


def get_state(name: str) -> BaseMonitorState | None:
    """
    Get a state instance by its key or class name.
    
    Examples:
        get_state("success") -> SuccessState
        get_state("running") -> StartedState
        get_state("StalledState") -> StalledState
    """
    if not name:
        return None

    # Mapping of common keys to class names
    mapping = {
        "success": "SuccessState",
        "crash": "CrashState",
        "stall": "StalledState",
        "stalled": "StalledState",
        "timeout": "TimeoutState",
        "running": "StartedState",
        "started": "StartedState",
        "pending": "PendingState",
        "undefined": "UndefinedState",
    }
    
    key = name.lower()
    class_name = mapping.get(key) or name
    if not class_name.endswith("State"):
        class_name = class_name[0].upper() + class_name[1:] + "State"

    try:
        return parse_config(MonitorStateInterface.cfgtype, {"class_name": class_name}).instantiate(MonitorStateInterface)
    except (KeyError, AttributeError, ValueError): # pragma: no cover
        return None