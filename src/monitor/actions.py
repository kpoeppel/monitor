"""Actions and event definitions for monitor."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
import hashlib
import json
import logging
import subprocess
import time
from typing import Any, Literal

from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    parse_config,
    register,
    register_interface,
)

from monitor.conditions import MonitorConditionInterface
from monitor.utils.template import replace_braced_keys
from slurm_gen import SlurmConfig

LOGGER = logging.getLogger(__name__)


class EventStatus(Enum):
    """Lifecycle status of a detected event."""

    PENDING = "pending"
    PROCESSED = "processed"
    FAILED = "failed"


@dataclass(kw_only=True)
class EventRecord:
    """Persistent record of a detected event and its action history."""

    event_id: str
    name: str
    source: str
    status: EventStatus = EventStatus.PENDING
    count: int = 1
    first_seen_ts: float = field(default_factory=time.time)
    last_seen_ts: float = field(default_factory=time.time)
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)

    def touch(self, *, payload: dict[str, Any] | None = None) -> None:
        """Increment the occurrence counter and update timestamps."""
        self.count += 1
        self.last_seen_ts = time.time()
        if payload:
            self.payload.update(payload)

    def set_status(self, status: EventStatus, *, note: str | None = None) -> None:
        """Move event into a new lifecycle state and append optional note."""
        self.status = status
        self.last_seen_ts = time.time()
        if note:
            self.history.append({"ts": self.last_seen_ts, "status": status.value, "note": note})  # pragma: no cover


@dataclass(kw_only=True)
class ActionResult:
    """Outcome of an action execution."""

    status: str  # success, retry, failed
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def event_key(job_id: str, event_name: str, metadata: dict[str, Any] | None = None) -> tuple[str, str]:
    h = hashlib.md5()
    h.update(json.dumps(metadata).encode("utf8"))
    h = str(h.digest())[:16]
    return (str(job_id), event_name, h)


def build_event_id(
    job_id: str,
    event_name: str,
    metadata: dict[str, Any] | None = None,
    *,
    now_ms: int | None = None,
) -> str:
    timestamp = int(time.time() * 1000) if now_ms is None else now_ms
    if metadata and "checkpoint_iteration" in metadata:
        return f"{job_id}:{event_name}:{metadata['checkpoint_iteration']}:{timestamp}"
    return f"{job_id}:{event_name}:{timestamp}"


@dataclass(kw_only=True)
class ActionContext:
    event: EventRecord
    job_metadata: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    workspace: Path | None = None

    @property
    def variables(self) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        merged.update(self.job_metadata)
        merged.update(self.event.metadata)
        merged.update(self.event.payload)
        merged.setdefault("event_id", self.event.event_id)
        merged.setdefault("event_name", self.event.name)
        merged.setdefault("attempts", self.attempts)
        if self.workspace:
            merged.setdefault("workspace", str(self.workspace))
        return merged

    def render(self, template: str) -> str:
        try:
            return replace_braced_keys(template, self.variables)
        except KeyError:  # pragma: no cover
            return template


@register_interface
class BaseMonitorAction(RegistrableConfigInterface):
    config: ConfigInterface

    def __init__(self, config: ConfigInterface) -> None:
        self.config = config

    def describe(self, job_id: str, metadata: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        payload = asdict(self.config)
        payload.update({"type": self.kind})
        return payload

    def execute(self, context: ActionContext) -> ActionResult:  # pragma: no cover
        raise NotImplementedError

    def update_event(self, event: EventRecord, result: ActionResult) -> None:
        if result.status == "success":
            event.set_status(EventStatus.PROCESSED, note=result.message)
        elif result.status == "retry":
            event.set_status(EventStatus.PENDING, note=result.message)
        else:
            event.set_status(EventStatus.FAILED, note=result.message)


@dataclass
class LogActionConfig(ConfigInterface):
    class_name: str = "LogAction"
    message: str = "Event {event_name} triggered"
    level: str = "info"


@register
class LogAction(BaseMonitorAction):
    config: LogActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        msg = context.render(self.config.message)
        level = self.config.level.lower()
        if level == "debug":
            LOGGER.debug(msg)
        elif level == "warning":
            LOGGER.warning(msg)
        elif level == "error":
            LOGGER.error(msg)
        else:
            LOGGER.info(msg)
        return ActionResult(status="success", message=msg)


@dataclass
class RunCommandActionConfig(ConfigInterface):
    class_name: str = "RunCommandAction"
    command: list[str] = field(default_factory=list)
    cwd: str | None = None


@register
class RunCommandAction(BaseMonitorAction):
    config: RunCommandActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        if not self.config.command:
            return ActionResult(status="failed", message="command is empty")
        rendered = [context.render(segment) for segment in self.config.command]
        cwd = self.config.cwd or (str(context.workspace) if context.workspace else None)
        try:
            proc = subprocess.run(
                rendered,
                capture_output=True,
                text=True,
                cwd=cwd,
            )
            if proc.returncode == 0:
                return ActionResult(
                    status="success",
                    message="command completed",
                    metadata={"stdout": proc.stdout.strip()},
                )
            return ActionResult(
                status="failed",
                message=f"command exited {proc.returncode}",
                metadata={"stdout": proc.stdout.strip(), "stderr": proc.stderr.strip()},
            )
        except Exception as e:  # pragma: no cover
            return ActionResult(status="failed", message=f"Command execution error: {e}")


@register_interface
class ActionBackendInterface(RegistrableConfigInterface):
    """Backend-specific configuration for actions."""


@dataclass
class ActionBackendConfig(ConfigInterface):
    class_name: str = "ActionBackend"
    job_kind: str = ""


@dataclass
class LocalActionBackendConfig(ActionBackendConfig):
    class_name: str = "LocalActionBackend"
    job_kind: str = "local"


@dataclass
class SlurmActionBackendConfig(ActionBackendConfig):
    class_name: str = "SlurmActionBackend"
    job_kind: str = "slurm"
    slurm: SlurmConfig | None = None


@register
class ActionBackend(ActionBackendInterface):
    config: ActionBackendConfig

    def __init__(self, config: ActionBackendConfig) -> None:
        self.config = config


@register
class LocalActionBackend(ActionBackendInterface):
    config: LocalActionBackendConfig

    def __init__(self, config: LocalActionBackendConfig) -> None:
        self.config = config


@register
class SlurmActionBackend(ActionBackendInterface):
    config: SlurmActionBackendConfig

    def __init__(self, config: SlurmActionBackendConfig) -> None:
        self.config = config


@dataclass
class BaseRestartActionConfig(ConfigInterface):
    reason: str = "auto_restart"
    command: list[str] | None = None
    log_path: str | None = None
    log_path_current: str | None = None
    extra_args: list[str] | None = None
    extra_args_append: list[str] | None = None
    metadata: dict[str, Any] | None = None
    log_to_file: bool | None = None
    backend_config: ActionBackendInterface.cfgtype | None = None


@dataclass
class RestartActionConfig(BaseRestartActionConfig):
    class_name: str = "RestartAction"


@register
class RestartAction(BaseMonitorAction):
    config: RestartActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        mismatch = _ensure_backend(context, self.config.backend_config)
        if mismatch:
            return mismatch
        adjustments = _build_adjustments(self.config, context)
        return ActionResult(
            status="retry",
            message=self.config.reason,
            metadata={"adjustments": adjustments},
        )


def _render_adjustment_value(value: Any, context: ActionContext) -> Any:
    if isinstance(value, str):
        return context.render(value)
    if isinstance(value, list):
        return [_render_adjustment_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_adjustment_value(item, context) for key, item in value.items()}
    return value


def _build_adjustments(
    config: BaseRestartActionConfig | BaseDuplicateActionConfig,
    context: ActionContext,
) -> dict[str, Any]:
    adjustments: dict[str, Any] = {}
    if config.command is not None:
        adjustments["command"] = [context.render(arg) for arg in config.command]
    if config.log_path is not None:
        adjustments["log_path"] = context.render(config.log_path)
    if config.log_path_current is not None:
        adjustments["log_path_current"] = context.render(config.log_path_current)
    if config.extra_args is not None:
        adjustments["extra_args"] = [context.render(arg) for arg in config.extra_args]
    if config.extra_args_append is not None:
        adjustments["extra_args_append"] = [context.render(arg) for arg in config.extra_args_append]
    if config.metadata is not None:
        adjustments["metadata"] = _render_adjustment_value(config.metadata, context)
    if config.log_to_file is not None:
        adjustments["log_to_file"] = config.log_to_file
    backend_config = _coerce_backend_config(getattr(config, "backend_config", None))
    if backend_config is not None and getattr(backend_config, "slurm", None) is not None:
        adjustments["slurm"] = _render_adjustment_value(getattr(backend_config, "slurm"), context)
    return adjustments


def _ensure_backend(
    context: ActionContext,
    backend_config: ActionBackendInterface.cfgtype | None,
) -> ActionResult | None:
    backend_config = _coerce_backend_config(backend_config)
    if backend_config is None:
        return None
    expected_kind = getattr(backend_config, "job_kind", "")
    if not expected_kind:
        return None
    job_kind = context.job_metadata.get("job_kind")
    if job_kind and job_kind != expected_kind:
        return ActionResult(
            status="failed",
            message=f"action requires job_kind '{expected_kind}' but got '{job_kind}'",
        )
    return None


def _coerce_backend_config(
    backend_config: ActionBackendInterface.cfgtype | None,
) -> ActionBackendConfig | None:
    if backend_config is None:
        return None
    if isinstance(backend_config, dict):
        raise TypeError("backend_config must be parsed config, not dict")
    return backend_config


@dataclass
class BaseDuplicateActionConfig(ConfigInterface):
    name_suffix: str = "_dup"
    command: list[str] | None = None
    log_path: str | None = None
    log_path_current: str | None = None
    extra_args: list[str] | None = None
    extra_args_append: list[str] | None = None
    metadata: dict[str, Any] | None = None
    log_to_file: bool | None = None
    backend_config: ActionBackendInterface.cfgtype | None = None


@dataclass
class DuplicateActionConfig(BaseDuplicateActionConfig):
    class_name: str = "DuplicateAction"


@register
class DuplicateAction(BaseMonitorAction):
    config: DuplicateActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        mismatch = _ensure_backend(context, self.config.backend_config)
        if mismatch:
            return mismatch
        adjustments = _build_adjustments(self.config, context)
        name_suffix = context.render(self.config.name_suffix)
        return ActionResult(
            status="success",
            message="duplicate requested",
            metadata={"duplicate_job": {"adjustments": adjustments, "name_suffix": name_suffix}},
        )


@dataclass
class FinishActionConfig(ConfigInterface):
    class_name: str = "FinishAction"
    reason: str = "finished"
    backend_config: ActionBackendInterface.cfgtype | None = None


@register
class FinishAction(BaseMonitorAction):
    config: FinishActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        mismatch = _ensure_backend(context, self.config.backend_config)
        if mismatch:
            return mismatch
        return ActionResult(
            status="success",
            message=self.config.reason,
            metadata={"finalize": "success"},
        )


@dataclass
class CancelActionConfig(ConfigInterface):
    class_name: str = "CancelAction"
    reason: str = "cancelled"
    backend_config: ActionBackendInterface.cfgtype | None = None


@register
class CancelAction(BaseMonitorAction):
    config: CancelActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        mismatch = _ensure_backend(context, self.config.backend_config)
        if mismatch:
            return mismatch
        return ActionResult(
            status="success",
            message=self.config.reason,
            metadata={"finalize": "cancel"},
        )


@dataclass(kw_only=True)
class EventActionConfig(ConfigInterface):
    """Single event/action specification (used by concrete event configs)."""

    action: BaseMonitorAction.cfgtype | None = None
    conditions: list[MonitorConditionInterface.cfgtype] = field(default_factory=list)
    action_id: str | None = None


@dataclass
class LogEventConfig(EventActionConfig):
    """Configuration for a log-triggered event and action."""

    class_name: str = "LogEvent"
    name: str = ""
    pattern: str = ""
    pattern_type: Literal["substring", "regex"] = "substring"
    metadata: dict[str, Any] = field(default_factory=dict)
    extract_groups: dict[str, int | str] = field(default_factory=dict)


@dataclass
class EventActionBinding:
    """Runtime binding used by the monitor loop."""

    action: BaseMonitorAction
    conditions: list[MonitorConditionInterface]
    action_id: str


def _coerce_action_config(payload: Any) -> BaseMonitorAction.cfgtype:
    if hasattr(payload, "instantiate"):
        return payload
    if isinstance(payload, dict):
        return parse_config(BaseMonitorAction.cfgtype, payload)
    raise TypeError(f"Unsupported action config payload: {payload!r}")  # pragma: no cover


def _coerce_condition_config(payload: Any) -> MonitorConditionInterface.cfgtype:
    if hasattr(payload, "instantiate"):
        return payload
    if isinstance(payload, dict):
        return parse_config(MonitorConditionInterface.cfgtype, payload)
    raise TypeError(f"Unsupported condition config payload: {payload!r}")  # pragma: no cover


def _build_action_id(
    *,
    action_name: str,
    event_name: str,
    kind: str,
    index: int,
    explicit: str | None = None,
) -> str:
    if explicit:
        return explicit
    return f"{kind}:{event_name}:{action_name}:{index}"


def instantiate_action_binding(
    config: EventActionConfig,
    *,
    event_name: str,
    kind: str,
    index: int,
) -> EventActionBinding:
    if config.action is None:
        raise ValueError("EventActionConfig requires 'action'")
    action_cfg = _coerce_action_config(config.action)
    action = action_cfg.instantiate(BaseMonitorAction)
    conditions = [
        _coerce_condition_config(condition).instantiate(MonitorConditionInterface)
        for condition in config.conditions
    ]
    action_id = _build_action_id(
        action_name=action.config.class_name,
        event_name=event_name,
        kind=kind,
        index=index,
        explicit=config.action_id,
    )
    return EventActionBinding(
        action=action,
        conditions=conditions,
        action_id=action_id,
    )


__all__ = [
    "EventStatus",
    "EventRecord",
    "ActionResult",
    "event_key",
    "build_event_id",
    "ActionContext",
    "BaseMonitorAction",
    "ActionBackendInterface",
    "ActionBackendConfig",
    "LocalActionBackendConfig",
    "SlurmActionBackendConfig",
    "ActionBackend",
    "LocalActionBackend",
    "SlurmActionBackend",
    "LogAction",
    "RestartActionConfig",
    "RestartAction",
    "DuplicateActionConfig",
    "DuplicateAction",
    "FinishActionConfig",
    "FinishAction",
    "CancelActionConfig",
    "CancelAction",
    "RunCommandAction",
    "EventActionConfig",
    "LogEventConfig",
    "EventActionBinding",
    "instantiate_action_binding",
]
