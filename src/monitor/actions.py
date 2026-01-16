"""Actions triggered by monitor events."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
import subprocess
from typing import Any, Literal
import logging
from collections.abc import Mapping
import re

from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    register,
    register_interface,
)

from monitor.events import EventRecord, ActionResult, EventStatus

LOGGER = logging.getLogger(__name__)

_PATTERN = re.compile(r"(?<!\$)\{([^\{\}\$:]+)\}")


def replace_braced_keys(s: str, values: Mapping[str, Any]) -> str:
    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        return str(values[key]) if key in values else m.group(0)

    return _PATTERN.sub(repl, s)


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
class _LegacyLogMessageActionConfig(LogActionConfig):
    class_name: str = "LogMessageAction"  # pragma: no cover


@register
class LogMessageAction(LogAction):  # pragma: no cover
    config: _LegacyLogMessageActionConfig  # pragma: no cover


@dataclass
class ShellActionConfig(ConfigInterface):
    class_name: str = "ShellAction"
    command: str = ""


@register
class ShellAction(BaseMonitorAction):
    config: ShellActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        cmd = context.render(self.config.command)
        try:
            proc = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                cwd=context.workspace,
            )
            if proc.returncode == 0:
                return ActionResult(status="success", message="Shell command succeeded")
            return ActionResult(
                status="failed",
                message=f"Shell command failed ({proc.returncode}): {proc.stderr.strip()}",
            )
        except Exception as e:  # pragma: no cover
            return ActionResult(status="failed", message=f"Shell execution error: {e}")


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


@dataclass
class RestartActionConfig(ConfigInterface):
    class_name: str = "RestartAction"
    reason: str = "auto_restart"


@register
class RestartAction(BaseMonitorAction):
    config: RestartActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        return ActionResult(
            status="retry",
            message=self.config.reason,
        )


@dataclass
class AutoExpRestartActionConfig(RestartActionConfig):
    class_name: str = "AutoExpRestartAction"


@register
class AutoExpRestartAction(RestartAction):
    config: AutoExpRestartActionConfig


@dataclass
class PublishEventActionConfig(ConfigInterface):
    class_name: str = "PublishEventAction"
    event_name: str = "custom_event"
    metadata: dict[str, Any] = field(default_factory=dict)
    payload: dict[str, Any] = field(default_factory=dict)


@register
class PublishEventAction(BaseMonitorAction):
    config: PublishEventActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        return ActionResult(
            status="success",
            message=f"Published event: {self.config.event_name}",
            metadata={
                "publish_event": {
                    "name": self.config.event_name,
                    "metadata": self.config.metadata,
                    "payload": self.config.payload,
                }
            },
        )


@dataclass
class RunAutoExpActionConfig(ConfigInterface):
    class_name: str = "RunAutoExpAction"
    script: str = ""
    overrides: list[str] = field(default_factory=list)
    config_path: str = ""


@register
class RunAutoExpAction(BaseMonitorAction):
    config: RunAutoExpActionConfig

    def execute(self, context: ActionContext) -> ActionResult:
        import sys
        cmd = [sys.executable, self.config.script]
        if self.config.config_path:
            cmd.extend(["--config-ref", self.config.config_path])
        for override in self.config.overrides:
            cmd.append(context.render(override))
        cmd.append("--no-monitor")
        session_id = context.variables.get("session_id", "unknown")
        cmd.extend(["--plan-id", session_id])
        cwd = str(context.workspace) if context.workspace else None
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
        if proc.returncode == 0:
            return ActionResult(
                status="success",
                message=f"run_autoexp completed",
                metadata={"stdout": proc.stdout.strip()},
            )
        return ActionResult(  # pragma: no cover
            status="failed",
            message=f"run_autoexp exited {proc.returncode}",
            metadata={"stderr": proc.stderr.strip()},
        )


# Aliases for backwards compatibility
RunAutoexpAction = RunAutoExpAction
RunAutoexpActionConfig = RunAutoExpActionConfig

__all__ = [
    "ActionContext",
    "ActionResult",
    "BaseMonitorAction",
    "LogAction",
    "ShellAction",
    "RestartAction",
    "RunCommandAction",
    "AutoExpRestartAction",
    "RunAutoExpAction",
    "RunAutoexpAction",
    "RunAutoexpActionConfig",
    "PublishEventAction",
    "PublishEventActionConfig",
]
