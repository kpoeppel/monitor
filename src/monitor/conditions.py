"""Conditions used to gate monitor actions."""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import re
from collections.abc import Mapping


from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    register,
    register_interface,
)

from monitor.events import EventRecord

LOGGER = logging.getLogger(__name__)

ConditionStatus = Literal["pass", "fail", "waiting"]


_PATTERN = re.compile(r"(?<!\$)\{([^\{\}\$:]+)\}")


def replace_braced_keys(s: str, values: Mapping[str, Any]) -> str:
    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        return str(values[key]) if key in values else m.group(0)  # keep as-is if missing

    return _PATTERN.sub(repl, s)


@register_interface
class MonitorConditionInterface(RegistrableConfigInterface):
    """Registrable interface for monitor conditions."""


@dataclass(kw_only=True)
class ConditionResult:
    status: ConditionStatus
    message: str = ""

    @property
    def passed(self) -> bool:
        return self.status == "pass"

    @property
    def waiting(self) -> bool:
        return self.status == "waiting"


@dataclass(kw_only=True)
class ConditionContext:
    event: EventRecord | None = None
    job_metadata: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    extra: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)

    @property
    def variables(self) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        merged.update(self.job_metadata)
        if self.event:
            merged.update(self.event.metadata) # pragma: no cover
            merged.update(self.event.payload) # pragma: no cover
            merged.setdefault("event_id", self.event.event_id) # pragma: no cover
            merged.setdefault("event_name", self.event.name) # pragma: no cover
        return merged

    def render(self, template: str) -> str:
        try:
            return replace_braced_keys(template, self.variables)
        except KeyError: # pragma: no cover
            return template


class BaseCondition(MonitorConditionInterface):
    config: ConfigInterface

    def __init__(self, config: ConfigInterface) -> None:
        self.config = config

    def check(self, context: ConditionContext) -> ConditionResult:  # pragma: no cover
        raise NotImplementedError


@dataclass
class AlwaysTrueConditionConfig(ConfigInterface):
    class_name: str = "AlwaysTrueCondition"
    message: str = ""


@register
class AlwaysTrueCondition(BaseCondition):
    config: AlwaysTrueConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        return ConditionResult(status="pass", message=self.config.message)


@dataclass
class MaxAttemptsConditionConfig(ConfigInterface):
    class_name: str = "MaxAttemptsCondition"
    max_attempts: int = 1


@register
class MaxAttemptsCondition(BaseCondition):
    config: MaxAttemptsConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        if context.attempts < self.config.max_attempts:
            return ConditionResult(status="pass")
        return ConditionResult(
            status="fail",
            message=f"attempts {context.attempts} >= limit {self.config.max_attempts}",
        )


@dataclass
class CooldownConditionConfig(ConfigInterface):
    class_name: str = "CooldownCondition"
    cooldown_seconds: float = 60.0
    note: str | None = None


@register
class CooldownCondition(BaseCondition):
    config: CooldownConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        if context.event is None:
            return ConditionResult(status="pass")  # No event means no cooldown needed
        last_ts = context.event.metadata.get("last_action_ts", context.event.last_seen_ts)
        elapsed = time.time() - float(last_ts)
        if elapsed >= self.config.cooldown_seconds:
            return ConditionResult(status="pass")
        remaining = self.config.cooldown_seconds - elapsed
        return ConditionResult(
            status="waiting",
            message=f"cooldown {remaining:.1f}s remaining",
        )


def _wait_for_predicate(
    predicate,
    *,
    blocking: bool,
    timeout_seconds: float,
    poll_interval_seconds: float,
    waiting_message: str,
) -> ConditionResult:
    start = time.time()
    while True:
        if predicate():
            return ConditionResult(status="pass")
        if not blocking:
            return ConditionResult(status="waiting", message=waiting_message)
        if timeout_seconds and (time.time() - start) >= timeout_seconds:
            return ConditionResult(status="fail", message=f"timeout waiting for {waiting_message}")
        time.sleep(max(poll_interval_seconds, 0.1))


@dataclass
class FileExistsConditionConfig(ConfigInterface):
    class_name: str = "FileExistsCondition"
    path: str = ""
    blocking: bool = False
    timeout_seconds: float = 600.0
    poll_interval_seconds: float = 5.0


@register
class FileExistsCondition(BaseCondition):
    config: FileExistsConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        rendered = context.render(self.config.path)
        target = Path(rendered).expanduser()

        def predicate() -> bool:
            return target.exists()

        return _wait_for_predicate(
            predicate,
            blocking=self.config.blocking,
            timeout_seconds=self.config.timeout_seconds,
            poll_interval_seconds=self.config.poll_interval_seconds,
            waiting_message=f"file {target} missing",
        )


@dataclass
class GlobExistsConditionConfig(ConfigInterface):
    class_name: str = "GlobExistsCondition"
    pattern: str = ""
    min_matches: int = 1
    blocking: bool = False
    timeout_seconds: float = 600.0
    poll_interval_seconds: float = 5.0


@register
class GlobExistsCondition(BaseCondition):
    config: GlobExistsConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        rendered = context.render(self.config.pattern)
        path = Path(rendered).expanduser()

        def predicate() -> bool:
            return len(list(path.parent.glob(path.name))) >= self.config.min_matches

        return _wait_for_predicate(
            predicate,
            blocking=self.config.blocking,
            timeout_seconds=self.config.timeout_seconds,
            poll_interval_seconds=self.config.poll_interval_seconds,
            waiting_message=f"glob {rendered} missing",
        )


@dataclass
class FileContentConditionConfig(ConfigInterface):
    class_name: str = "FileContentCondition"
    path: str = ""
    pattern: str = ""
    mode: Literal["contains", "regex"] = "contains"
    blocking: bool = False
    timeout_seconds: float = 600.0
    poll_interval_seconds: float = 5.0


@register
class FileContentCondition(BaseCondition):
    config: FileContentConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        rendered_path = context.render(self.config.path)
        path = Path(rendered_path).expanduser()
        
        def predicate() -> bool:
            if not path.exists():
                return False
            try:
                content = path.read_text()
                if self.config.mode == "contains":
                    return self.config.pattern in content
                elif self.config.mode == "regex":
                    return bool(re.search(self.config.pattern, content))
                return False # pragma: no cover
            except OSError: # pragma: no cover
                return False

        return _wait_for_predicate(
            predicate,
            blocking=self.config.blocking,
            timeout_seconds=self.config.timeout_seconds,
            poll_interval_seconds=self.config.poll_interval_seconds,
            waiting_message=f"file {path} content match failed",
        )


@dataclass
class CommandConditionConfig(ConfigInterface):
    class_name: str = "CommandCondition"
    command: list[str] = field(default_factory=list)


@register
class CommandCondition(BaseCondition):
    config: CommandConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        if not self.config.command:
            return ConditionResult(status="fail", message="no command supplied") # pragma: no cover
        rendered = [context.render(segment) for segment in self.config.command]
        proc = subprocess.run(rendered, capture_output=True, text=True)
        if proc.returncode == 0:
            return ConditionResult(status="pass")
        return ConditionResult(
            status="fail",
            message=f"command exited with {proc.returncode}: {proc.stderr.strip()}",
        )


@dataclass
class ShellCommandConditionConfig(ConfigInterface):
    class_name: str = "ShellCommandCondition"
    command: str = ""


@register
class ShellCommandCondition(BaseCondition):
    config: ShellCommandConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        rendered = context.render(self.config.command)
        proc = subprocess.run(rendered, shell=True, capture_output=True, text=True)
        if proc.returncode == 0:
            return ConditionResult(status="pass")
        return ConditionResult(
            status="fail",
            message=f"shell command exited with {proc.returncode}",
        )


@dataclass
class CompositeConditionConfig(ConfigInterface):
    class_name: str = "CompositeCondition"
    mode: Literal["all", "any"] = "all"
    conditions: list[MonitorConditionInterface.cfgtype] = field(default_factory=list)


@register
class CompositeCondition(BaseCondition):
    config: CompositeConditionConfig

    def __init__(self, config: CompositeConditionConfig) -> None:
        super().__init__(config)
        self._children = [condition.instantiate(MonitorConditionInterface) for condition in config.conditions]

    def check(self, context: ConditionContext) -> ConditionResult:
        results: list[ConditionResult] = [child.check(context) for child in self._children]
        if self.config.mode == "all":
            if all(result.passed for result in results):
                return ConditionResult(status="pass")
            waiting = next((r for r in results if r.waiting), None)
            if waiting:
                return waiting
            failed = next((r for r in results if not r.passed), None)
            return failed or ConditionResult(status="fail", message="unknown composite failure") # pragma: no cover

        # mode == "any"
        if any(result.passed for result in results):
            return ConditionResult(status="pass")
        waiting = next((r for r in results if r.waiting), None)
        if waiting:
            return waiting
        return ConditionResult(status="fail", message="all child conditions failed") # pragma: no cover


@dataclass
class AndConditionConfig(ConfigInterface):
    class_name: str = "AndCondition"
    conditions: list[MonitorConditionInterface.cfgtype] = field(default_factory=list)


@register
class AndCondition(CompositeCondition):
    config: AndConditionConfig

    def __init__(self, config: AndConditionConfig) -> None:
        composite_config = CompositeConditionConfig(
            mode="all",
            conditions=config.conditions,
        )
        super().__init__(composite_config)


@dataclass
class OrConditionConfig(ConfigInterface):
    class_name: str = "OrCondition"
    conditions: list[MonitorConditionInterface.cfgtype] = field(default_factory=list)


@register
class OrCondition(CompositeCondition):
    config: OrConditionConfig

    def __init__(self, config: OrConditionConfig) -> None:
        composite_config = CompositeConditionConfig(
            mode="any",
            conditions=config.conditions,
        )
        super().__init__(composite_config)


class NotConditionConfig(ConfigInterface):
    def __init__(self, condition: MonitorConditionInterface.cfgtype, class_name: str = "NotCondition"):
        self.condition = condition
        self.class_name = class_name


@register
class NotCondition(BaseCondition):
    config: NotConditionConfig

    def __init__(self, config: NotConditionConfig) -> None:
        super().__init__(config)
        self._child = config.condition.instantiate(MonitorConditionInterface)

    def check(self, context: ConditionContext) -> ConditionResult:
        result = self._child.check(context)
        if result.status == "waiting":
            return result
        if result.passed:
            return ConditionResult(status="fail", message=f"NOT condition failed: child {result.message}")
        return ConditionResult(status="pass")


@dataclass
class MetadataConditionConfig(ConfigInterface):
    class_name: str = "MetadataCondition"
    key: str = ""
    equals: Any | None = None
    within: list[Any] | None = None


@register
class MetadataCondition(BaseCondition):
    config: MetadataConditionConfig

    def check(self, context: ConditionContext) -> ConditionResult:
        if not self.config.key:
            return ConditionResult(status="fail", message="metadata key missing") # pragma: no cover
        value = context.event.metadata.get(self.config.key)
        if value is None:
            return ConditionResult(status="fail", message=f"metadata key '{self.config.key}' not present")
        if self.config.equals is not None and value != self.config.equals:
            return ConditionResult(
                status="fail",
                message=f"metadata key '{self.config.key}' != {self.config.equals!r}",
            )
        if self.config.within is not None and value not in self.config.within:
            return ConditionResult(
                status="fail",
                message=f"metadata key '{self.config.key}' not in {self.config.within!r}",
            )
        return ConditionResult(status="pass")


__all__ = [
    "MonitorConditionInterface",
    "ConditionContext",
    "ConditionResult",
    "ConditionStatus",
    "AlwaysTrueCondition",
    "MaxAttemptsCondition",
    "CooldownCondition",
    "FileExistsCondition",
    "FileContentCondition",
    "GlobExistsCondition",
    "CommandCondition",
    "ShellCommandCondition",
    "CompositeCondition",
    "AndCondition",
    "OrCondition",
    "NotCondition",
    "MetadataCondition",
]