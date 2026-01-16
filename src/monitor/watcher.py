"""SLURM log and queue monitoring."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Pattern, TYPE_CHECKING

from compoconf import (
    ConfigInterface,
    RegistrableConfigInterface,
    register,
    register_interface,
)

from monitor.event_bindings import EventActionBinding, instantiate_bindings
from monitor.utils.states import get_state_by_name

if TYPE_CHECKING:
    from monitor.states import BaseMonitorState, MonitorStateInterface


LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class MonitoredJob:
    """Read-only view of a job for the monitor to evaluate."""

    job_id: str
    name: str
    log_path: str
    check_interval_seconds: float
    state: str
    metadata: dict[str, Any] = field(default_factory=dict)
    output_paths: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class MonitorEvent:
    """Event detected by a monitor cycle."""

    job_id: str
    name: str
    state: BaseMonitorState | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    actions: list[EventActionBinding] = field(default_factory=list)


@dataclass(kw_only=True)
class MonitorOutcome:
    """Snapshot of a job's status and detected events."""

    job_id: str
    status: Literal["active", "stall", "complete", "pending"]
    last_update_seconds: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    events: list[MonitorEvent] = field(default_factory=list)


@register_interface
class MonitorConfigInterface(RegistrableConfigInterface):
    """Interface for monitor configurations."""


class BaseMonitor:
    """Base class for monitoring implementations."""

    config: ConfigInterface

    def __init__(self, config: ConfigInterface) -> None:
        self.config = config

    def watch_sync(self, jobs: list[MonitoredJob]) -> dict[str, MonitorOutcome]:  # pragma: no cover
        raise NotImplementedError


@dataclass
class NullMonitorConfig(MonitorConfigInterface):
    """Configuration for NullMonitor (no-op monitor for testing)."""

    class_name: str = "NullMonitor"
    log_path: str = ""


@register
class NullMonitor(BaseMonitor):
    """No-op monitor implementation for testing."""

    config: NullMonitorConfig

    def watch_sync(self, jobs: list[MonitoredJob]) -> dict[str, MonitorOutcome]:
        return {}


@dataclass
class LogEventConfig(ConfigInterface):
    """Configuration for a specific event triggered by log patterns."""

    class_name: str = "LogEvent"
    name: str = ""
    pattern: str = ""
    pattern_type: Literal["substring", "regex", "inactivity"] = "substring"
    state: MonitorStateInterface.cfgtype | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extract_groups: dict[str, int | str] = field(default_factory=dict)
    actions: list[EventActionBinding.cfgtype] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.state is None and not self.metadata:
            raise ValueError(f"LogEventConfig '{self.name}' must specify a state change or metadata")
        if self.pattern_type not in ("substring", "regex", "inactivity"):
            raise ValueError(f"Unsupported pattern_type: {self.pattern_type}")
        # StateEventConfig and inactivity patterns don't require a pattern
        if self.class_name == "LogEvent" and self.pattern_type in ("substring", "regex") and not self.pattern:
            raise ValueError(f"LogEventConfig '{self.name}' with pattern_type='{self.pattern_type}' requires a pattern")


@dataclass
class StateEventConfig(LogEventConfig):
    """Configuration for an event triggered by job state change."""
    class_name: str = "StateEvent"


@dataclass
class InactivityRuleConfig(ConfigInterface):
    """Configuration for inactivity (stall) detection."""

    class_name: str = "InactivityRule"
    name: str = "stall"
    threshold_seconds: float | None = None
    state: MonitorStateInterface.cfgtype | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    actions: list[EventActionBinding.cfgtype] = field(default_factory=list)


@dataclass
class SlurmLogMonitorConfig(MonitorConfigInterface):
    """Configuration for SlurmLogMonitor."""

    class_name: str = "SlurmLogMonitor"
    log_path: str = ""
    poll_interval_seconds: float = 60.0
    inactivity_threshold_seconds: float = 3600.0
    log_events: list[LogEventConfig] = field(default_factory=list)
    inactivity_rules: list[InactivityRuleConfig] = field(default_factory=list)
    state_events: list[LogEventConfig] = field(default_factory=list)
    state_whitelist: list[str] = field(default_factory=list)


@register
class SlurmLogMonitor(BaseMonitor):
    """Monitor SLURM jobs via logs and queue state."""

    config: SlurmLogMonitorConfig

    def __init__(self, config: SlurmLogMonitorConfig) -> None:
        super().__init__(config)
        self._snapshots: dict[str, _JobSnapshot] = {}
        self._compiled_events = [_CompiledLogEvent(rule, _compile_pattern(rule)) for rule in config.log_events]
        self._compiled_rules = self._compiled_events  # Alias for backwards compatibility
        self._inactivity_rules = config.inactivity_rules
        self._state_event_configs = {cfg.name: cfg for cfg in config.state_events}
        self._state_whitelist = set(config.state_whitelist)  # Convert to set for backwards compatibility

    def watch_sync(self, jobs: list[MonitoredJob]) -> dict[str, MonitorOutcome]:
        now = time.time()
        outcomes: dict[str, MonitorOutcome] = {}
        for job in jobs:
            outcomes[job.job_id] = self._evaluate_job(job, now)
        return outcomes

    def _evaluate_job(self, job: MonitoredJob, now: float) -> MonitorOutcome:
        # Check state whitelist - if job state is not in whitelist, return early
        if self._state_whitelist and job.state not in self._state_whitelist:
            return MonitorOutcome(
                job_id=job.job_id,
                status="complete",
                last_update_seconds=None,
                metadata={},
                events=[],
            )

        log_path = Path(job.log_path)
        snapshot = self._snapshots.get(job.job_id)
        events: list[MonitorEvent] = []
        metadata: dict[str, Any] = {}

        log_current = ""
        updated = False
        last_update_seconds: float | None = None

        if log_path.exists():
            try:
                log_current = log_path.read_text(encoding="utf-8", errors="replace")
                mtime = log_path.stat().st_mtime
                last_update_seconds = now - mtime
                updated = True
            except OSError:  # pragma: no cover
                pass

        if updated:
            previous_log = snapshot.log_content if snapshot else ""
            new_events = self._extract_events(job, log_current, previous_log)
            events.extend(new_events)

            if snapshot:
                snapshot.log_content = log_current
                snapshot.last_update = mtime
            else:
                snapshot = _JobSnapshot(log_content=log_current, last_update=mtime)

        status: Literal["active", "stall", "complete", "pending"] = "pending"
        if last_update_seconds is not None:
            threshold = self._effective_threshold(job)
            if threshold is not None and last_update_seconds >= threshold:
                status = "stall"
            else:
                status = "active" # pragma: no cover
        else:
            status = "pending"
            last_update_seconds = None

        inactivity_events: list[MonitorEvent] = []
        if status == "stall":
            if snapshot is None:
                snapshot = _JobSnapshot(log_content=log_current, last_update=now) # pragma: no cover
            inactivity_events = self._build_inactivity_events(
                job,
                snapshot,
                last_update_seconds,
                threshold if updated else self._effective_threshold(job),
            )
        else:
            if snapshot:
                snapshot.triggered_inactivity_events.clear()

        if snapshot:
            snapshot.last_status = status
            self._snapshots[job.job_id] = snapshot

        events.extend(inactivity_events)

        return MonitorOutcome(
            job_id=job.job_id,
            status=status,
            last_update_seconds=last_update_seconds,
            metadata=metadata,
            events=events,
        )

    def _effective_threshold(self, job: MonitoredJob) -> float | None:
        threshold = job.metadata.get("inactivity_threshold_seconds")
        if threshold is not None:
            return float(threshold)
        return self.config.inactivity_threshold_seconds

    def _extract_events(
        self,
        job: MonitoredJob,
        content: str,
        previous: str,
        source: str = "log",
    ) -> list[MonitorEvent]:
        events: list[MonitorEvent] = []
        new_text = content
        if previous and content.startswith(previous):
            new_text = content[len(previous):]

        if not new_text:
            return events

        for compiled in self._compiled_events:
            for match in compiled.pattern.finditer(new_text):
                metadata = _extract_metadata(match, compiled.rule)
                metadata["match"] = match.group(0)
                metadata["line"] = match.string[match.start() : match.end()]
                metadata["source"] = source

                event = MonitorEvent(
                    job_id=job.job_id,
                    name=compiled.rule.name,
                    state=compiled.instantiate_state(),
                    metadata={**compiled.rule.metadata, **metadata},
                    actions=instantiate_bindings(compiled.rule.actions),
                )
                events.append(event)
        return events

    def _build_inactivity_events(
        self,
        job: MonitoredJob,
        snapshot: _JobSnapshot,
        stall_duration: float | None,
        threshold: float | None,
    ) -> list[MonitorEvent]:
        if not self._inactivity_rules:
            return []
        if stall_duration is None:
            return [] # pragma: no cover
        events: list[MonitorEvent] = []
        for rule in self._inactivity_rules:
            if rule.name in snapshot.triggered_inactivity_events:
                continue # pragma: no cover
            
            rule_threshold = rule.threshold_seconds or threshold
            if rule_threshold is not None and stall_duration >= rule_threshold:
                event = self._build_state_event(job, rule.name, {"stall_duration": stall_duration})
                if event:
                    events.append(event)
                    snapshot.triggered_inactivity_events.add(rule.name)
        return events

    def _build_state_event(
        self,
        job: MonitoredJob,
        state_name: str,
        extra_metadata: dict[str, Any],
    ) -> MonitorEvent | None:
        cfg = self._state_event_configs.get(state_name)
        from monitor.states import MonitorStateInterface
        state = cfg.state.instantiate(MonitorStateInterface) if cfg and cfg.state else get_state_by_name(state_name)
        metadata = dict(cfg.metadata if cfg else {})
        metadata.setdefault("job_name", job.name)
        metadata.update(extra_metadata)
        actions = instantiate_bindings(cfg.actions) if cfg else []
        if state is None and not actions:
            return None
        return MonitorEvent(
            job_id=job.job_id,
            name=state_name,
            state=state,
            metadata=metadata,
            actions=actions,
        )


@dataclass(kw_only=True)
class _JobSnapshot:
    log_content: str
    last_update: float
    output_contents: dict[str, str] = field(default_factory=dict)
    last_status: str = "pending" # pragma: no cover
    triggered_inactivity_events: set[str] = field(default_factory=set) # pragma: no cover


@dataclass(frozen=True)
class _CompiledLogEvent:
    rule: LogEventConfig
    pattern: Pattern[str]

    def instantiate_state(self) -> BaseMonitorState | None:
        from monitor.states import MonitorStateInterface
        if self.rule.state is None:
            return None # pragma: no cover
        return self.rule.state.instantiate(MonitorStateInterface)


def _compile_pattern(rule: LogEventConfig) -> Pattern[str]:
    if rule.pattern_type == "regex":
        return re.compile(rule.pattern, flags=re.MULTILINE)
    escaped = re.escape(rule.pattern)
    return re.compile(escaped, flags=re.MULTILINE)


def _extract_metadata(match: re.Match[str], rule: LogEventConfig) -> dict[str, Any]:
    extracted: dict[str, Any] = {}
    if not rule.extract_groups:
        return extracted
    for key, group in rule.extract_groups.items():
        if isinstance(group, str) and group == "match":
            extracted[key] = match.group(0) # pragma: no cover
            continue
        try:
            extracted[key] = match.group(group)
        except (IndexError, KeyError): # pragma: no cover
            continue
    return extracted


def _fallback_state_for(name: str) -> BaseMonitorState | None:
    from monitor.states import SuccessState, SuccessStateConfig, StalledState, StalledStateConfig, TimeoutState, TimeoutStateConfig, CrashState, CrashStateConfig
    key = name.lower()
    if key == "stall":
        return StalledState(StalledStateConfig())
    if key == "timeout":
        return TimeoutState(TimeoutStateConfig())
    if key == "crash":
        return CrashState(CrashStateConfig())
    if key == "success":
        return SuccessState(SuccessStateConfig())
    return None # pragma: no cover


__all__ = [
    "SlurmLogMonitor",
    "SlurmLogMonitorConfig",
    "MonitoredJob",
    "MonitorOutcome",
    "MonitorEvent",
    "LogEventConfig",
    "StateEventConfig",
    "InactivityRuleConfig",
]
