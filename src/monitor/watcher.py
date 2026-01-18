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
    parse_config,
    register,
    register_interface,
)

from monitor.event_bindings import EventActionBinding, EventActionConfig, instantiate_action_binding
from monitor.states import get_state, MonitorStateInterface

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
    log_events: list[Any] = field(default_factory=list)
    inactivity_rules: list[Any] = field(default_factory=list)
    state_events: list[Any] = field(default_factory=list)
    inactivity_threshold_seconds: float | None = None


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
    class_name: str = "NullMonitor"


@register
class NullMonitor(BaseMonitor):
    """Monitor that does nothing (for testing)."""
    def watch_sync(self, jobs: list[MonitoredJob]) -> dict[str, MonitorOutcome]:
        return {job.job_id: MonitorOutcome(job_id=job.job_id, status="pending") for job in jobs}


@dataclass
class LogEventConfig(EventActionConfig):
    """Configuration for a specific event triggered by log patterns."""

    class_name: str = "LogEvent"
    name: str = ""
    pattern: str = ""
    pattern_type: Literal["substring", "regex"] = "substring"
    state: MonitorStateInterface.cfgtype | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extract_groups: dict[str, int | str] = field(default_factory=dict)


@dataclass
class StateEventConfig(LogEventConfig):
    """Configuration for an event triggered by job state change."""
    class_name: str = "StateEvent"


@dataclass
class InactivityRuleConfig(EventActionConfig):
    """Configuration for inactivity (stall) detection."""

    class_name: str = "InactivityRule"
    name: str = "stall"
    threshold_seconds: float | None = None
    state: MonitorStateInterface.cfgtype | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SlurmLogMonitorConfig(MonitorConfigInterface):
    """Configuration for SlurmLogMonitor."""

    class_name: str = "SlurmLogMonitor"
    poll_interval_seconds: float = 60.0


@register
class SlurmLogMonitor(BaseMonitor):
    """Monitor SLURM jobs via logs and queue state."""

    config: SlurmLogMonitorConfig

    def __init__(self, config: SlurmLogMonitorConfig) -> None:
        super().__init__(config)
        self._snapshots: dict[str, _JobSnapshot] = {}

    def watch_sync(self, jobs: list[MonitoredJob]) -> dict[str, MonitorOutcome]:
        now = time.time()
        outcomes: dict[str, MonitorOutcome] = {}
        for job in jobs:
            outcomes[job.job_id] = self._evaluate_job(job, now)
        return outcomes

    def _evaluate_job(self, job: MonitoredJob, now: float) -> MonitorOutcome:
        log_path = Path(job.log_path)
        snapshot = self._snapshots.get(job.job_id)
        events: list[MonitorEvent] = []
        metadata: dict[str, Any] = {}

        log_events = self._coerce_log_events(job.log_events)
        compiled_events = [
            _CompiledLogEvent(
                rule=rule,
                pattern=_compile_pattern(rule),
                binding=_maybe_instantiate_binding(rule, index=idx, kind="log"),
            )
            for idx, rule in enumerate(log_events)
        ]
        inactivity_rules = self._coerce_inactivity_rules(
            job.inactivity_rules
        )
        state_event_configs = {
            cfg.name: cfg
            for cfg in self._coerce_state_events(job.state_events)
        }

        log_current = ""
        last_update_seconds: float | None = None

        if log_path.exists():
            try:
                log_current = log_path.read_text(encoding="utf-8", errors="replace")
                mtime = log_path.stat().st_mtime
                last_update_seconds = now - mtime
                
                previous_log = snapshot.log_content if snapshot else ""
                new_events = self._extract_events(
                    job.job_id,
                    log_current,
                    previous_log,
                    compiled_events,
                )
                events.extend(new_events)

                if snapshot:
                    snapshot.log_content = log_current
                    snapshot.last_update = mtime
                else:
                    snapshot = _JobSnapshot(log_content=log_current, last_update=mtime)
            except OSError:  # pragma: no cover
                pass

        status: Literal["active", "stall", "complete", "pending"] = "pending"
        threshold = self._effective_threshold(job)
        if last_update_seconds is not None:
            if threshold is not None and last_update_seconds >= threshold:
                status = "stall"
            else:
                status = "active"
        else:
            status = "pending"

        if snapshot is None:
             snapshot = _JobSnapshot(log_content=log_current, last_update=now)

        inactivity_events: list[MonitorEvent] = []
        if last_update_seconds is not None:
            inactivity_events = self._build_inactivity_events(
                job,
                snapshot,
                last_update_seconds,
                threshold,
                inactivity_rules,
                state_event_configs,
            )

        if status != "stall":
            snapshot.triggered_inactivity_events.clear()

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
        if job.inactivity_threshold_seconds is not None:
            return float(job.inactivity_threshold_seconds)
        return None

    def _extract_events(
        self,
        job_id: str,
        content: str,
        previous: str,
        compiled_events: list[_CompiledLogEvent],
        source: str = "log",
    ) -> list[MonitorEvent]:
        events: list[MonitorEvent] = []
        new_text = content
        if previous and content.startswith(previous):
            new_text = content[len(previous) :]

        if not new_text:
            return events

        for compiled in compiled_events:
            for match in compiled.pattern.finditer(new_text):
                metadata = _extract_metadata(match, compiled.rule)
                metadata["match"] = match.group(0)
                metadata["line"] = match.string[match.start() : match.end()]
                
                event = MonitorEvent(
                    job_id=job_id,
                    name=compiled.rule.name,
                    state=compiled.instantiate_state(),
                    metadata={**compiled.rule.metadata, **metadata},
                    actions=[compiled.binding] if compiled.binding else [],
                )
                events.append(event)
        return events

    def _build_inactivity_events(
        self,
        job: MonitoredJob,
        snapshot: _JobSnapshot,
        stall_duration: float,
        threshold: float | None,
        inactivity_rules: list[InactivityRuleConfig],
        state_event_configs: dict[str, StateEventConfig],
    ) -> list[MonitorEvent]:
        if not inactivity_rules:
            return []
        events: list[MonitorEvent] = []
        for rule in inactivity_rules:
            if rule.name in snapshot.triggered_inactivity_events:
                continue
            
            rule_threshold = rule.threshold_seconds if rule.threshold_seconds is not None else threshold
            if rule_threshold is not None and stall_duration >= rule_threshold:
                fallback = state_event_configs.get(rule.name)
                state_cfg = rule if rule.state is not None else (fallback or rule)
                action_cfg = rule if rule.action is not None else (fallback or rule)
                metadata = dict(fallback.metadata) if fallback else {}
                metadata.update(rule.metadata)
                event = self._build_event_from_config(
                    job_id=job.job_id,
                    name=rule.name,
                    extra_metadata={"stall_duration": stall_duration},
                    job=job,
                    cfg=rule,
                    fallback_state=state_cfg,
                    fallback_action=action_cfg,
                    metadata_override=metadata,
                    kind="inactivity",
                )
                if event:
                    events.append(event)
                    snapshot.triggered_inactivity_events.add(rule.name)
        return events

    def _build_state_event(
        self,
        job_id: str | MonitoredJob,
        name: str,
        extra_metadata: dict[str, Any],
        job: MonitoredJob | None = None,
        state_event_configs: dict[str, StateEventConfig] | None = None,
    ) -> MonitorEvent | None:
        if isinstance(job_id, MonitoredJob):
             job = job_id
             job_id = job.job_id

        cfg = (state_event_configs or {}).get(name)
        if cfg is None:
            return None
        return self._build_event_from_config(
            job_id=str(job_id),
            name=name,
            extra_metadata=extra_metadata,
            job=job,
            cfg=cfg,
            fallback_state=cfg,
            fallback_action=cfg,
            metadata_override=dict(cfg.metadata),
            kind="state",
        )

    def _build_event_from_config(
        self,
        *,
        job_id: str,
        name: str,
        extra_metadata: dict[str, Any],
        job: MonitoredJob | None,
        cfg: EventActionConfig,
        fallback_state: EventActionConfig,
        fallback_action: EventActionConfig,
        metadata_override: dict[str, Any],
        kind: str,
    ) -> MonitorEvent | None:
        from monitor.states import MonitorStateInterface
        state_source = fallback_state
        action_source = fallback_action
        state = (
            state_source.state.instantiate(MonitorStateInterface)
            if getattr(state_source, "state", None)
            else get_state(name)
        )
        metadata = dict(metadata_override)
        if job:
            metadata.setdefault("job_name", job.name)
        metadata.update(extra_metadata)
        binding = _maybe_instantiate_binding(action_source, index=0, kind=kind)
        return MonitorEvent(
            job_id=str(job_id),
            name=name,
            state=state,
            metadata=metadata,
            actions=[binding] if binding else [],
        )

    @staticmethod
    def _coerce_log_events(events: list[Any]) -> list[LogEventConfig]:
        coerced: list[LogEventConfig] = []
        for item in events:
            if isinstance(item, LogEventConfig):
                coerced.append(item)
            elif isinstance(item, dict):
                coerced.append(parse_config(LogEventConfig, item))
        return coerced

    @staticmethod
    def _coerce_state_events(events: list[Any]) -> list[StateEventConfig]:
        coerced: list[StateEventConfig] = []
        for item in events:
            if isinstance(item, StateEventConfig):
                coerced.append(item)
            elif isinstance(item, dict):
                coerced.append(parse_config(StateEventConfig, item))
        return coerced

    @staticmethod
    def _coerce_inactivity_rules(events: list[Any]) -> list[InactivityRuleConfig]:
        coerced: list[InactivityRuleConfig] = []
        for item in events:
            if isinstance(item, InactivityRuleConfig):
                coerced.append(item)
            elif isinstance(item, dict):
                coerced.append(parse_config(InactivityRuleConfig, item))
        return coerced


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
    binding: EventActionBinding | None

    def instantiate_state(self) -> BaseMonitorState | None:
        from monitor.states import MonitorStateInterface
        if self.rule.state is None:
            return None
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
            extracted[key] = match.group(0)
            continue
        try:
            extracted[key] = match.group(group)
        except (IndexError, KeyError):
            continue
    return extracted


def _maybe_instantiate_binding(
    rule: EventActionConfig | None,
    *,
    index: int,
    kind: str,
) -> EventActionBinding | None:
    if rule is None or rule.action is None:
        return None
    return instantiate_action_binding(
        rule,
        event_name=getattr(rule, "name", kind),
        kind=kind,
        index=index,
    )


__all__ = [
    "SlurmLogMonitor",
    "SlurmLogMonitorConfig",
    "NullMonitor",
    "NullMonitorConfig",
    "MonitoredJob",
    "MonitorOutcome",
    "MonitorEvent",
    "LogEventConfig",
    "StateEventConfig",
    "InactivityRuleConfig",
]
