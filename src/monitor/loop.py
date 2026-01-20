"""Simplified synchronous monitor loop."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable
import re

from compoconf import ConfigInterface, parse_config

from monitor.conditions import ConditionContext, ConditionResult, MonitorConditionInterface
from monitor.actions import EventRecord, build_event_id, instantiate_action_binding, LogEventConfig
from monitor.job_client_protocol import JobClientProtocol
from monitor.submission import JobInterface
from monitor.utils.paths import resolve_log_path


SCHEMA_VERSION = 1


@dataclass
class JobRuntimeConfig:
    class_name: str = "JobRuntime"
    submitted: bool = False
    attempts: int = 0
    runtime_job_id: str | None = None
    start_ts: float | None = None
    log_cursor: int = 0
    condition_state: dict[str, Any] = field(default_factory=dict)
    action_state: dict[str, Any] = field(default_factory=dict)
    last_status: str | None = None


@dataclass
class JobRecordConfig:
    class_name: str = "JobRecord"
    job_id: str = ""
    definition: JobInterface.cfgtype | None = None
    runtime: JobRuntimeConfig = field(default_factory=JobRuntimeConfig)
    schema_version: int = SCHEMA_VERSION


class JobFileStore:
    """Store job records as files in a state directory."""

    def __init__(self, state_dir: str | Path) -> None:
        self.root = Path(state_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def list_paths(self) -> Iterable[Path]:
        return self.root.glob("*.job.json")

    def load_all(self) -> list[JobRecordConfig]:
        jobs: list[JobRecordConfig] = []
        for path in self.list_paths():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                _import_registry()
                jobs.append(parse_config(JobRecordConfig, payload))
            except (OSError, json.JSONDecodeError, ValueError, KeyError):
                continue
        return jobs

    def upsert(self, record: JobRecordConfig) -> None:
        path = self.path_for(record.job_id)
        payload = asdict(record)
        payload.setdefault("schema_version", SCHEMA_VERSION)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def remove(self, job_id: str) -> None:
        path = self.path_for(job_id)
        if path.exists():
            path.unlink()

    def load(self, job_id: str) -> JobRecordConfig | None:
        path = self.path_for(job_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            _import_registry()
            return parse_config(JobRecordConfig, payload)
        except (OSError, json.JSONDecodeError, ValueError, KeyError):
            return None

    def path_for(self, job_id: str) -> Path:
        return self.root / f"{job_id}.job.json"


class MonitorLoop:
    """Synchronous monitor loop that evaluates jobs and actions inline."""

    def __init__(
        self,
        store: JobFileStore,
        client: JobClientProtocol,
        *,
        poll_interval_seconds: float = 60.0,
    ) -> None:
        _import_registry()
        self._store = store
        self._client = client
        self.poll_interval_seconds = poll_interval_seconds

    def observe_once(self) -> None:
        statuses = self._client.squeue()
        for job in self._store.load_all():
            if job.definition is None:
                continue
            runtime = job.runtime
            runtime_id = runtime.runtime_job_id
            new_status = statuses.get(runtime_id) if runtime_id else None
            self._status_action(job, runtime.last_status, new_status)
            runtime.last_status = new_status
            if not runtime.submitted:
                if self._check_cancel(job):
                    self._store.remove(job.job_id)
                    continue
                if self._check_finish(job):
                    self._store.remove(job.job_id)
                    continue
                if self._check_start(job):
                    self._start_job(job)
                self._store.upsert(job)
                continue

            if self._check_cancel(job):
                if runtime_id:
                    self._client.cancel(runtime_id)
                    self._client.remove(runtime_id)
                self._store.remove(job.job_id)
                continue
            if self._check_finish(job):
                if runtime_id:
                    self._client.remove(runtime_id)
                self._store.remove(job.job_id)
                continue
            if not self._process_log_events(job):
                continue
            if runtime.last_status in {"COMPLETED", "FAILED", "CANCELLED"}:
                if runtime_id:
                    self._client.remove(runtime_id)
                self._store.remove(job.job_id)
                continue
            self._store.upsert(job)

    def _check_start(self, job: JobRecordConfig) -> bool:
        condition = job.definition.start_condition
        if condition is None:
            return True
        return self._evaluate_condition(job, condition, label="start").passed

    def _check_cancel(self, job: JobRecordConfig) -> bool:
        condition = job.definition.cancel_condition
        if condition is None:
            return False
        return self._evaluate_condition(job, condition, label="cancel").passed

    def _check_finish(self, job: JobRecordConfig) -> bool:
        condition = job.definition.finish_condition
        if condition is None:
            return False
        return self._evaluate_condition(job, condition, label="finish").passed

    def _start_job(self, job: JobRecordConfig) -> None:
        runtime = job.runtime
        runtime.attempts += 1
        runtime.start_ts = time.time()
        runtime_job_id = self._client.submit(job.definition)
        runtime.runtime_job_id = runtime_job_id
        runtime.submitted = True
        runtime.log_cursor = 0

    def _process_log_events(self, job: JobRecordConfig) -> bool:
        runtime = job.runtime
        definition = job.definition
        log_path = self._resolve_log_path(job)
        if not log_path.exists():
            return True
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(runtime.log_cursor)
                new_text = handle.read()
                runtime.log_cursor = handle.tell()
        except OSError:
            return True

        if not new_text:
            return True

        for idx, rule in enumerate(self._coerce_log_events(definition.log_events)):
            if rule.action is None:
                continue
            for match in self._iter_matches(rule, new_text):
                metadata = self._build_metadata(job, rule, match, new_text)
                event_id = build_event_id(job.job_id, rule.name, metadata)
                binding = instantiate_action_binding(rule, event_name=rule.name, kind="log", index=idx)
                action_state = runtime.action_state.get(binding.action_id, {})
                event = EventRecord(
                    event_id=event_id,
                    name=rule.name,
                    source="log",
                    payload=metadata,
                    metadata={
                        "job_id": job.job_id,
                        "job_name": definition.name,
                        "last_action_ts": float(action_state.get("last_action_ts", 0.0)),
                    },
                )
                if self._evaluate_action_conditions(job, event, binding):
                    result = binding.action.execute(self._action_context(event, job))
                    binding.action.update_event(event, result)
                    self._update_action_state(runtime, binding.action_id, result)
                    effect = self._handle_action_result(job, result)
                    if effect == "remove":
                        return False
                    if effect == "restart":
                        return True
        return True

    def _status_action(self, job: JobRecordConfig, old_status: str, new_status: str):
        for state_event in job.definition.state_events:
            state_event = StateEvent(state_event)

    def _action_context(self, event: EventRecord, job: JobRecordConfig):
        from monitor.actions import ActionContext

        return ActionContext(
            event=event,
            job_metadata=self._build_job_metadata(job),
            attempts=job.runtime.attempts,
        )

    def _evaluate_action_conditions(
        self,
        job: JobRecordConfig,
        event: EventRecord,
        binding,
    ) -> bool:
        if not binding.conditions:
            return True
        action_state = job.runtime.action_state.setdefault(binding.action_id, {})
        condition_states = action_state.setdefault("conditions", {})
        for idx, condition in enumerate(binding.conditions):
            state = condition_states.setdefault(str(idx), {})
            if "started_ts" not in state:
                state["started_ts"] = time.time()
            ctx = ConditionContext(
                event=event,
                job_metadata=self._build_job_metadata(job),
                attempts=job.runtime.attempts,
                state=state,
                started_ts=state.get("started_ts"),
            )
            result = condition.check(ctx)
            result = _apply_persistence(condition.config, state, result)
            if not result.passed:
                return False
        return True

    def _evaluate_condition(
        self,
        job: JobRecordConfig,
        condition_cfg: MonitorConditionInterface.cfgtype,
        *,
        label: str,
    ) -> ConditionResult:
        state = job.runtime.condition_state.setdefault(label, {})
        if "started_ts" not in state:
            state["started_ts"] = time.time()
        condition = condition_cfg.instantiate(MonitorConditionInterface)
        ctx = ConditionContext(
            job_metadata=self._build_job_metadata(job),
            attempts=job.runtime.attempts,
            state=state,
            started_ts=state.get("started_ts"),
        )
        result = condition.check(ctx)
        return _apply_persistence(condition_cfg, state, result)

    def _build_job_metadata(self, job: JobRecordConfig) -> dict[str, Any]:
        registration = job.definition
        metadata = dict(registration.metadata)
        metadata.setdefault("job_id", job.job_id)
        metadata.setdefault("job_name", registration.name)
        job_kind = registration.job_kind
        if not job_kind:
            job_kind = "slurm" if registration.slurm else "local"
        metadata.setdefault("job_kind", job_kind)
        return metadata

    def _resolve_log_path(self, job: JobRecordConfig) -> Path:
        definition = job.definition
        job_id = job.runtime.runtime_job_id or job.job_id
        runtime = job.runtime
        if "_" in job_id:
            array_index = int(job_id.split("_")[-1])
        else:
            array_index = 0
        if definition.log_path_current:
            log_path_cur = definition.log_path_current.replace("%a", str(array_index))
            return Path(log_path_cur)
        timestamp = int(runtime.start_ts or time.time())
        return resolve_log_path(
            definition.log_path,
            job_id=runtime.runtime_job_id or job.job_id,
            timestamp=timestamp,
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
    def _iter_matches(rule: LogEventConfig, text: str) -> Iterable[re.Match[str]]:
        if rule.pattern_type == "regex":
            pattern = re.compile(rule.pattern, flags=re.MULTILINE)
            return pattern.finditer(text)
        escaped = re.escape(rule.pattern)
        pattern = re.compile(escaped, flags=re.MULTILINE)
        return pattern.finditer(text)

    def _build_metadata(
        self,
        job: JobRecordConfig,
        rule: LogEventConfig,
        match: re.Match[str],
        text: str,
    ) -> dict[str, Any]:
        metadata = dict(rule.metadata)
        metadata["match"] = match.group(0)
        metadata["line"] = match.string[match.start() : match.end()]
        metadata.update(_extract_groups(match, rule))
        return metadata

    def _update_action_state(self, runtime: JobRuntimeConfig, action_id: str, result) -> None:
        state = runtime.action_state.setdefault(action_id, {})
        state["last_action_ts"] = time.time()
        state["last_status"] = result.status

    def _handle_action_result(self, job: JobRecordConfig, result) -> str:
        metadata = result.metadata or {}
        if "adjustments" in metadata:
            self._restart_job(job, metadata["adjustments"])
            return "restart"
        duplicate = metadata.get("duplicate_job")
        if isinstance(duplicate, dict):
            self._duplicate_job(job, duplicate)
        finalize = metadata.get("finalize")
        if finalize == "cancel":
            self._finalize_job(job, cancel=True)
            return "remove"
        if finalize == "success":
            self._finalize_job(job, cancel=False)
            return "remove"
        return "continue"

    def _restart_job(self, job: JobRecordConfig, adjustments: dict[str, Any]) -> None:
        runtime = job.runtime
        if runtime.runtime_job_id:
            self._client.cancel(runtime.runtime_job_id)
            self._client.remove(runtime.runtime_job_id)
        runtime.submitted = False
        runtime.runtime_job_id = None
        runtime.log_cursor = 0
        runtime.condition_state = {}
        runtime.action_state = {}
        runtime.start_ts = None
        self._start_job(job)

    def _finalize_job(self, job: JobRecordConfig, *, cancel: bool) -> None:
        runtime = job.runtime
        if runtime.runtime_job_id:
            if cancel:
                self._client.cancel(runtime.runtime_job_id)
            self._client.remove(runtime.runtime_job_id)
        self._store.remove(job.job_id)


def _apply_persistence(
    condition_cfg: MonitorConditionInterface.cfgtype,
    condition_state: dict[str, Any],
    result: ConditionResult,
) -> ConditionResult:
    if condition_state.get("latched_pass"):
        return ConditionResult(passed=True, message=result.message, metadata=result.metadata)
    if condition_state.get("latched_fail"):
        return ConditionResult(passed=False, message=result.message, metadata=result.metadata)
    persistent_pass = bool(getattr(condition_cfg, "persistent_pass", False))
    persistent_fail = bool(getattr(condition_cfg, "persistent_fail", False))
    if result.passed and persistent_pass:
        condition_state["latched_pass"] = True
    if (not result.passed) and persistent_fail:
        condition_state["latched_fail"] = True
    return result


def _clone_registration(registration):
    payload = asdict(registration)
    return parse_config(JobInterface.cfgtype, payload)


def _suffix_path(path: str, suffix: str) -> str:
    if not suffix:
        return path
    candidate = Path(path)
    if candidate.suffix:
        return f"{candidate.with_suffix('')}{suffix}{candidate.suffix}"
    return f"{path}{suffix}"


def _unique_job_id(store: JobFileStore, base_id: str) -> str:
    job_id = base_id
    counter = 1
    while store.path_for(job_id).exists():
        job_id = f"{base_id}-{counter}"
        counter += 1
    return job_id


def _extract_groups(match: re.Match[str], rule: LogEventConfig) -> dict[str, Any]:
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


def _import_registry() -> None:
    import monitor.actions  # noqa: F401
    import monitor.conditions  # noqa: F401
    import monitor.submission  # noqa: F401
