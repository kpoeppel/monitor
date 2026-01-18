"""Monitoring controller bridging SLURM state and event actions."""

from __future__ import annotations

from dataclasses import dataclass, field, MISSING
import logging
from pathlib import Path
from typing import Any, Literal
from collections.abc import Iterable

from monitor.action_queue import ActionQueue
from monitor.condition_evaluator import ConditionEvaluator
from monitor.dispatcher import ActionDispatcher
from monitor.event_bindings import instantiate_action_binding
from monitor.conditions import MonitorConditionInterface
from monitor.events import EventRecord, EventStatus, build_event_id, event_key
from monitor.executor import Executor
from monitor.transition import TransitionManager
from monitor.states import (
    BaseMonitorState,
    MonitorStateInterface,
    get_state,
)
from monitor.submission import (
    JobRegistration,
    JobRuntimeState,
    SubmissionManager,
)
from monitor.watcher import BaseMonitor, MonitoredJob, MonitorEvent
from monitor.persistence import MonitorStateStore, StoredJob
from monitor.job_client_protocol import JobClientProtocol
from monitor.utils.paths import expand_log_path


LOGGER = logging.getLogger(__name__)


@dataclass(kw_only=True)
class MonitorRecord:
    """Recorded monitor event and optional action payload."""

    job_id: str = field(default_factory=MISSING)
    job_name: str = field(default_factory=MISSING)
    event: str = field(default_factory=MISSING)
    state: str | None = None
    action: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


ActionLiteral = Literal["restart", "stop", "success", "noop"]


@dataclass(kw_only=True)
class MonitorDecision:
    """Controller-level decision recorded for a job."""

    action: ActionLiteral = field(default_factory=MISSING)
    reason: str = field(default_factory=MISSING)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class MonitorCycleResult:
    """Aggregated outcome of a single monitoring iteration."""

    decisions: dict[str, MonitorDecision] = field(default_factory=dict)
    events: list[MonitorRecord] = field(default_factory=list)


class MonitorController:
    """Coordinate monitors, SLURM state, and event-driven actions."""

    def __init__(
        self,
        monitor: BaseMonitor,
        slurm: JobClientProtocol,
        state_store: MonitorStateStore | None = None,
    ) -> None:
        self._monitor = monitor
        self._slurm = slurm
        self._submission_manager = SubmissionManager(state_store)
        self._executor = Executor(self._submission_manager, slurm, monitor)
        self._pending_records: list[MonitorRecord] = []
        self._state_store = state_store
        self._event_records: dict[str, EventRecord] = {}
        self._event_index: dict[tuple[str, str], str] = {}
        self._action_queue = None
        if self._state_store is not None:
            self._event_records = self._state_store.load_events()
            for record in self._event_records.values():
                job_id = record.metadata.get("job_id")
                if job_id:
                    key = event_key(str(job_id), record.name, record.metadata)
                    self._event_index[key] = record.event_id
            queue_path = self._state_store.session_path.with_suffix(".actions")
            self._action_queue = ActionQueue(queue_path)
            self._action_queue.recover_running()
        self._condition_evaluator = ConditionEvaluator(self._build_job_metadata)
        self._transition_manager = TransitionManager(
            set_state=self._set_state,
            queue_event=self._queue_event,
            persist_job=self._persist_job,
        )
        self._action_dispatcher = ActionDispatcher(
            action_queue=self._action_queue,
            build_job_metadata=self._build_job_metadata,
        )

    def register_job(
        self,
        job_id: str,
        registration: JobRegistration,
        attempts: int = 1,
        state: BaseMonitorState | None = None,
    ) -> None:
        self._submission_manager.register_job(job_id, registration, attempts, state)

    def jobs(self) -> Iterable[JobRuntimeState]:
        return self._submission_manager.jobs()

    def observe_once_sync(self) -> MonitorCycleResult:
        self._process_pending_submissions()

        interval = getattr(
            self._monitor.config,
            "check_interval_seconds",
            getattr(self._monitor.config, "poll_interval_seconds", 60),
        )
        monitored_jobs = [
            MonitoredJob(
                job_id=str(state.job_id),
                name=state.name,
                log_path=str(
                    state.registration.log_path_current
                    or expand_log_path(state.registration.log_path, state.job_id)
                ),
                check_interval_seconds=interval,
                state=state.state.key if state.state else "undefined",
                metadata=self._build_job_metadata(state),
                output_paths=[
                    str(expand_log_path(path, state.job_id))
                    for path in state.registration.output_paths
                ],
                log_events=list(state.registration.log_events),
                inactivity_rules=list(state.registration.inactivity_rules),
                state_events=list(state.registration.state_events),
                inactivity_threshold_seconds=state.registration.inactivity_threshold_seconds,
            )
            for state in self.jobs()
            if state.submitted
        ]
        outcomes = self._monitor.watch_sync(monitored_jobs)
        slurm_snapshot = self._slurm.squeue()

        cycle_result = MonitorCycleResult()
        for state in list(self.jobs()):
            if not state.submitted:
                continue

            outcome = outcomes.get(state.job_id)
            state.last_outcome = outcome
            slurm_state = slurm_snapshot.get(state.job_id)

            transition_records = self._transition_manager.capture_slurm_transitions(state, slurm_state)
            if transition_records:
                cycle_result.events.extend(transition_records)

            if state.registration.cancel_condition:
                result = self._condition_evaluator.evaluate(state, state.registration.cancel_condition, label="cancel")
                if result.passed:
                    self._executor.stop_job(state)
                    synthetic_event = self._build_state_event(
                        state,
                        "crash",
                        {"reason": "cancel_condition_met", "message": result.message, "error_type": "cancelled"},
                        default_state=get_state("crash"),
                    )
                    self._handle_monitor_event(state, synthetic_event, cycle_result)
                    continue

            if state.registration.finish_condition:
                result = self._condition_evaluator.evaluate(state, state.registration.finish_condition, label="finish")
                if result.passed:
                    synthetic_event = self._build_state_event(
                        state,
                        "success",
                        {"reason": "finish_condition_met", "message": result.message},
                        default_state=get_state("success"),
                    )
                    self._handle_monitor_event(state, synthetic_event, cycle_result)
                    continue

            handled = False
            if outcome is not None:
                for event in outcome.events:
                    self._handle_monitor_event(state, event, cycle_result)
                    handled = handled or bool(event.state)
            if handled:
                continue

            classification = self._transition_manager.classify_mode(state, outcome, slurm_snapshot)
            if classification is None:
                continue
            
            mode, mode_metadata = classification
            combined_metadata = dict(outcome.metadata if outcome else {})
            combined_metadata.update(mode_metadata)
            synthetic_event = self._build_state_event(
                state,
                mode,
                combined_metadata,
                default_state=get_state(mode),
            )
            self._handle_monitor_event(state, synthetic_event, cycle_result)
        
        return cycle_result

    def _process_pending_submissions(self) -> None:
        for state in list(self.jobs()):
            if state.submitted:
                continue
            
            if state.registration.cancel_condition:
                result = self._condition_evaluator.evaluate(state, state.registration.cancel_condition, label="cancel")
                if result.passed:
                    self._submission_manager.remove_job(state.job_id)
                    continue

            if state.registration.finish_condition:
                result = self._condition_evaluator.evaluate(state, state.registration.finish_condition, label="finish")
                if result.passed:
                    self._submission_manager.remove_job(state.job_id)
                    continue

            if state.registration.start_condition:
                result = self._condition_evaluator.evaluate(state, state.registration.start_condition, label="start")
                if not result.passed:
                    continue
            
            self._executor.start_job(state)

    async def observe_once(self) -> MonitorCycleResult:
        return self.observe_once_sync() # pragma: no cover

    def handle_state_change(self, job_id: str, mode: str) -> MonitorDecision:
        state = self._submission_manager.get_job(job_id)
        if state is None:
            return MonitorDecision(action="noop", reason=f"job '{job_id}' not found")
        event = self._build_state_event(
            state,
            mode,
            metadata={},
            default_state=get_state(mode),
        )
        cycle = MonitorCycleResult()
        decision = self._handle_monitor_event(state, event, cycle)
        if decision:
            return decision
        return MonitorDecision(action="noop", reason=f"no action for mode '{mode}'") # pragma: no cover

    def snapshot(self) -> dict[str, str]:
        return self._slurm.squeue() # pragma: no cover

    def drain_events(self) -> list[MonitorRecord]:
        records = self._pending_records
        self._pending_records = []
        return records # pragma: no cover

    def clear_state(self) -> None:
        if self._state_store:
            self._state_store.clear() # pragma: no cover

    def _persist_job(self, state: JobRuntimeState) -> None:
        if not self._state_store:
            return # pragma: no cover
        resolved_log_path = expand_log_path(state.registration.log_path, state.job_id)
        monitor_state = state.state.key if state.state else None
        stored = StoredJob.from_registration(
            state.job_id,
            state.attempts,
            state.registration,
            resolved_log_path=str(resolved_log_path),
            monitor_state=monitor_state,
            slurm_state=state.last_slurm_state,
            condition_data=state.condition_data,
        )
        self._state_store.upsert_job(stored)

    def _set_state(self, state: JobRuntimeState, key: str) -> None:
        new_state = get_state(key)
        if new_state:
            state.state = new_state
        self._submission_manager.update_job(state) # pragma: no cover

    def _build_job_metadata(self, state: JobRuntimeState) -> dict[str, Any]:
        metadata = dict(state.registration.metadata)
        metadata.setdefault("job_name", state.name)
        metadata.setdefault("job_id", state.job_id)
        metadata.setdefault("job_kind", state.registration.job_kind)
        output_dir = metadata.get("output_dir")
        if not output_dir:
            output_dir = str(Path(state.registration.log_path).parent)
            metadata["output_dir"] = output_dir
        if state.registration.inactivity_threshold_seconds is not None:
            metadata.setdefault("inactivity_threshold_seconds", state.registration.inactivity_threshold_seconds)
        if state.registration.output_paths:
            metadata.setdefault("output_paths", [str(path) for path in state.registration.output_paths])
        return metadata

    def _get_or_create_event_record(self, state: JobRuntimeState, monitor_event: MonitorEvent) -> EventRecord:
        key = event_key(state.job_id, monitor_event.name, monitor_event.metadata)
        event_id = self._event_index.get(key)
        if event_id and event_id in self._event_records:
            record = self._event_records[event_id]
            record.touch(payload=dict(monitor_event.metadata))
            return record
        event_id = build_event_id(state.job_id, monitor_event.name, monitor_event.metadata)
        metadata = {"job_id": state.job_id, "job_name": state.name}
        metadata.update(monitor_event.metadata)
        record = EventRecord(event_id=event_id, name=monitor_event.name, source="monitor", payload=dict(monitor_event.metadata), metadata=metadata)
        self._event_index[key] = event_id
        self._event_records[event_id] = record
        return record

    def _persist_event(self, record: EventRecord) -> None:
        if self._state_store is not None:
            self._state_store.upsert_event(record)

    def _maybe_release_event(self, job_id: str, record: EventRecord) -> None:
        if record.status in {EventStatus.PROCESSED, EventStatus.FAILED}:
            key = event_key(job_id, record.name, record.metadata)
            self._event_index.pop(key, None)

    def _handle_monitor_event(self, state: JobRuntimeState, monitor_event: MonitorEvent, cycle_result: MonitorCycleResult) -> MonitorDecision | None:
        event_metadata = dict(monitor_event.metadata)
        event_metadata.setdefault("event_name", monitor_event.name)
        event_metadata.setdefault("note", monitor_event.name)
        event_record = self._get_or_create_event_record(state, monitor_event)
        workspace = self._command_workspace(state.registration.command)
        action_outcome = self._action_dispatcher.dispatch(state, monitor_event, event_record, workspace)
        self._persist_event(event_record)
        self._maybe_release_event(state.job_id, event_record)
        event_record_entry = self._queue_event(state.job_id, state.name, monitor_event.name, monitor_event.state.key if monitor_event.state else None, metadata=event_metadata, action_name="event", payload={"status": event_record.status.value, "count": event_record.count})
        if event_record_entry:
            cycle_result.events.append(event_record_entry)
        if monitor_event.actions:
            summary_record = self._queue_event(state.job_id, state.name, monitor_event.name, monitor_event.state.key if monitor_event.state else None, metadata=event_metadata, action_name="actions", payload={"restart": action_outcome["restart"], "queued": action_outcome["queued"], "results": [result.message for result in action_outcome["results"]]})
            if summary_record:
                cycle_result.events.append(summary_record)
        if monitor_event.state:
            state.state = monitor_event.state
        decision = self._finalize_event(state, monitor_event, action_outcome)
        if decision:
            cycle_result.decisions[state.job_id] = decision
        return decision

    def _queue_event(self, job_id: str, job_name: str, event_name: str, state_key: str | None, metadata: dict[str, Any], *, action_name: str | None = None, payload: dict[str, Any] | None = None) -> MonitorRecord | None:
        record = MonitorRecord(job_id=job_id, job_name=job_name, event=event_name, state=state_key, action=action_name, payload=payload or {}, metadata=dict(metadata))
        self._pending_records.append(record)
        return record

    def _finalize_event(self, state: JobRuntimeState, monitor_event: MonitorEvent, action_outcome: dict[str, Any]) -> MonitorDecision | None:
        finalize = action_outcome.get("finalize")
        if finalize == "cancel":
            reason = action_outcome.get("finalize_reason", "cancel action requested")
            self._executor.stop_job(state)
            return MonitorDecision(action="stop", reason=reason)
        if finalize == "success":
            reason = action_outcome.get("finalize_reason", "finish action requested")
            self._executor.finalize_job(state.job_id)
            return MonitorDecision(action="success", reason=reason)
        for duplicate in action_outcome.get("duplicates", []):
            if not isinstance(duplicate, dict):
                continue
            adjustments = duplicate.get("adjustments")
            name_suffix = duplicate.get("name_suffix")
            self._executor.duplicate_job(
                state,
                adjustments=adjustments if isinstance(adjustments, dict) else None,
                name_suffix=name_suffix if isinstance(name_suffix, str) else None,
            )
        if action_outcome.get("restart"):
            adjustments = action_outcome.get("restart_adjustments")
            self._executor.restart_job(state, adjustments=adjustments if isinstance(adjustments, dict) else None)
            return MonitorDecision(action="restart", reason=f"{monitor_event.name} requested restart", metadata={"state": "reset_for_restart"})
        state_key = monitor_event.state.key if monitor_event.state else None
        if state_key == "success":
            self._executor.finalize_job(state.job_id)
            return MonitorDecision(action="success", reason="job completed")
        if state_key in {"crash", "stall", "timeout"}:
            self._executor.finalize_job(state.job_id)
            return MonitorDecision(action="stop", reason=f"{state_key} detected")
        return None # pragma: no cover

    def _build_state_event(self, state: JobRuntimeState, event_name: str, metadata: dict[str, Any], default_state: BaseMonitorState | None = None) -> MonitorEvent:
        state_event_configs = {cfg.name: cfg for cfg in state.registration.state_events}
        cfg = state_event_configs.get(event_name)
        merged_metadata = dict(cfg.metadata if cfg else {})
        merged_metadata.setdefault("job_name", state.name)
        merged_metadata.update(metadata)
        if cfg and cfg.state is not None:
            event_state = cfg.state.instantiate(MonitorStateInterface)
        else:
            event_state = default_state or get_state(event_name)
        binding = (
            instantiate_action_binding(cfg, event_name=event_name, kind="state", index=0)
            if cfg and cfg.action is not None
            else None
        )
        actions = [binding] if binding else []
        return MonitorEvent(
            job_id=state.job_id,
            name=event_name,
            state=event_state,
            metadata=merged_metadata,
            actions=actions,
        )

    @staticmethod
    def _command_workspace(command: list[str]) -> Path | None:
        if not command:
            return None
        candidate = command[0]
        if candidate in {"bash", "sh"} and len(command) > 1:
            candidate = command[1]
        return Path(candidate).parent


__all__ = [
    "MonitorController",
    "JobRuntimeState",
    "JobRegistration",
    "MonitorRecord",
    "MonitorCycleResult",
]
