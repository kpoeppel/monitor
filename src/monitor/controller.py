"""Monitoring controller bridging SLURM state and event actions."""

from __future__ import annotations

from dataclasses import dataclass, field, MISSING, asdict
import logging
from pathlib import Path
import time
from typing import Any, Literal
from collections.abc import Iterable

from monitor.action_queue import ActionQueue
from monitor.actions import ActionContext, ActionResult, BaseMonitorAction
from monitor.event_bindings import EventActionBinding, instantiate_bindings
from monitor.conditions import ConditionContext, MonitorConditionInterface
from monitor.events import EventRecord, EventStatus
from monitor.executor import Executor
from monitor.states import (
    BaseMonitorState,
    CrashState,
    CrashStateConfig,
    MonitorStateInterface,
    StalledState,
    StalledStateConfig,
    TimeoutState,
    TimeoutStateConfig,
    SuccessState,
    SuccessStateConfig,
    PendingState,
    PendingStateConfig,
)
from monitor.submission import (
    JobRegistration,
    JobRuntimeState,
    SubmissionManager,
)
from monitor.utils.states import get_state_by_name
from monitor.watcher import BaseMonitor, MonitoredJob, MonitorEvent, MonitorOutcome
from monitor.persistence import MonitorStateStore, StoredJob
from monitor.job_client_protocol import JobClientProtocol


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
        self._action_queue: ActionQueue | None = None
        if self._state_store is not None:
            self._event_records = self._state_store.load_events()
            for record in self._event_records.values():
                job_id = record.metadata.get("job_id")
                if job_id:
                    key = self._event_key(str(job_id), record.name, record.metadata)
                    self._event_index[key] = record.event_id
            queue_path = self._state_store.session_path.with_suffix(".actions")
            self._action_queue = ActionQueue(queue_path)
        state_event_cfgs = getattr(self._monitor.config, "state_events", None) or []
        self._state_event_configs = {cfg.name: cfg for cfg in state_event_cfgs}

    def register_job(
        self,
        job_id: str,
        registration: JobRegistration,
        state: BaseMonitorState = PendingState(PendingStateConfig()),
        attempts: int = 1,
    ) -> None:
        self._submission_manager.register_job(job_id, registration, attempts=attempts, state=state)

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
                log_path=str(self._submission_manager._expand_log_path(state.job_id, state.registration.log_path)),
                check_interval_seconds=interval,
                state=state.state.key,
                metadata=self._build_job_metadata(state),
                output_paths=[
                    str(self._submission_manager._expand_log_path(state.job_id, path))
                    for path in state.registration.output_paths
                ],
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

            if outcome:
                LOGGER.debug(
                    f"[job {state.job_id}] monitor outcome: status={outcome.status}, "
                    f"last_update={outcome.last_update_seconds}s, events={len(outcome.events)}"
                )  # pragma: no cover

            LOGGER.debug(
                f"[job {state.job_id}] SLURM state: {slurm_state or 'NOT_FOUND'} "
                f"(previous: {state.last_slurm_state or 'NONE'})"
            )  # pragma: no cover

            transition_records = self._capture_slurm_transitions(state, slurm_state)
            if transition_records:
                cycle_result.events.extend(transition_records)

            if state.registration.cancel_condition:
                condition = state.registration.cancel_condition.instantiate(MonitorConditionInterface)
                context = ConditionContext(
                    job_metadata=self._build_job_metadata(state),
                    attempts=state.attempts,
                    state=state.condition_data,
                )
                result = condition.check(context)
                if result.passed:
                    LOGGER.info(
                        f"[job {state.job_id}] cancel condition met while running: {result.message}"
                    )  # pragma: no cover
                    self._executor.stop_job(state)
                    synthetic_event = self._build_state_event(
                        state,
                        "crash",
                        {"reason": "cancel_condition_met", "message": result.message, "error_type": "cancelled"},
                        default_state=get_state_by_name("crash"),
                    )
                    self._handle_monitor_event(state, synthetic_event, cycle_result)
                    continue

            if state.registration.finish_condition:
                condition = state.registration.finish_condition.instantiate(MonitorConditionInterface)
                context = ConditionContext(
                    job_metadata=self._build_job_metadata(state),
                    attempts=state.attempts,
                    state=state.condition_data,
                )
                result = condition.check(context)
                if result.passed:
                    LOGGER.info(f"[job {state.job_id}] finish condition met: {result.message}")  # pragma: no cover
                    synthetic_event = self._build_state_event(
                        state,
                        "success",
                        {"reason": "finish_condition_met", "message": result.message},
                        default_state=get_state_by_name("success"),
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

            classification = self._classify_mode(state, outcome, slurm_snapshot)
            if classification is None:
                continue  # pragma: no cover

            mode, mode_metadata = classification
            LOGGER.info(
                f"[job {state.job_id}] classified as mode '{mode}' "
                f"(monitor_status={outcome.status if outcome else 'NONE'}, slurm_state={slurm_state})"
            )  # pragma: no cover
            combined_metadata = dict(outcome.metadata if outcome else {})
            combined_metadata.update(mode_metadata)
            synthetic_event = self._build_state_event(
                state,
                mode,
                combined_metadata,
                default_state=get_state_by_name(mode),
            )
            self._handle_monitor_event(state, synthetic_event, cycle_result)

        return cycle_result

    def _process_pending_submissions(self) -> None:
        for state in list(self.jobs()):
            if state.submitted:
                continue

            if state.registration.cancel_condition:
                condition = state.registration.cancel_condition.instantiate(MonitorConditionInterface)
                context = ConditionContext(
                    job_metadata=self._build_job_metadata(state),
                    attempts=state.attempts,
                    state=state.condition_data,
                )
                result = condition.check(context)
                if result.passed:
                    LOGGER.info(
                        f"[job {state.name}] cancel condition met: {result.message}. Cancelling before submission."
                    )  # pragma: no cover
                    self._submission_manager.remove_job(state.job_id)
                    continue

            if state.registration.finish_condition:
                condition = state.registration.finish_condition.instantiate(MonitorConditionInterface)
                context = ConditionContext(
                    job_metadata=self._build_job_metadata(state),
                    attempts=state.attempts,
                    state=state.condition_data,
                )
                result = condition.check(context)
                if result.passed:
                    LOGGER.info(
                        f"[job {state.name}] finish condition met: {result.message}. Skipping submission (already done)."
                    )  # pragma: no cover
                    self._submission_manager.remove_job(state.job_id)
                    continue

            if state.registration.start_condition:
                condition = state.registration.start_condition.instantiate(MonitorConditionInterface)
                context = ConditionContext(
                    job_metadata=self._build_job_metadata(state),
                    attempts=state.attempts,
                    state=state.condition_data,
                )
                result = condition.check(context)
                if not result.passed:
                    LOGGER.debug(f"[job {state.name}] start condition not met: {result.message}")  # pragma: no cover
                    continue
                else:
                    LOGGER.info(f"[job {state.name}] start condition PASSED")  # pragma: no cover

            LOGGER.info(f"[job {state.name}] Starting job...")  # pragma: no cover
            self._executor.start_job(state)

    async def observe_once(self) -> MonitorCycleResult:
        return self.observe_once_sync()  # pragma: no cover

    def handle_state_change(self, job_id: str, mode: str) -> MonitorDecision:
        state = self._submission_manager.get_job(job_id)
        if state is None:
            return MonitorDecision(action="noop", reason=f"job '{job_id}' not found")
        event = self._build_state_event(
            state,
            mode,
            metadata={},
            default_state=get_state_by_name(mode),
        )
        cycle = MonitorCycleResult()
        result = self._handle_monitor_event(state, event, cycle)
        if result:
            decision, _ = result
            return decision
        return MonitorDecision(action="noop", reason=f"no action for mode '{mode}'")  # pragma: no cover

    def snapshot(self) -> dict[str, str]:
        return self._slurm.squeue()  # pragma: no cover

    def drain_events(self) -> list[MonitorRecord]:
        records = self._pending_records
        self._pending_records = []
        return records  # pragma: no cover

    def clear_state(self) -> None:
        if self._state_store:
            self._state_store.clear()  # pragma: no cover

    def _persist_job(self, state: JobRuntimeState) -> None:
        if not self._state_store:
            return  # pragma: no cover
        resolved_log_path = self._expand_log_path(state.job_id, state.registration.log_path)
        monitor_state = state.state.key if state.state else None
        stored = StoredJob.from_registration(
            state.job_id,
            state.attempts,
            state.registration,
            resolved_log_path=str(resolved_log_path),
            monitor_state=monitor_state,
            slurm_state=state.last_slurm_state,
        )
        self._state_store.upsert_job(stored)

    def _set_state(self, state: JobRuntimeState, key: str) -> None:
        new_state = get_state_by_name(key)
        if new_state:
            state.state = new_state
        self._submission_manager.update_job(state)  # pragma: no cover

    def _capture_slurm_transitions(
        self,
        state: JobRuntimeState,
        current_state: str | None,
    ) -> list[MonitorRecord]:
        previous = state.last_slurm_state
        records: list[MonitorRecord] = []
        LOGGER.debug(f"DEBUG: capture_transitions job={state.job_id} prev={previous} curr={current_state}")
        if current_state != previous:
            LOGGER.info(
                (f"[job {state.job_id}] SLURM state transition: {previous or 'NONE'} ")
                + (f"-> {current_state or 'NOT_FOUND'}")
            )  # pragma: no cover
            metadata = {
                "slurm_state": current_state or "NOT_FOUND",
                "previous_state": previous or "NONE",
            }
            if current_state == "RUNNING":
                self._set_state(state, "running")
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key,
                    metadata=metadata,
                    action_name="run_started",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
            if current_state in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}:
                self._set_state(state, current_state.lower())
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key,
                    metadata=metadata,
                    action_name="run_ended",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
            if current_state is None and previous is not None:
                self._set_state(state, "timeout")
                record = self._queue_event(
                    job_id=state.job_id,
                    job_name=state.name,
                    event_name="slurm_state_transition",
                    state_key=state.state.key,
                    metadata=metadata,
                    action_name="run_ended",
                    payload={"type": "slurm", **metadata},
                )
                if record:
                    records.append(record)
        state.last_slurm_state = current_state
        self._persist_job(state)
        return records  # pragma: no cover

    @staticmethod
    def _classify_mode(
        state: JobRuntimeState,
        outcome: MonitorOutcome | None,
        slurm_snapshot: dict[str, str],
    ) -> tuple[str, dict[str, Any]] | None:
        if outcome and outcome.status == "complete":
            return "success", {"reason": "termination_condition_met"}

        slurm_state = slurm_snapshot.get(state.job_id)
        if outcome and outcome.status == "stall":
            return "stall", {"reason": "inactivity_timeout"}
        if slurm_state is None:
            return "timeout", {"reason": "job_not_in_queue", "error_type": "timeout"}
        if slurm_state == "CANCELLED":
            return "crash", {
                "reason": "job_cancelled",
                "slurm_state": "CANCELLED",
                "error_type": "cancelled",
                "subsystem": "slurm",
            }
        if slurm_state == "FAILED":
            return "crash", {"reason": "job_failed", "slurm_state": "FAILED", "error_type": "slurm_failure"}
        if slurm_state == "COMPLETED":
            return "success", {"reason": "slurm_completed", "slurm_state": "COMPLETED"}
        if slurm_state == "TIMEOUT":
            return "timeout", {"reason": "slurm_timeout", "slurm_state": "TIMEOUT", "error_type": "timeout"}
        return None  # pragma: no cover

    def _build_job_metadata(self, state: JobRuntimeState) -> dict[str, Any]:
        metadata = dict(state.registration.metadata)
        metadata.setdefault("job_name", state.name)
        metadata.setdefault("job_id", state.job_id)
        output_dir = metadata.get("output_dir")
        if not output_dir:
            output_dir = str(Path(state.registration.log_path).parent)
            metadata["output_dir"] = output_dir
        if state.registration.inactivity_threshold_seconds is not None:
            metadata.setdefault("inactivity_threshold_seconds", state.registration.inactivity_threshold_seconds)
        if state.registration.output_paths:
            metadata.setdefault("output_paths", [str(path) for path in state.registration.output_paths])
        return metadata

    def _expand_log_path(self, job_id: str, log_path: str) -> Path:
        log_str = str(log_path)
        if "_" in job_id:
            base_id, array_idx = job_id.split("_")
            log_str = log_str.replace("%A", str(base_id)).replace("%a", str(array_idx))
        log_str = log_str.replace("%j", str(job_id))
        if str(log_path) != log_str:
            LOGGER.debug(f"[job {job_id}] expanded single job log path: {log_path} -> {log_str}")  # pragma: no cover
        return Path(log_str)

    def _event_key(self, job_id: str, event_name: str, metadata: dict[str, Any] | None = None) -> tuple[str, str]:
        if metadata and "checkpoint_iteration" in metadata:
            return (str(job_id), f"{event_name}:{metadata['checkpoint_iteration']}")
        return (str(job_id), event_name)

    def _get_or_create_event_record(self, state: JobRuntimeState, monitor_event: MonitorEvent) -> EventRecord:
        key = self._event_key(state.job_id, monitor_event.name, monitor_event.metadata)
        event_id = self._event_index.get(key)
        if event_id and event_id in self._event_records:
            record = self._event_records[event_id]
            record.touch(payload=dict(monitor_event.metadata))
            return record
        if monitor_event.metadata and "checkpoint_iteration" in monitor_event.metadata:
            event_id = f"{state.job_id}:{monitor_event.name}:{monitor_event.metadata['checkpoint_iteration']}:{int(time.time() * 1000)}"
        else:
            event_id = f"{state.job_id}:{monitor_event.name}:{int(time.time() * 1000)}"
        metadata = {"job_id": state.job_id, "job_name": state.name}
        metadata.update(monitor_event.metadata)
        record = EventRecord(
            event_id=event_id,
            name=monitor_event.name,
            source="monitor",
            payload=dict(monitor_event.metadata),
            metadata=metadata,
        )
        self._event_index[key] = event_id
        self._event_records[event_id] = record
        return record

    def _persist_event(self, record: EventRecord) -> None:
        if self._state_store is not None:
            self._state_store.upsert_event(record)

    def _maybe_release_event(self, job_id: str, record: EventRecord) -> None:
        if record.status in {EventStatus.PROCESSED, EventStatus.FAILED}:
            key = self._event_key(job_id, record.name, record.metadata)
            self._event_index.pop(key, None)

    def _handle_monitor_event(
        self, state: JobRuntimeState, monitor_event: MonitorEvent, cycle_result: MonitorCycleResult
    ) -> tuple[MonitorDecision, str] | None:
        event_metadata = dict(monitor_event.metadata)
        event_metadata.setdefault("event_name", monitor_event.name)
        event_metadata.setdefault("note", monitor_event.name)
        event_record = self._get_or_create_event_record(state, monitor_event)
        workspace = Path(state.registration.script_path).parent if state.registration.script_path else None
        action_outcome = self._execute_event_actions(state, monitor_event, event_record, workspace)
        self._persist_event(event_record)
        self._maybe_release_event(state.job_id, event_record)
        event_record_entry = self._queue_event(
            state.job_id,
            state.name,
            monitor_event.name,
            monitor_event.state.key if monitor_event.state else None,
            metadata=event_metadata,
            action_name="event",
            payload={"status": event_record.status.value, "count": event_record.count},
        )
        if event_record_entry:
            cycle_result.events.append(event_record_entry)
        if monitor_event.actions:
            summary_record = self._queue_event(
                state.job_id,
                state.name,
                monitor_event.name,
                monitor_event.state.key if monitor_event.state else None,
                metadata=event_metadata,
                action_name="actions",
                payload={
                    "restart": action_outcome["restart"],
                    "queued": action_outcome["queued"],
                    "results": [result.message for result in action_outcome["results"]],
                },
            )
            if summary_record:
                cycle_result.events.append(summary_record)
        if monitor_event.state:
            LOGGER.info(
                f"[job {state.job_id}] detected event '{monitor_event.name}' with state '{monitor_event.state.key}'"
            )  # pragma: no cover
            state.state = monitor_event.state
        else:
            LOGGER.info(f"[job {state.job_id}] detected event '{monitor_event.name}'")  # pragma: no cover
        decision = self._finalize_event(state, monitor_event, action_outcome)
        if decision:
            cycle_result.decisions[state.job_id] = decision
        return decision

    def _queue_event(
        self,
        job_id: str,
        job_name: str,
        event_name: str,
        state_key: str | None,
        metadata: dict[str, Any],
        *,
        action_name: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> MonitorRecord | None:
        record = MonitorRecord(
            job_id=job_id,
            job_name=job_name,
            event=event_name,
            state=state_key,
            action=action_name,
            payload=payload or {},
            metadata=dict(metadata),
        )
        self._pending_records.append(record)
        return record

    def _execute_event_actions(
        self, state: JobRuntimeState, monitor_event: MonitorEvent, event_record: EventRecord, workspace: Path | None
    ) -> dict[str, Any]:
        job_metadata = self._build_job_metadata(state)
        condition_context = ConditionContext(event=event_record, job_metadata=job_metadata, attempts=state.attempts)
        action_context = ActionContext(
            event=event_record, job_metadata=job_metadata, attempts=state.attempts, workspace=workspace
        )
        restart_requested = False
        queued_ids: list[str] = []
        inline_results: list[ActionResult] = []
        if not monitor_event.actions:
            event_record.set_status(EventStatus.PROCESSED, note="no actions configured")
            return {"restart": False, "queued": [], "results": []}
        for binding in monitor_event.actions:
            status = self._evaluate_action_conditions(binding, condition_context, event_record)
            if status == "waiting":
                continue  # pragma: no cover
            if status == "fail":
                continue  # pragma: no cover
            if binding.mode == "queue":
                if self._action_queue is None:
                    raise RuntimeError("Action queue not configured but queue mode requested")  # pragma: no cover
                config_payload = self._render_queued_action_config(binding.action, action_context)
                record = self._action_queue.enqueue(
                    binding.action.config.class_name,
                    config_payload,
                    event_id=event_record.event_id,
                    metadata={"job": job_metadata, "event": event_record.event_id},
                )
                LOGGER.info(
                    f"Queued action: {binding.action.config.class_name} (queue_id={record.queue_id}, event_id={event_record.event_id})"
                )  # pragma: no cover
                print(
                    f"[controller] Queued {binding.action.config.class_name} for event {event_record.event_id}",
                    flush=True,
                )  # pragma: no cover
                queued_ids.append(record.queue_id)
                event_record.set_status(EventStatus.PENDING, note="action queued")
                continue
            result = binding.action.execute(action_context)
            binding.action.update_event(event_record, result)
            inline_results.append(result)
            if result.status == "retry":
                restart_requested = True
            event_record.metadata["last_action_ts"] = time.time()
        return {"restart": restart_requested, "queued": queued_ids, "results": inline_results}

    def _render_queued_action_config(self, action: BaseMonitorAction, context: ActionContext) -> dict[str, Any]:
        payload = asdict(action.config)
        return self._render_action_value(payload, context)

    def _render_action_value(self, value: Any, context: ActionContext) -> Any:
        if isinstance(value, str):
            return context.render(value)
        if isinstance(value, list):
            return [self._render_action_value(item, context) for item in value]
        if isinstance(value, dict):
            return {key: self._render_action_value(item, context) for key, item in value.items()}
        return value  # pragma: no cover

    def _evaluate_action_conditions(
        self, binding: EventActionBinding, context: ConditionContext, event_record: EventRecord
    ) -> Literal["pass", "waiting", "fail"]:
        if not binding.conditions:
            return "pass"
        for condition in binding.conditions:
            result = condition.check(context)
            if result.status == "waiting":
                event_record.set_status(EventStatus.PENDING, note=result.message or "condition waiting")
                return "waiting"  # pragma: no cover
            if result.status == "fail":
                event_record.set_status(EventStatus.FAILED, note=result.message or "condition failed")
                return "fail"  # pragma: no cover
        return "pass"

    def _finalize_event(
        self, state: JobRuntimeState, monitor_event: MonitorEvent, action_outcome: dict[str, Any]
    ) -> MonitorDecision | None:
        if action_outcome.get("restart"):
            self._executor.restart_job(state)
            return MonitorDecision(
                action="restart",
                reason=f"{monitor_event.name} requested restart",
                metadata={"state": "reset_for_restart"},
            )
        state_key = monitor_event.state.key if monitor_event.state else None
        if state_key == "success":
            self._executor.finalize_job(state.job_id)
            return MonitorDecision(action="success", reason="job completed")
        if state_key in {"crash", "stall", "timeout"}:
            self._executor.finalize_job(state.job_id)
            return MonitorDecision(action="stop", reason=f"{state_key} detected")
        return None  # pragma: no cover

    def _build_state_event(
        self,
        state: JobRuntimeState,
        event_name: str,
        metadata: dict[str, Any],
        default_state: BaseMonitorState | None = None,
    ) -> MonitorEvent:
        cfg = self._state_event_configs.get(event_name)
        merged_metadata = dict(cfg.metadata if cfg else {})
        merged_metadata.setdefault("job_name", state.name)
        merged_metadata.update(metadata)
        if cfg and cfg.state is not None:
            event_state = cfg.state.instantiate(MonitorStateInterface)
        else:
            event_state = default_state or get_state_by_name(event_name)
        actions = instantiate_bindings(cfg.actions) if cfg else []
        return MonitorEvent(
            job_id=state.job_id, name=event_name, state=event_state, metadata=merged_metadata, actions=actions
        )


def _fallback_state_for(name: str) -> BaseMonitorState | None:
    key = name.lower()
    if key == "stall":
        return StalledState(StalledStateConfig())
    if key == "timeout":
        return TimeoutState(TimeoutStateConfig())
    if key == "crash":
        return CrashState(CrashStateConfig())
    if key == "success":
        return SuccessState(SuccessStateConfig())
    return None  # pragma: no cover


__all__ = [
    "MonitorController",
    "JobRuntimeState",
    "JobRegistration",
    "MonitorRecord",
    "MonitorCycleResult",
]
