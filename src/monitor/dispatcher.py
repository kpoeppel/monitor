"""Action dispatching helpers for monitor events."""

from __future__ import annotations

from dataclasses import asdict
import time
from pathlib import Path
from typing import Any
from collections.abc import Callable

from monitor.action_queue import ActionQueue
from monitor.actions import ActionContext, ActionResult, BaseMonitorAction
from monitor.conditions import ConditionContext, ConditionResult
from monitor.event_bindings import EventActionBinding
from monitor.events import EventRecord, EventStatus
from monitor.submission import JobRuntimeState
from monitor.watcher import MonitorEvent


class ActionDispatcher:
    """Execute event actions inline or queue them for later."""

    def __init__(
        self,
        *,
        action_queue: ActionQueue | None,
        build_job_metadata: Callable[[JobRuntimeState], dict[str, Any]],
    ) -> None:
        self._action_queue = action_queue
        self._build_job_metadata = build_job_metadata

    def dispatch(
        self,
        state: JobRuntimeState,
        monitor_event: MonitorEvent,
        event_record: EventRecord,
        workspace: Path | None,
    ) -> dict[str, Any]:
        job_metadata = self._build_job_metadata(state)
        action_context = ActionContext(
            event=event_record,
            job_metadata=job_metadata,
            attempts=state.attempts,
            workspace=workspace,
        )
        restart_requested = False
        restart_adjustments: dict[str, Any] | None = None
        duplicates: list[dict[str, Any]] = []
        queued_ids: list[str] = []
        inline_results: list[ActionResult] = []
        finalize: str | None = None
        finalize_reason: str | None = None
        if not monitor_event.actions:
            event_record.set_status(EventStatus.PROCESSED, note="no actions configured")
            return {"restart": False, "queued": [], "results": []}
        for binding in monitor_event.actions:
            if not self._evaluate_action_conditions(binding, event_record, job_metadata, state.attempts):
                continue
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
                queued_ids.append(record.queue_id)
                event_record.set_status(EventStatus.PENDING, note="action queued")
                continue
            result = binding.action.execute(action_context)
            binding.action.update_event(event_record, result)
            inline_results.append(result)
            finalize_action = result.metadata.get("finalize")
            if isinstance(finalize_action, str):
                finalize = finalize_action
                finalize_reason = result.message
            if result.status == "retry":
                restart_requested = True
                adjustments = result.metadata.get("adjustments")
                if restart_adjustments is None and isinstance(adjustments, dict):
                    restart_adjustments = adjustments
            duplicate_request = result.metadata.get("duplicate_job")
            if isinstance(duplicate_request, dict):
                duplicates.append(duplicate_request)
            event_record.metadata["last_action_ts"] = time.time()
        return {
            "restart": restart_requested,
            "restart_adjustments": restart_adjustments,
            "duplicates": duplicates,
            "queued": queued_ids,
            "results": inline_results,
            "finalize": finalize,
            "finalize_reason": finalize_reason,
        }

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
        return value

    def _evaluate_action_conditions(
        self,
        binding: EventActionBinding,
        event_record: EventRecord,
        job_metadata: dict[str, Any],
        attempts: int,
    ) -> bool:
        if not binding.conditions:
            return True
        action_state = self._get_action_state(event_record, binding)
        condition_states = action_state.setdefault("conditions", {})
        for idx, condition in enumerate(binding.conditions):
            state = condition_states.setdefault(str(idx), {})
            if "started_ts" not in state:
                state["started_ts"] = time.time()
            context = ConditionContext(
                event=event_record,
                job_metadata=job_metadata,
                attempts=attempts,
                state=state,
                started_ts=state.get("started_ts"),
            )
            result = condition.check(context)
            result = self._apply_persistence(condition.config, state, result)
            if not result.passed:
                event_record.set_status(EventStatus.PENDING, note=result.message or "condition not met")
                return False
        return True

    @staticmethod
    def _get_action_state(event_record: EventRecord, binding: EventActionBinding) -> dict[str, Any]:
        action_state = event_record.metadata.setdefault("action_state", {})
        per_action = action_state.setdefault(binding.action_id, {})
        return per_action

    @staticmethod
    def _apply_persistence(
        condition_cfg: Any,
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


__all__ = ["ActionDispatcher"]
