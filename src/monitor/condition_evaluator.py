"""Helpers for evaluating monitor conditions."""

from __future__ import annotations

from typing import Any, Callable

from monitor.conditions import ConditionContext, ConditionResult, MonitorConditionInterface
from monitor.submission import JobRuntimeState


class ConditionEvaluator:
    """Evaluate start/cancel/finish conditions with shared context setup."""

    def __init__(
        self,
        build_job_metadata: Callable[[JobRuntimeState], dict[str, Any]],
    ) -> None:
        self._build_job_metadata = build_job_metadata

    def evaluate(
        self,
        state: JobRuntimeState,
        condition_cfg: MonitorConditionInterface.cfgtype,
        *,
        label: str | None = None,
    ):
        condition_state: dict[str, Any]
        if label:
            condition_state = state.condition_data.setdefault(label, {})
        else:
            condition_state = state.condition_data
        condition = condition_cfg.instantiate(MonitorConditionInterface)
        context = ConditionContext(
            job_metadata=self._build_job_metadata(state),
            attempts=state.attempts,
            state=condition_state,
            started_ts=condition_state.get("started_ts"),
        )
        result = condition.check(context)
        return _apply_persistence(condition_cfg, condition_state, result)


def _apply_persistence(
    condition_cfg: MonitorConditionInterface.cfgtype,
    condition_state: dict[str, Any],
    result: ConditionResult,
) -> ConditionResult:
    if condition_state.get("latched_pass"):
        return ConditionResult(passed=True, message=result.message)
    if condition_state.get("latched_fail"):
        return ConditionResult(passed=False, message=result.message)
    persistent_pass = bool(getattr(condition_cfg, "persistent_pass", False))
    persistent_fail = bool(getattr(condition_cfg, "persistent_fail", False))
    if result.passed and persistent_pass:
        condition_state["latched_pass"] = True
    if (not result.passed) and persistent_fail:
        condition_state["latched_fail"] = True
    return result


__all__ = ["ConditionEvaluator"]
