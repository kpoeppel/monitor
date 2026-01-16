"""Utilities for resolving monitor states."""

from __future__ import annotations

from monitor.states import (
    BaseMonitorState,
    CrashState,
    CrashStateConfig,
    StalledState,
    StalledStateConfig,
    TimeoutState,
    TimeoutStateConfig,
    PendingState,
    PendingStateConfig,
    StartedState,
    StartedStateConfig,
    SuccessState,
    SuccessStateConfig,
)

def get_state_by_name(name: str) -> BaseMonitorState | None:
    key = (name or "").lower()
    if key in {"success", "completed"}:
        return SuccessState(SuccessStateConfig())
    if key in {"running", "started", "active"}:
        return StartedState(StartedStateConfig())
    if key in {"stall", "stalled"}:
        return StalledState(StalledStateConfig())
    if key in {"timeout"}:
        return TimeoutState(TimeoutStateConfig())
    if key in {"crash", "failed", "error", "cancelled"}:
        return CrashState(CrashStateConfig())
    if key in {"pending"}:
        return PendingState(PendingStateConfig())
    return None  # pragma: no cover
