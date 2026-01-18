"""Event action binding helpers for monitor events."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Any

from compoconf import ConfigInterface, parse_config

from monitor.actions import BaseMonitorAction
from monitor.conditions import MonitorConditionInterface


@dataclass
class EventActionBinding:
    """Runtime binding used by the controller."""

    action: BaseMonitorAction
    mode: Literal["inline", "queue"]
    conditions: list[MonitorConditionInterface]
    action_id: str


def _coerce_action_config(payload: Any) -> BaseMonitorAction.cfgtype:
    if hasattr(payload, "instantiate"):
        return payload
    if isinstance(payload, dict):
        return parse_config(BaseMonitorAction.cfgtype, payload)
    raise TypeError(f"Unsupported action config payload: {payload!r}")  # pragma: no cover


def _coerce_condition_config(payload: Any) -> MonitorConditionInterface.cfgtype:
    if hasattr(payload, "instantiate"):
        return payload
    if isinstance(payload, dict):
        return parse_config(MonitorConditionInterface.cfgtype, payload)
    raise TypeError(f"Unsupported condition config payload: {payload!r}")  # pragma: no cover

def _build_action_id(
    *,
    action_name: str,
    event_name: str,
    kind: str,
    index: int,
    explicit: str | None = None,
) -> str:
    if explicit:
        return explicit
    return f"{kind}:{event_name}:{action_name}:{index}"


@dataclass(kw_only=True)
class EventActionConfig(ConfigInterface):
    """Single event/action specification (used by concrete event configs)."""

    mode: Literal["inline", "queue"] = "inline"
    action: BaseMonitorAction.cfgtype | None = None
    conditions: list[MonitorConditionInterface.cfgtype] = field(default_factory=list)
    action_id: str | None = None
    aux: dict[str, Any] = field(default_factory=dict)


def instantiate_action_binding(
    config: EventActionConfig,
    *,
    event_name: str,
    kind: str,
    index: int,
) -> EventActionBinding:
    if config.action is None:
        raise ValueError("EventActionConfig requires 'action'")
    action_cfg = _coerce_action_config(config.action)
    action = action_cfg.instantiate(BaseMonitorAction)
    conditions = [
        _coerce_condition_config(condition).instantiate(MonitorConditionInterface)
        for condition in config.conditions
    ]
    action_id = _build_action_id(
        action_name=action.config.class_name,
        event_name=event_name,
        kind=kind,
        index=index,
        explicit=config.action_id,
    )
    return EventActionBinding(
        action=action,
        mode=config.mode,
        conditions=conditions,
        action_id=action_id,
    )


__all__ = ["EventActionConfig", "EventActionBinding", "instantiate_action_binding"]
