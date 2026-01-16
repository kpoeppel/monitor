"""Core event and result models."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from enum import Enum
from typing import Any


class EventStatus(Enum):
    """Lifecycle status of a detected event."""

    PENDING = "pending"
    PROCESSED = "processed"
    FAILED = "failed"


@dataclass(kw_only=True)
class EventRecord:
    """Persistent record of a detected event and its action history."""

    event_id: str
    name: str
    source: str
    status: EventStatus = EventStatus.PENDING
    count: int = 1
    first_seen_ts: float = field(default_factory=time.time)
    last_seen_ts: float = field(default_factory=time.time)
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)

    def touch(self, *, payload: dict[str, Any] | None = None) -> None:
        """Increment the occurrence counter and update timestamps."""
        self.count += 1
        self.last_seen_ts = time.time()
        if payload:
            self.payload.update(payload)

    def set_status(self, status: EventStatus, *, note: str | None = None) -> None:
        """Move event into a new lifecycle state and append optional note."""
        self.status = status
        self.last_seen_ts = time.time()
        if note:
            self.history.append({"ts": self.last_seen_ts, "status": status.value, "note": note}) # pragma: no cover


@dataclass(kw_only=True)
class ActionResult:
    """Outcome of an action execution."""

    status: str  # success, retry, failed
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


__all__ = ["EventStatus", "EventRecord", "ActionResult"]