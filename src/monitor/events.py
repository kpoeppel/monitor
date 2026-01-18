"""Core event and result models."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from enum import Enum
from typing import Any
import json
import hashlib


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
            self.history.append({"ts": self.last_seen_ts, "status": status.value, "note": note})  # pragma: no cover


@dataclass(kw_only=True)
class ActionResult:
    """Outcome of an action execution."""

    status: str  # success, retry, failed
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def event_key(job_id: str, event_name: str, metadata: dict[str, Any] | None = None) -> tuple[str, str]:
    h = hashlib.md5()
    h.update(json.dumps(metadata).encode("utf8"))
    h = str(h.digest())[:16]
    return (str(job_id), event_name, h)


def build_event_id(
    job_id: str,
    event_name: str,
    metadata: dict[str, Any] | None = None,
    *,
    now_ms: int | None = None,
) -> str:
    timestamp = int(time.time() * 1000) if now_ms is None else now_ms
    if metadata and "checkpoint_iteration" in metadata:
        return f"{job_id}:{event_name}:{metadata['checkpoint_iteration']}:{timestamp}"
    return f"{job_id}:{event_name}:{timestamp}"


__all__ = ["EventStatus", "EventRecord", "ActionResult", "event_key", "build_event_id"]
