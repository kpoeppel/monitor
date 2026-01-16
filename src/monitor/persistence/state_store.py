"""Persistent storage for monitor state."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from compoconf import parse_config

from monitor.events import EventRecord, EventStatus
from monitor.submission import JobRegistration, JobRuntimeState
from monitor.utils.states import get_state_by_name


@dataclass(kw_only=True)
class StoredJob:
    """Serialized representation of a monitored job."""

    job_id: str
    name: str
    script_path: str
    log_path: str
    resolved_log_path: str | None = None
    attempts: int = 1
    submitted: bool = False
    monitor_state: str | None = None
    slurm_state: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    inactivity_threshold_seconds: float | None = None
    output_paths: list[str] = field(default_factory=list)

    @classmethod
    def from_registration(
        cls,
        job_id: str,
        attempts: int,
        reg: JobRegistration,
        *,
        resolved_log_path: str | None = None,
        monitor_state: str | None = None,
        slurm_state: str | None = None,
    ) -> StoredJob:
        return cls(
            job_id=job_id,
            name=reg.name,
            script_path=reg.script_path,
            log_path=reg.log_path,
            resolved_log_path=resolved_log_path,
            attempts=attempts,
            metadata=reg.metadata,
            inactivity_threshold_seconds=reg.inactivity_threshold_seconds,
            output_paths=reg.output_paths,
            monitor_state=monitor_state,
            slurm_state=slurm_state,
        )

    def to_runtime_state(self) -> JobRuntimeState:
        reg = JobRegistration(
            name=self.name,
            script_path=self.script_path,
            log_path=self.log_path,
            metadata=self.metadata,
            inactivity_threshold_seconds=self.inactivity_threshold_seconds,
            output_paths=self.output_paths,
        )
        state = get_state_by_name(self.monitor_state) if self.monitor_state else None
        return JobRuntimeState(
            job_id=self.job_id,
            registration=reg,
            attempts=self.attempts,
            submitted=self.submitted,
            state=state,
            last_slurm_state=self.slurm_state,
        )


class MonitorStateStore:
    """Disk-based persistence for monitor state."""

    def __init__(self, directory: str | Path, session_id: str | None = None) -> None:
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)
        self._session_id = session_id or str(uuid.uuid4())[:8]
        self._session_path = self.root / f"{self._session_id}.json"

        # For per-file storage (backwards compatibility)
        self.jobs_dir = self.root / "jobs"
        self.events_dir = self.root / "events"
        self.jobs_dir.mkdir(exist_ok=True)
        self.events_dir.mkdir(exist_ok=True)

        # Legacy path
        self._legacy_path = self.root / "monitor" / "state.json"
        self._legacy_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def session_path(self) -> Path:
        return self._session_path

    @property
    def descriptor_path(self) -> Path:
        return self._session_path

    def _write_payload(self, payload: dict[str, Any]) -> None:
        """Write payload to session file."""
        text = json.dumps(payload, indent=2, default=_serialize_for_json)
        self._session_path.write_text(text, encoding="utf-8")
        try:
            self._legacy_path.write_text(text, encoding="utf-8")
        except OSError:
            pass  # Legacy path may be read-only

    def _load_payload(self) -> dict[str, Any]:
        """Load payload from session or legacy file."""
        for path in (self._session_path, self._legacy_path):
            if path.exists():
                try:
                    return json.loads(path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    continue
        return {}

    def upsert_job(self, job: StoredJob) -> None:
        path = self.jobs_dir / f"{job.job_id}.json"
        data = asdict(job)
        path.write_text(json.dumps(data, indent=2, default=_serialize_for_json), encoding="utf-8")

    def remove_job(self, job_id: str) -> None:
        path = self.jobs_dir / f"{job_id}.json"
        if path.exists():
            path.unlink()
        # If no jobs left, clear session
        if not list(self.jobs_dir.glob("*.json")):
            self.clear()

    def save_jobs(self, jobs: list[StoredJob]) -> None:
        """Save jobs to session file."""
        payload = self._load_payload()
        payload["jobs"] = [asdict(job) for job in jobs]
        self._write_payload(payload)

    def load_jobs(self) -> list[StoredJob]:
        jobs = []
        # Try per-file storage first
        for path in self.jobs_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if "expanded_log_path" in data and "resolved_log_path" not in data:
                    data["resolved_log_path"] = data.pop("expanded_log_path")
                jobs.append(parse_config(StoredJob, data))
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        if jobs:
            return jobs

        # Fall back to session file
        payload = self._load_payload()
        for raw in payload.get("jobs", []):
            try:
                if "expanded_log_path" in raw and "resolved_log_path" not in raw:
                    raw["resolved_log_path"] = raw.pop("expanded_log_path")
                jobs.append(parse_config(StoredJob, raw))
            except (KeyError, ValueError):
                continue
        return jobs

    def load(self) -> dict[str, StoredJob]:
        """Load jobs as a dict keyed by job_id."""
        return {job.job_id: job for job in self.load_jobs()}

    def save_events(self, events: list[EventRecord]) -> None:
        """Save events to individual files."""
        for event in events:
            self.upsert_event(event)

    def upsert_event(self, record: EventRecord) -> None:
        path = self.events_dir / f"{record.event_id.replace(':', '_')}.json"
        data = {
            "event_id": record.event_id,
            "name": record.name,
            "source": record.source,
            "status": record.status.value,
            "count": record.count,
            "first_seen_ts": record.first_seen_ts,
            "last_seen_ts": record.last_seen_ts,
            "payload": record.payload,
            "metadata": record.metadata,
            "history": record.history,
        }
        path.write_text(json.dumps(data, indent=2, default=_serialize_for_json), encoding="utf-8")

    def remove_event(self, event_id: str) -> None:
        """Remove an event by ID."""
        path = self.events_dir / f"{event_id.replace(':', '_')}.json"
        if path.exists():
            path.unlink()

    def load_events(self) -> dict[str, EventRecord]:
        events = {}
        # Try per-file storage first
        for path in self.events_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                record = EventRecord(
                    event_id=data["event_id"],
                    name=data["name"],
                    source=data["source"],
                    status=EventStatus(data["status"]),
                    count=data.get("count", 1),
                    first_seen_ts=data.get("first_seen_ts", 0),
                    last_seen_ts=data.get("last_seen_ts", 0),
                    payload=data.get("payload", {}),
                    metadata=data.get("metadata", {}),
                    history=data.get("history", []),
                )
                events[record.event_id] = record
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        if events:
            return events

        # Fall back to session file
        payload = self._load_payload()
        for raw in payload.get("events", []):
            try:
                record = EventRecord(
                    event_id=raw["event_id"],
                    name=raw["name"],
                    source=raw["source"],
                    status=EventStatus(raw.get("status", "pending")),
                    count=raw.get("count", 1),
                    first_seen_ts=raw.get("first_seen_ts", 0),
                    last_seen_ts=raw.get("last_seen_ts", 0),
                    payload=raw.get("payload", {}),
                    metadata=raw.get("metadata", {}),
                    history=raw.get("history", []),
                )
                events[record.event_id] = record
            except (KeyError, ValueError):
                continue
        return events

    def save_session(self, config: dict[str, Any], project_name: str = "") -> None:
        """Save session metadata."""
        payload = {
            "session_id": self._session_id,
            "project_name": project_name,
            "config": config,
            "timestamp": time.time(),
        }
        self._write_payload(payload)

    @staticmethod
    def load_session(path: Path) -> dict[str, Any] | None:
        """Load session data from a file."""
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    @staticmethod
    def list_sessions(directory: Path) -> list[dict[str, Any]]:
        """List all sessions in a directory, sorted by timestamp descending."""
        sessions = []
        for path in directory.glob("*.json"):
            data = MonitorStateStore.load_session(path)
            if data and "session_id" in data:
                sessions.append(data)
        sessions.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return sessions

    def clear(self) -> None:
        for path in self.jobs_dir.glob("*.json"):
            path.unlink()
        for path in self.events_dir.glob("*.json"):
            path.unlink()
        if self._session_path.exists():
            self._session_path.unlink()
        if self._legacy_path.exists():
            self._legacy_path.unlink()


def _serialize_for_json(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize_for_json(item) for item in obj]
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    try:
        return str(obj)
    except Exception:
        return None
