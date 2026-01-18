"""Persistent storage for monitor state with atomic writes."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from compoconf import parse_config

from monitor.events import EventRecord, EventStatus
from monitor.submission import JobRegistration, JobRuntimeState
from monitor.states import get_state

SCHEMA_VERSION = 2


@dataclass(kw_only=True)
class StoredJob:
    """Serialized representation of a monitored job."""

    job_id: str
    name: str
    command: list[str] = field(default_factory=list)
    log_path: str
    log_path_current: str | None = None
    extra_args: list[str] = field(default_factory=list)
    resolved_log_path: str | None = None
    log_to_file: bool = True
    attempts: int = 1
    submitted: bool = False
    monitor_state: str | None = None
    slurm_state: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    slurm: dict[str, Any] | None = None
    job_kind: str | None = None
    inactivity_threshold_seconds: float | None = None
    output_paths: list[str] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION
    condition_data: dict[str, Any] = field(default_factory=dict)

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
        condition_data: dict[str, Any] | None = None,
    ) -> StoredJob:
        return cls(
            job_id=job_id,
            name=reg.name,
            command=list(reg.command),
            log_path=reg.log_path,
            log_path_current=reg.log_path_current,
            extra_args=reg.extra_args,
            log_to_file=reg.log_to_file,
            resolved_log_path=resolved_log_path,
            attempts=attempts,
            metadata=reg.metadata,
            slurm=reg.slurm,
            job_kind=reg.job_kind,
            inactivity_threshold_seconds=reg.inactivity_threshold_seconds,
            output_paths=reg.output_paths,
            monitor_state=monitor_state,
            slurm_state=slurm_state,
            schema_version=SCHEMA_VERSION,
            condition_data=dict(condition_data or {}),
        )

    def to_runtime_state(self) -> JobRuntimeState:
        reg = JobRegistration(
            name=self.name,
            command=list(self.command),
            log_path=self.log_path,
            log_path_current=self.log_path_current,
            metadata=self.metadata,
            slurm=self.slurm,
            inactivity_threshold_seconds=self.inactivity_threshold_seconds,
            output_paths=self.output_paths,
            extra_args=self.extra_args,
            log_to_file=self.log_to_file,
            job_kind=self.job_kind,
        )
        state = get_state(self.monitor_state) if self.monitor_state else None
        return JobRuntimeState(
            job_id=self.job_id,
            registration=reg,
            attempts=self.attempts,
            submitted=self.submitted,
            state=state,
            last_slurm_state=self.slurm_state,
            condition_data=dict(self.condition_data),
        )


class MonitorStateStore:
    """Disk-based persistence for monitor state with atomic writes."""

    def __init__(self, directory: str | Path, session_id: str | None = None) -> None:
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)
        self.jobs_dir = self.root / "jobs"
        self.events_dir = self.root / "events"
        self.jobs_dir.mkdir(exist_ok=True)
        self.events_dir.mkdir(exist_ok=True)
        self.session_path = self.root / "session.json"
        self.config_path = self.root / "config.json"
        
    def _atomic_write(self, path: Path, data: Any) -> None:
        """Write data to a path atomically using a temporary file."""
        content = json.dumps(data, indent=2, default=_serialize_for_json)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=f"{path.name}.tmp")
        try:
            with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
                f.write(content)
            os.replace(tmp_path, path)
        except Exception: # pragma: no cover
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def upsert_job(self, job: StoredJob) -> None:
        path = self.jobs_dir / f"{job.job_id}.json"
        existing_events = {}
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                existing_events = dict(payload.get("events", {}))
            except (OSError, json.JSONDecodeError):
                existing_events = {}
        data = asdict(job)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "job": data,
            "events": existing_events,
        }
        self._atomic_write(path, payload)

    def remove_job(self, job_id: str) -> None:
        path = self.jobs_dir / f"{job_id}.json"
        if path.exists():
            path.unlink()

    def load_jobs(self) -> list[StoredJob]:
        jobs = []
        for path in self.jobs_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                payload = data.get("job", data)
                if "expanded_log_path" in payload and "resolved_log_path" not in payload:
                    payload["resolved_log_path"] = payload.pop("expanded_log_path")
                payload.setdefault("schema_version", SCHEMA_VERSION)
                payload.setdefault("condition_data", {})
                payload.setdefault("extra_args", [])
                payload.setdefault("log_to_file", True)
                if "log_path_latest" in payload and "log_path_current" not in payload:
                    payload["log_path_current"] = payload.pop("log_path_latest")
                payload.setdefault("log_path_current", None)
                payload.setdefault("slurm", None)
                payload.setdefault("job_kind", None)
                if "script_path" in payload and "command" not in payload:
                    payload["command"] = [payload.pop("script_path")]
                payload.setdefault("command", [])
                jobs.append(parse_config(StoredJob, payload))
            except (json.JSONDecodeError, KeyError, ValueError):  # pragma: no cover
                continue
        return jobs

    def upsert_event(self, record: EventRecord) -> None:
        job_id = record.metadata.get("job_id")
        data = {
            "schema_version": SCHEMA_VERSION,
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
        if not job_id:
            path = self.events_dir / f"{record.event_id.replace(':', '_')}.json"
            self._atomic_write(path, data)
            return
        path = self.jobs_dir / f"{job_id}.json"
        if not path.exists():
            path = self.events_dir / f"{record.event_id.replace(':', '_')}.json"
            self._atomic_write(path, data)
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {"schema_version": SCHEMA_VERSION, "job": {}, "events": {}}
        payload.setdefault("schema_version", SCHEMA_VERSION)
        payload.setdefault("job", {})
        events = payload.setdefault("events", {})
        events[record.event_id] = data
        self._atomic_write(path, payload)

    def save_config(self, payload: dict[str, Any]) -> None:
        self._atomic_write(self.config_path, payload)

    def load_config(self) -> dict[str, Any] | None:
        if not self.config_path.exists():
            return None
        try:
            return json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):  # pragma: no cover
            return None

    def load_events(self) -> dict[str, EventRecord]:
        events = {}
        for path in self.jobs_dir.glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                for data in payload.get("events", {}).values():
                    data.setdefault("schema_version", SCHEMA_VERSION)
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
            except (json.JSONDecodeError, KeyError, ValueError):  # pragma: no cover
                continue
        for path in self.events_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                data.setdefault("schema_version", SCHEMA_VERSION)
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
                events.setdefault(record.event_id, record)
            except (json.JSONDecodeError, KeyError, ValueError):  # pragma: no cover
                continue
        return events

    def clear(self) -> None:
        for path in self.jobs_dir.glob("*.json"):
            path.unlink()
        for path in self.events_dir.glob("*.json"):
            path.unlink()


def _serialize_for_json(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, set):
        return list(obj)
    try:
        return str(obj)
    except Exception:
        return "<unserializable>"
