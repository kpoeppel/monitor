"""Consolidated and modernized persistence tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from monitor.persistence.state_store import MonitorStateStore, StoredJob
from monitor.submission import JobRegistration
from monitor.events import EventRecord, EventStatus


@pytest.fixture
def store(tmp_path):
    return MonitorStateStore(tmp_path)


def test_store_initialization(tmp_path):
    store = MonitorStateStore(tmp_path)
    assert store.jobs_dir.exists()
    assert store.events_dir.exists()


def test_store_upsert_and_load_job(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    
    store.upsert_job(job)
    
    loaded_jobs = store.load_jobs()
    assert len(loaded_jobs) == 1
    assert loaded_jobs[0].job_id == "123"
    assert loaded_jobs[0].name == "job1"


def test_store_remove_job(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    store.upsert_job(job)
    
    store.remove_job("123")
    assert len(store.load_jobs()) == 0


def test_store_events_lifecycle(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    store.upsert_job(job)
    event1 = EventRecord(
        event_id="e1",
        name="event1",
        source="test",
        status=EventStatus.PENDING,
        metadata={"job_id": "123"},
    )
    store.upsert_event(event1)
    
    loaded = store.load_events()
    assert "e1" in loaded
    assert loaded["e1"].name == "event1"
    
    event1.status = EventStatus.PROCESSED
    store.upsert_event(event1)
    assert store.load_events()["e1"].status == EventStatus.PROCESSED


def test_store_clear(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    store.upsert_job(job)
    
    event = EventRecord(event_id="e1", name="event1", source="test", metadata={"job_id": "123"})
    store.upsert_event(event)
    
    store.clear()
    assert len(store.load_jobs()) == 0
    assert len(store.load_events()) == 0


def test_store_atomic_write_failure(store, monkeypatch):
    # Mock os.replace to fail
    import os
    def mock_replace(src, dst):
        raise OSError("atomic fail")
    
    monkeypatch.setattr(os, "replace", mock_replace)
    
    with pytest.raises(OSError, match="atomic fail"):
        store.upsert_job(StoredJob.from_registration("j", 1, JobRegistration(name="n", command=["s"], log_path="l")))


def test_store_compatibility_fix(store):
    # Old format with expanded_log_path
    job_id = "compat1"
    job_path = store.jobs_dir / f"{job_id}.json"
    data = {
        "job_id": job_id,
        "name": "n",
        "script_path": "s",
        "log_path": "l",
        "expanded_log_path": "expanded_l"
    }
    job_path.write_text(json.dumps(data))
    
    loaded = store.load_jobs()
    assert len(loaded) == 1
    assert loaded[0].resolved_log_path == "expanded_l"


def test_store_load_errors(store):
    # Corrupt JSON
    (store.jobs_dir / "bad.json").write_text("{corrupt")
    assert len(store.load_jobs()) == 0
    
    (store.events_dir / "bad.json").write_text("{corrupt")
    assert len(store.load_events()) == 0


def test_store_schema_version_on_job(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    reg.extra_args = ["--foo=bar"]
    job = StoredJob.from_registration("123", 1, reg, condition_data={"start": {"started_ts": 1.0}})
    store.upsert_job(job)
    payload = json.loads((store.jobs_dir / "123.json").read_text())
    assert payload["schema_version"] == 2
    assert payload["job"]["condition_data"] == {"start": {"started_ts": 1.0}}
    assert payload["job"]["extra_args"] == ["--foo=bar"]


def test_store_schema_version_on_event(store):
    reg = JobRegistration(name="job1", command=["s"], log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    store.upsert_job(job)
    event = EventRecord(event_id="e1", name="event1", source="test", metadata={"job_id": "123"})
    store.upsert_event(event)
    payload = json.loads((store.jobs_dir / "123.json").read_text())
    assert payload["schema_version"] == 2
    assert payload["events"]["e1"]["schema_version"] == 2



def test_serialize_for_json(tmp_path):
    from monitor.persistence.state_store import _serialize_for_json
    assert _serialize_for_json(tmp_path) == str(tmp_path)
    assert _serialize_for_json({"a", "b"}) == ["a", "b"] or _serialize_for_json({"a", "b"}) == ["b", "a"]
    assert _serialize_for_json(123) == "123"
    
    class Unstr:
        def __str__(self):
            raise ValueError("fail")
    assert _serialize_for_json(Unstr()) == "<unserializable>"
