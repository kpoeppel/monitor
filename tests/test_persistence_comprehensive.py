import json
import pytest
import time
from pathlib import Path
from monitor.persistence.state_store import MonitorStateStore, StoredJob
from monitor.events import EventRecord, EventStatus
from monitor.submission import JobRegistration

def test_store_session_initialization(tmp_path):
    """Test session file and legacy path creation."""
    store = MonitorStateStore(str(tmp_path), session_id="test-session")
    
    assert store.session_id == "test-session"
    assert store.session_path == tmp_path / "test-session.json"
    assert store.descriptor_path == store.session_path
    
    # Legacy path should also be created
    legacy_path = tmp_path / "monitor" / "state.json"
    assert legacy_path.parent.exists()

def test_store_events_lifecycle(tmp_path):
    """Test saving, loading, upserting, and removing events."""
    store = MonitorStateStore(str(tmp_path))
    
    event1 = EventRecord(event_id="e1", name="event1", source="test", status=EventStatus.PENDING)
    event2 = EventRecord(event_id="e2", name="event2", source="test", status=EventStatus.PROCESSED)
    
    # Save events
    store.save_events([event1, event2])
    
    # Load events
    loaded = store.load_events()
    assert len(loaded) == 2
    assert "e1" in loaded
    assert "e2" in loaded
    assert loaded["e1"].status == EventStatus.PENDING
    
    # Upsert event
    event1.status = EventStatus.PROCESSED
    store.upsert_event(event1)
    
    loaded = store.load_events()
    assert loaded["e1"].status == EventStatus.PROCESSED
    
    # Remove event
    store.remove_event("e1")
    loaded = store.load_events()
    assert len(loaded) == 1
    assert "e1" not in loaded
    assert "e2" in loaded
    
    # Remove non-existent event (should be safe)
    store.remove_event("e3")
    loaded = store.load_events()
    assert len(loaded) == 1

def test_store_jobs_lifecycle(tmp_path):
    """Test upserting and removing jobs."""
    store = MonitorStateStore(str(tmp_path))
    
    reg = JobRegistration(name="test", script_path="s", log_path="l")
    job1 = StoredJob.from_registration("j1", 1, reg)
    job2 = StoredJob.from_registration("j2", 1, reg)
    
    # Upsert jobs
    store.upsert_job(job1)
    store.upsert_job(job2)
    
    loaded = store.load()
    assert len(loaded) == 2
    assert "j1" in loaded
    assert "j2" in loaded
    
    # Remove job
    store.remove_job("j1")
    loaded = store.load()
    assert len(loaded) == 1
    assert "j1" not in loaded
    
    # Remove remaining job -> should trigger clear() if list becomes empty?
    # logic in remove_job: if records: save else: clear
    store.remove_job("j2")
    
    # File should be removed by clear()
    assert not store.session_path.exists()

def test_store_clear(tmp_path):
    """Test clearing state removes files."""
    store = MonitorStateStore(str(tmp_path))
    store.save_jobs([]) # Create file
    
    assert store.session_path.exists()
    
    store.clear()
    
    assert not store.session_path.exists()
    # Legacy path check
    legacy_path = tmp_path / "monitor" / "state.json"
    assert not legacy_path.exists()

def test_store_save_session(tmp_path):
    """Test saving full session metadata."""
    store = MonitorStateStore(str(tmp_path))
    
    config = {"path": Path("/some/path"), "timeout": 10}
    store.save_session(config, project_name="my-project")
    
    data = store.load_session(store.session_path)
    assert data["project_name"] == "my-project"
    assert data["config"]["path"] == "/some/path" # Path serialized to str
    assert data["config"]["timeout"] == 10

def test_store_list_sessions(tmp_path):
    """Test listing multiple sessions."""
    # Create session 1
    store1 = MonitorStateStore(str(tmp_path), session_id="s1")
    store1.save_session({}, "p1")
    
    # Sleep to ensure timestamp diff
    time.sleep(0.01)
    
    # Create session 2
    store2 = MonitorStateStore(str(tmp_path), session_id="s2")
    store2.save_session({}, "p2")
    
    # Create garbage file
    (tmp_path / "garbage.json").write_text("not json")
    
    sessions = MonitorStateStore.list_sessions(tmp_path)
    
    # Should find 2 valid sessions, sorted by time desc (s2 then s1)
    assert len(sessions) == 2
    assert sessions[0]["session_id"] == "s2"
    assert sessions[1]["session_id"] == "s1"

def test_store_load_session_errors(tmp_path):
    """Test load_session error handling."""
    # Non-existent
    assert MonitorStateStore.load_session(tmp_path / "missing.json") is None
    
    # Corrupt
    p = tmp_path / "corrupt.json"
    p.write_text("{bad")
    assert MonitorStateStore.load_session(p) is None

def test_store_legacy_load(tmp_path):
    """Test loading from legacy path if session path missing."""
    store = MonitorStateStore(str(tmp_path), session_id="legacy-test")
    
    # Ensure session path missing
    if store.session_path.exists():
        store.session_path.unlink()
        
    # Write to legacy path
    legacy_path = tmp_path / "monitor" / "state.json"
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"jobs": [{"job_id": "l1", "name": "legacy", "script_path": "s", "log_path": "l"}]}
    legacy_path.write_text(json.dumps(payload))
    
    # Load should look at legacy path
    jobs = store.load()
    assert "l1" in jobs

def test_store_corrupt_payload(tmp_path):
    """Test loading corrupted payload returns empty."""
    store = MonitorStateStore(str(tmp_path))
    store.session_path.write_text("{bad")
    
    assert store.load() == {}
    assert store.load_events() == {}

def test_store_compatibility_fix(tmp_path):
    """Test compatibility fix for resolved_log_path."""
    store = MonitorStateStore(str(tmp_path))
    
    # Payload with old key "expanded_log_path"
    payload = {
        "jobs": [{
            "job_id": "c1", 
            "name": "compat", 
            "script_path": "s", 
            "log_path": "l",
            "expanded_log_path": "l_expanded"
        }]
    }
    store._write_payload(payload)
    
    jobs = store.load()
    assert "c1" in jobs
    assert jobs["c1"].resolved_log_path == "l_expanded"
