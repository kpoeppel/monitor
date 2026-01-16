import pytest
from monitor.persistence import MonitorStateStore, StoredJob
from monitor.controller import JobRegistration

def test_store_upsert_and_load_job(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    store = MonitorStateStore(str(state_dir))
    
    reg = JobRegistration(name="job1", script_path="s", log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    
    store.upsert_job(job)
    
    loaded = store.load()
    assert "123" in loaded
    assert loaded["123"].job_id == "123"
    assert loaded["123"].name == "job1"

def test_store_remove_job(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    store = MonitorStateStore(str(state_dir))
    
    reg = JobRegistration(name="job1", script_path="s", log_path="l")
    job = StoredJob.from_registration("123", 1, reg)
    store.upsert_job(job)
    
    store.remove_job("123")
    loaded = store.load()
    assert "123" not in loaded
