from __future__ import annotations

from pathlib import Path

from monitor.loop import JobFileStore, JobRecordConfig, stable_hash_hex
from monitor.submission import LocalJobConfig


def test_job_store_finished_filtering(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    record = JobRecordConfig(
        job_id="job1",
        definition=LocalJobConfig(
            name="job1",
            command=["echo", "hi"],
            log_path=str(tmp_path / "job1_%j.log"),
        ),
    )
    store.upsert(record)

    store.mark_finished("job1", "finished")

    assert store.load("job1") is None
    loaded = store.load("job1", include_finished=True)
    assert loaded is not None
    assert loaded.runtime.final_state == "finished"
    assert loaded.runtime.end_ts is not None
    assert store.load_all() == []
    assert len(store.load_all(include_finished=True)) == 1


def test_job_store_load_corrupt_json(tmp_path: Path) -> None:
    """Corrupt JSON files should be silently skipped in load_all and return None in load."""
    store = JobFileStore(tmp_path / "state")
    corrupt = store.root / "bad.job.json"
    corrupt.write_text("not valid json", encoding="utf-8")

    assert store.load_all() == []
    assert store.load("bad") is None


def test_job_store_mark_finished_nonexistent(tmp_path: Path) -> None:
    """mark_finished on an unknown job ID should be a no-op."""
    store = JobFileStore(tmp_path / "state")
    store.mark_finished("ghost", "finished")  # should not raise


def test_stable_hash_hex() -> None:
    h = stable_hash_hex("hello")
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex is 64 chars
    assert stable_hash_hex("hello") == stable_hash_hex("hello")
    assert stable_hash_hex("hello") != stable_hash_hex("world")
