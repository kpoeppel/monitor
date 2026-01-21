from __future__ import annotations

from pathlib import Path

from monitor.loop import JobFileStore, JobRecordConfig
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
