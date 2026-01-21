from __future__ import annotations

from monitor.actions import EventRecord, build_event_id, event_key


def test_build_event_id_includes_checkpoint() -> None:
    event_id = build_event_id("job1", "checkpoint", {"checkpoint_iteration": 3}, now_ms=123)
    assert event_id == "job1:checkpoint:3:123"


def test_event_key_stable_hash() -> None:
    key1 = event_key("job1", "event", {"a": 1})
    key2 = event_key("job1", "event", {"a": 1})
    assert key1 == key2


def test_event_record_set_status_appends_note() -> None:
    record = EventRecord(event_id="e1", name="evt", source="log")
    record.set_status(note="ok")
    assert record.history[-1]["note"] == "ok"
