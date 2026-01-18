"""Event identity helpers."""

from __future__ import annotations

from monitor.events import build_event_id, event_key


def test_event_key_with_checkpoint_iteration():
    key = event_key("job-1", "checkpoint", {"checkpoint_iteration": 5})
    assert key[:2] == ("job-1", "checkpoint")


def test_event_key_without_metadata():
    key = event_key("job-2", "status", None)
    assert key[:2] == ("job-2", "status")


def test_build_event_id_with_checkpoint_iteration():
    event_id = build_event_id(
        "job-3",
        "checkpoint",
        {"checkpoint_iteration": 7},
        now_ms=12345,
    )
    assert event_id == "job-3:checkpoint:7:12345"


def test_build_event_id_without_metadata():
    event_id = build_event_id("job-4", "status", None, now_ms=67890)
    assert event_id == "job-4:status:67890"
