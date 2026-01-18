"""Comprehensive tests for ActionQueue module."""

import pytest
import tempfile
import json
import time
from pathlib import Path
from monitor.action_queue import ActionQueue, QueuedAction


def test_queued_action_to_dict():
    """Test QueuedAction serialization to dict."""
    action = QueuedAction(
        queue_id="test-123",
        action_class="TestAction",
        config={"param": "value"},
        event_id="event-456",
        status="pending",
        metadata={"key": "val"},
        result={"output": "data"}
    )

    data = action.to_dict()

    assert data["queue_id"] == "test-123"
    assert data["action_class"] == "TestAction"
    assert data["config"] == {"param": "value"}
    assert data["event_id"] == "event-456"
    assert data["status"] == "pending"
    assert data["metadata"] == {"key": "val"}
    assert data["result"] == {"output": "data"}
    assert "enqueued_at" in data
    assert "updated_at" in data


def test_queued_action_from_dict():
    """Test QueuedAction deserialization from dict."""
    payload = {
        "queue_id": "test-123",
        "action_class": "TestAction",
        "config": {"param": "value"},
        "event_id": "event-456",
        "status": "running",
        "enqueued_at": 1234567890.0,
        "updated_at": 1234567891.0,
        "metadata": {"key": "val"},
        "result": {"output": "data"}
    }

    action = QueuedAction.from_dict(payload)

    assert action.queue_id == "test-123"
    assert action.action_class == "TestAction"
    assert action.config == {"param": "value"}
    assert action.event_id == "event-456"
    assert action.status == "running"
    assert action.enqueued_at == 1234567890.0
    assert action.updated_at == 1234567891.0
    assert action.metadata == {"key": "val"}
    assert action.result == {"output": "data"}


def test_queued_action_from_dict_with_defaults():
    """Test QueuedAction deserialization with missing optional fields."""
    payload = {
        "queue_id": "test-123",
        "action_class": "TestAction",
        "event_id": "event-456"
    }

    action = QueuedAction.from_dict(payload)

    assert action.queue_id == "test-123"
    assert action.action_class == "TestAction"
    assert action.event_id == "event-456"
    assert action.status == "pending"
    assert action.config == {}
    assert action.metadata == {}
    assert action.result == {}


def test_action_queue_initialization():
    """Test ActionQueue initialization creates root directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue_root = Path(tmpdir) / "queue"
        queue = ActionQueue(queue_root)

        assert queue_root.exists()
        assert queue_root.is_dir()


def test_action_queue_enqueue():
    """Test enqueuing actions to the queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="SlackNotify",
            config={"channel": "#alerts"},
            event_id="error-123",
            metadata={"severity": "high"}
        )

        assert action.action_class == "SlackNotify"
        assert action.config == {"channel": "#alerts"}
        assert action.event_id == "error-123"
        assert action.status == "pending"
        assert action.metadata == {"severity": "high"}
        assert action.queue_id is not None


def test_action_queue_list():
    """Test listing all actions in the queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Enqueue multiple actions
        action1 = queue.enqueue(
            action_class="Action1",
            config={},
            event_id="event-1"
        )

        action2 = queue.enqueue(
            action_class="Action2",
            config={},
            event_id="event-2"
        )

        actions = queue.list()

        assert len(actions) == 2
        queue_ids = {a.queue_id for a in actions}
        assert action1.queue_id in queue_ids
        assert action2.queue_id in queue_ids


def test_action_queue_claim_next():
    """Test claiming next pending action."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Enqueue an action
        enqueued = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        # Claim the next action
        claimed = queue.claim_next()

        assert claimed is not None
        assert claimed.queue_id == enqueued.queue_id
        assert claimed.status == "running"

        # Try to claim again - should be None since no pending actions
        claimed2 = queue.claim_next()
        assert claimed2 is None


def test_action_queue_claim_next_empty_queue():
    """Test claiming from empty queue returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        claimed = queue.claim_next()
        assert claimed is None


def test_action_queue_mark_done():
    """Test marking action as done removes it from queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        # Mark as done
        queue.mark_done(
            action.queue_id,
            status="done",
            result={"success": True}
        )

        # Should no longer be in queue
        actions = queue.list()
        assert len(actions) == 0


def test_action_queue_mark_failed():
    """Test marking action as failed removes it from queue."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        # Mark as failed
        queue.mark_done(
            action.queue_id,
            status="failed",
            result={"error": "Connection timeout"}
        )

        # Should no longer be in queue
        actions = queue.list()
        assert len(actions) == 0


def test_action_queue_mark_done_nonexistent():
    """Test marking nonexistent action as done is a no-op."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Should not raise an error
        queue.mark_done("nonexistent-id", status="done")


def test_action_queue_load():
    """Test loading action without mutating state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="TestAction",
            config={"key": "value"},
            event_id="event-1"
        )

        # Load the action
        loaded = queue.load(action.queue_id)

        assert loaded is not None
        assert loaded.queue_id == action.queue_id
        assert loaded.action_class == "TestAction"
        assert loaded.config == {"key": "value"}
        assert loaded.status == "pending"


def test_action_queue_load_nonexistent():
    """Test loading nonexistent action returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        loaded = queue.load("nonexistent-id")
        assert loaded is None


def test_action_queue_retry():
    """Test retrying a running action resets it to pending."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        # Claim the action (sets to running)
        queue.claim_next()

        # Retry it
        result = queue.retry(action.queue_id)

        assert result is True

        # Verify it's back to pending
        loaded = queue.load(action.queue_id)
        assert loaded is not None
        assert loaded.status == "pending"


def test_action_queue_retry_nonexistent():
    """Test retrying nonexistent action returns False."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        result = queue.retry("nonexistent-id")
        assert result is False


def test_action_queue_corrupted_json():
    """Test loading corrupted JSON file returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Create a corrupted JSON file manually
        event_dir = Path(tmpdir) / "event-1"
        event_dir.mkdir()
        corrupted_file = event_dir / "corrupted.json"
        corrupted_file.write_text("{invalid json", encoding="utf-8")

        # List should skip corrupted files
        actions = queue.list()
        assert len(actions) == 0


def test_action_queue_invalid_data():
    """Test loading JSON with invalid data structure returns None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Create a JSON file with invalid structure
        event_dir = Path(tmpdir) / "event-1"
        event_dir.mkdir()
        invalid_file = event_dir / "invalid.json"
        invalid_file.write_text('{"missing": "required_fields"}', encoding="utf-8")

        # List should skip invalid files
        actions = queue.list()
        assert len(actions) == 0


def test_action_queue_multiple_events():
    """Test queue handles multiple event directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Enqueue actions for different events
        action1 = queue.enqueue(
            action_class="Action1",
            config={},
            event_id="event-1"
        )

        action2 = queue.enqueue(
            action_class="Action2",
            config={},
            event_id="event-2"
        )

        # Both should be listed
        actions = queue.list()
        assert len(actions) == 2

        # Event directories should exist
        assert (Path(tmpdir) / "event-1").exists()
        assert (Path(tmpdir) / "event-2").exists()


def test_action_queue_non_directory_files():
    """Test queue ignores non-directory files in root."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Create a regular file in root
        (Path(tmpdir) / "not-a-dir.txt").write_text("test")

        action = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        # Should only find the real action
        actions = queue.list()
        assert len(actions) == 1
        assert actions[0].queue_id == action.queue_id


def test_action_queue_mark_done_cleans_empty_directories():
    """Test marking done cleans up empty event directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        action = queue.enqueue(
            action_class="TestAction",
            config={},
            event_id="event-1"
        )

        event_dir = Path(tmpdir) / "event-1"
        assert event_dir.exists()

        # Mark as done
        queue.mark_done(action.queue_id, status="done")

        # Event directory should be cleaned up
        assert not event_dir.exists()


def test_action_queue_ordering_by_enqueue_time():
    """Test actions are listed in order of enqueue time."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queue = ActionQueue(tmpdir)

        # Enqueue with slight time delays
        action1 = queue.enqueue(
            action_class="Action1",
            config={},
            event_id="event-1"
        )

        time.sleep(0.01)

        action2 = queue.enqueue(
            action_class="Action2",
            config={},
            event_id="event-1"
        )

        actions = queue.list()

        # Should be ordered by enqueue time
        assert len(actions) == 2
        assert actions[0].queue_id == action1.queue_id
        assert actions[1].queue_id == action2.queue_id
