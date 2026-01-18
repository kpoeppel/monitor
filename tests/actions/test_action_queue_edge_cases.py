import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from monitor.action_queue import ActionQueue

def test_action_queue_mark_done_unlink_error(tmp_path):
    """Test OSError during unlink in mark_done is ignored."""
    queue = ActionQueue(tmp_path)
    action = queue.enqueue("Action", {}, event_id="e1")
    
    # Mock unlink to raise OSError
    with patch("pathlib.Path.unlink", side_effect=OSError("permission denied")):
        queue.mark_done(action.queue_id, status="done")
        
    # Should not raise exception

def test_action_queue_load_path_oserror(tmp_path):
    """Test OSError during read_text in _load_path."""
    queue = ActionQueue(tmp_path)
    
    # Create a valid action file
    action = queue.enqueue("Action", {}, event_id="e1")
    
    # Mock read_text to raise OSError
    with patch("pathlib.Path.read_text", side_effect=OSError("read failed")):
        # list() calls _load_path
        actions = queue.list()
        
    assert len(actions) == 0

def test_action_queue_mark_done_path_none(tmp_path):
    """Test mark_done when _find_record_path returns None."""
    queue = ActionQueue(tmp_path)
    
    # Mock _find_record_path to return None even if file exists?
    # Or simply pass an ID that doesn't exist.
    # We already have `test_action_queue_mark_done_nonexistent` which passes unknown ID.
    # That hits `if path is None: return`.
    
    queue.mark_done("unknown_id", status="done")


def test_action_queue_recover_running(tmp_path):
    queue = ActionQueue(tmp_path)
    action = queue.enqueue("Action", {}, event_id="e1")
    claimed = queue.claim_next()
    assert claimed is not None
    assert claimed.queue_id == action.queue_id

    recovered = queue.recover_running()
    assert recovered == 1
    reloaded = queue.load(action.queue_id)
    assert reloaded is not None
    assert reloaded.status == "pending"
