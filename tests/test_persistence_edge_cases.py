import pytest
import json
from unittest.mock import MagicMock, patch
from pathlib import Path
from monitor.persistence.state_store import MonitorStateStore, _serialize_for_json

def test_store_legacy_write_failure(tmp_path):
    """Test failure to write to legacy path is ignored."""
    store = MonitorStateStore(tmp_path)
    
    # Mock legacy path write to fail
    # We need to mock Path.write_text, but only for legacy path?
    # Easier: Make legacy path a directory or non-writable?
    # Or mock `pathlib.Path.write_text` and check arguments?
    
    legacy_path = tmp_path / "monitor" / "state.json"
    
    with patch.object(Path, "write_text") as mock_write:
        def side_effect(text, encoding=None):
            # Check if this is the legacy path instance
            # We can't easily check instance identity.
            # But we can check if path string ends with "monitor/state.json"
            # Wait, `mock_write` is called on the Path object. 
            # How to differentiate?
            pass
            
    # Alternative: Mock `_legacy_path` on the store instance to be a read-only file or invalid path?
    # MonitorStateStore init creates the directory.
    
    # Let's try to mock the specific write call in `_write_payload`
    # But `_write_payload` calls `self._legacy_path.write_text`.
    
    # We can mock `self._legacy_path` on the instance!
    store._legacy_path = MagicMock()
    store._legacy_path.write_text.side_effect = OSError("readonly")
    store._legacy_path.exists.return_value = False # So clear() doesn't try to unlink it if called
    
    store.save_jobs([]) # This calls _write_payload
    
    # Should not raise exception
    store._legacy_path.write_text.assert_called()

def test_store_load_events_exception(tmp_path):
    """Test load_events handles exceptions during parsing."""
    store = MonitorStateStore(tmp_path)
    
    # Create valid JSON but invalid event data (missing required fields for EventRecord)
    payload = {
        "events": [
            {"event_id": "e1"}, # Missing name, etc. -> parse_config raises error
            {"event_id": "e2", "name": "valid", "source": "s"}
        ]
    }
    store._write_payload(payload)
    
    events = store.load_events()
    
    # e1 failed, e2 passed
    assert len(events) == 1
    assert "e2" in events
    assert "e1" not in events

def test_serialize_for_json_exception():
    """Test _serialize_for_json handles non-serializable objects gracefully."""
    
    class Unstr:
        def __str__(self):
            raise ValueError("cannot str")
            
    obj = Unstr()
    
    result = _serialize_for_json(obj)
    assert result is None
    
    # Valid cases
    assert _serialize_for_json(Path("p")) == "p"
    assert _serialize_for_json({"a": Path("b")}) == {"a": "b"}
    assert _serialize_for_json([Path("c")]) == ["c"]
