from __future__ import annotations

from monitor.persistence import MonitorStateStore


def test_state_store_save_and_load_config(tmp_path):
    store = MonitorStateStore(tmp_path)
    payload = {"monitor": {"class_name": "NullMonitor"}, "jobs": []}

    store.save_config(payload)
    loaded = store.load_config()

    assert loaded == payload
