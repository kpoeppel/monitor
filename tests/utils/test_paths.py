from __future__ import annotations

from monitor.utils.paths import expand_log_path, resolve_log_path


def test_expand_log_path_array() -> None:
    path = expand_log_path("logs/job_%A_%a.log", "123_4")
    assert str(path) == "logs/job_123_4.log"


def test_resolve_log_path_timestamp() -> None:
    path = resolve_log_path("logs/job_%t.log", timestamp=42)
    assert str(path) == "logs/job_42.log"
