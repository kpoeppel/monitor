from __future__ import annotations

from pathlib import Path

from monitor.utils.paths import resolve_log_path, update_log_symlink


def test_resolve_log_path_expands_job_and_timestamp():
    resolved = resolve_log_path("logs/job_%j_%t.log", job_id="123", timestamp=456)
    assert str(resolved) == "logs/job_123_456.log"


def test_resolve_log_path_without_tokens():
    resolved = resolve_log_path("logs/output.log", timestamp=999)
    assert str(resolved) == "logs/output.log"


def test_update_log_symlink_points_to_target(tmp_path):
    target = tmp_path / "output.log"
    target.write_text("ok")
    link = tmp_path / "latest.log"

    update_log_symlink(target, link)

    assert link.is_symlink()
    assert link.resolve() == target.resolve()
