from __future__ import annotations

from pathlib import Path

from monitor.utils.paths import expand_log_path, resolve_log_path, update_log_symlink


def test_expand_log_path_array() -> None:
    path = expand_log_path("logs/job_%A_%a.log", "123_4")
    assert str(path) == "logs/job_123_4.log"


def test_resolve_log_path_timestamp() -> None:
    path = resolve_log_path("logs/job_%t.log", timestamp=42)
    assert str(path) == "logs/job_42.log"


def test_expand_log_path_non_array() -> None:
    path = expand_log_path("logs/job_%A_%a_%j.log", "123")
    assert str(path) == "logs/job_123_0_123.log"


def test_resolve_log_path_with_job_id_and_timestamp() -> None:
    path = resolve_log_path("logs/job_%j_%t.log", job_id="55", timestamp=9)
    assert str(path) == "logs/job_55_9.log"


def test_update_log_symlink(tmp_path: Path) -> None:
    target = tmp_path / "logs" / "job.log"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("ok", encoding="utf-8")
    link = tmp_path / "latest.log"

    update_log_symlink(target, link)

    assert link.is_symlink()
    assert link.resolve() == target
