"""Tests for AppendToFileAction.

Chained from a LogEvent, this persists something a log line said (typically a
failing node name captured via ``extract_groups``) into an external list that
outlives the job -- e.g. the node-exclusion file read back when the next sbatch
is rendered.
"""

from __future__ import annotations

from pathlib import Path

from monitor.actions import (
    ActionContext,
    AppendToFileAction,
    AppendToFileActionConfig,
    EventRecord,
)


def _context(**payload) -> ActionContext:
    event = EventRecord(event_id="e", name="node_fault", source="log", payload=dict(payload))
    return ActionContext(event=event, job_metadata={"job_id": "j1"})


def test_appends_the_rendered_line(tmp_path: Path):
    target = tmp_path / "excluded.txt"
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}")
    )

    result = action.execute(_context(node="node-001-01"))

    assert result.status == "success"
    assert result.metadata["appended"] is True
    assert target.read_text() == "node-001-01\n"


def test_defaults_to_the_matched_text(tmp_path: Path):
    target = tmp_path / "excluded.txt"
    action = AppendToFileAction(AppendToFileActionConfig(path=str(target)))

    action.execute(_context(match="node-002-02"))

    assert target.read_text() == "node-002-02\n"


def test_dedup_skips_a_line_already_present(tmp_path: Path):
    """The same node failing twice must not grow the list twice."""
    target = tmp_path / "excluded.txt"
    target.write_text("node-001-01\n")
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}", dedup=True)
    )

    result = action.execute(_context(node="node-001-01"))

    assert result.status == "success"
    assert result.metadata["appended"] is False
    assert target.read_text() == "node-001-01\n"


def test_dedup_off_appends_regardless(tmp_path: Path):
    target = tmp_path / "excluded.txt"
    target.write_text("node-001-01\n")
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}", dedup=False)
    )

    action.execute(_context(node="node-001-01"))

    assert target.read_text() == "node-001-01\nnode-001-01\n"


def test_creates_parent_directories(tmp_path: Path):
    target = tmp_path / "nested" / "deeper" / "excluded.txt"
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}")
    )

    action.execute(_context(node="node-003-03"))

    assert target.read_text() == "node-003-03\n"


def test_create_parents_off_leaves_a_missing_directory_alone(tmp_path: Path):
    target = tmp_path / "nested" / "excluded.txt"
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}", create_parents=False)
    )

    try:
        action.execute(_context(node="node-004-04"))
    except OSError:
        pass  # the directory does not exist; either way nothing was created
    assert not target.exists()


def test_empty_content_appends_nothing(tmp_path: Path):
    """An event whose capture came back empty must not add a blank line."""
    target = tmp_path / "excluded.txt"
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}")
    )

    result = action.execute(_context(node="   "))

    assert result.status == "failed"
    assert not target.exists()


def test_the_written_list_reads_back_as_a_nodelist(tmp_path: Path):
    """End to end with the reader that consumes it."""
    from slurm_gen import read_exclude_nodes

    target = tmp_path / "excluded.txt"
    action = AppendToFileAction(
        AppendToFileActionConfig(path=str(target), content="{node}")
    )
    action.execute(_context(node="node-001-01"))
    action.execute(_context(node="node-002-02"))

    assert read_exclude_nodes(target) == "node-001-01,node-002-02"
