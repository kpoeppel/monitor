"""Tests for checkpoint classification and QuarantineCheckpointAction.

``checkpoint_status`` has THREE answers, and the third is the point: a
checkpoint whose ``.metadata`` cannot be read is UNVERIFIABLE, not broken. The
action refuses to choose a resume point in that case rather than guessing --
guessing either way discards real training or resumes from corrupt state.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from monitor.actions import (
    ActionContext,
    EventRecord,
    QuarantineCheckpointAction,
    QuarantineCheckpointActionConfig,
    checkpoint_is_complete,
    checkpoint_status,
)


def _checkpoint(root: Path, iteration: int, *, complete: bool = True, tmp_shard: bool = False):
    """Build an iter_* directory.

    ``complete`` writes .metadata and common.pt.
    """
    d = root / f"iter_{iteration:07d}"
    d.mkdir(parents=True)
    (d / "__0_0.distcp").write_text("shard")
    if complete:
        (d / ".metadata").write_text("metadata")
        (d / "common.pt").write_text("common")
    if tmp_shard:
        (d / ".__1_0.distcp.tmp").write_text("in flight")
    return d


@pytest.fixture
def unreadable_metadata(monkeypatch):
    """Torch is absent here, so `.metadata` is never actually parsed."""
    import monitor.actions as actions

    monkeypatch.setattr(actions, "_torch_dist_missing_files", lambda path: None)


@pytest.fixture
def verified_metadata(monkeypatch):
    """Pretend `.metadata` parsed and promised nothing that is missing."""
    import monitor.actions as actions

    monkeypatch.setattr(actions, "_torch_dist_missing_files", lambda path: [])


class TestCheckpointStatus:
    def test_missing_directory_is_incomplete(self, tmp_path: Path):
        status, why = checkpoint_status(tmp_path / "iter_0000100")
        assert status == "incomplete"
        assert "not a directory" in why

    def test_no_metadata_means_the_save_never_finished(self, tmp_path: Path):
        d = _checkpoint(tmp_path, 100, complete=False)
        status, why = checkpoint_status(d)
        assert status == "incomplete"
        assert ".metadata" in why

    def test_leftover_tmp_shards_are_incomplete(self, tmp_path: Path):
        d = _checkpoint(tmp_path, 100, tmp_shard=True)
        status, why = checkpoint_status(d)
        assert status == "incomplete"
        assert "tmp shards" in why

    def test_missing_common_pt_is_incomplete(self, tmp_path: Path):
        d = _checkpoint(tmp_path, 100)
        (d / "common.pt").unlink()
        status, _ = checkpoint_status(d)
        assert status == "incomplete"

    def test_unreadable_metadata_is_unverifiable_not_complete(
        self, tmp_path: Path, unreadable_metadata
    ):
        """Never guess: an unreadable checkpoint may be perfectly good."""
        d = _checkpoint(tmp_path, 100)
        status, why = checkpoint_status(d)
        assert status == "unverifiable"
        assert "unreadable" in why
        assert checkpoint_is_complete(d) == (False, why)

    def test_shards_named_in_metadata_but_absent_are_incomplete(self, tmp_path: Path, monkeypatch):
        import monitor.actions as actions

        monkeypatch.setattr(actions, "_torch_dist_missing_files", lambda path: ["__7_0.distcp"])
        d = _checkpoint(tmp_path, 100)
        status, why = checkpoint_status(d)
        assert status == "incomplete"
        assert "1 shard(s)" in why

    def test_verified_against_metadata_is_complete(self, tmp_path: Path, verified_metadata):
        d = _checkpoint(tmp_path, 100)
        status, why = checkpoint_status(d)
        assert status == "complete"
        assert "verified against .metadata" in why
        assert checkpoint_is_complete(d)[0] is True


def _run(root: Path, iteration: str = "{iteration}", **kwargs):
    action = QuarantineCheckpointAction(
        QuarantineCheckpointActionConfig(checkpoint_dir=str(root), iteration=iteration, **kwargs)
    )
    event = EventRecord(event_id="e", name="ckpt", source="log", payload={"iteration": "200"})
    return action.execute(ActionContext(event=event, job_metadata={"job_id": "j"}))


class TestQuarantineCheckpointAction:
    def test_moves_the_bad_checkpoint_aside_and_rolls_the_tracker_back(
        self, tmp_path: Path, verified_metadata
    ):
        _checkpoint(tmp_path, 100)
        _checkpoint(tmp_path, 200)
        tracker = tmp_path / "latest_checkpointed_iteration.txt"
        tracker.write_text("200\n")

        result = _run(tmp_path)

        assert result.status == "success"
        assert result.metadata["resume_iteration"] == 100
        assert not (tmp_path / "iter_0000200").exists()
        assert (tmp_path / "failed_iter_0000200").is_dir()
        assert tracker.read_text().strip() == "100"

    def test_dry_run_changes_nothing(self, tmp_path: Path, verified_metadata):
        _checkpoint(tmp_path, 100)
        _checkpoint(tmp_path, 200)
        tracker = tmp_path / "latest_checkpointed_iteration.txt"
        tracker.write_text("200\n")

        result = _run(tmp_path, dry_run=True)

        assert result.status == "success"
        assert result.metadata["dry_run"] is True
        assert (tmp_path / "iter_0000200").is_dir()
        assert tracker.read_text().strip() == "200"

    def test_falls_back_to_the_tracker_when_the_iteration_is_not_captured(
        self, tmp_path: Path, verified_metadata
    ):
        _checkpoint(tmp_path, 100)
        _checkpoint(tmp_path, 300)
        (tmp_path / "latest_checkpointed_iteration.txt").write_text("300\n")

        result = _run(tmp_path, iteration="not-a-number")

        assert result.status == "success"
        assert result.metadata["quarantined_iteration"] == 300

    def test_fails_when_the_iteration_cannot_be_determined(self, tmp_path: Path):
        _checkpoint(tmp_path, 100)
        result = _run(tmp_path, iteration="not-a-number")
        assert result.status == "failed"
        assert "failing iteration" in result.message

    def test_missing_checkpoint_dir_fails(self, tmp_path: Path):
        result = _run(tmp_path / "nope")
        assert result.status == "failed"
        assert "not found" in result.message

    def test_refuses_once_max_rollbacks_have_been_quarantined(
        self, tmp_path: Path, verified_metadata
    ):
        """A persistent fault must not walk backwards through every checkpoint."""
        _checkpoint(tmp_path, 100)
        (tmp_path / "failed_iter_0000200").mkdir()
        (tmp_path / "failed_iter_0000300").mkdir()

        result = _run(tmp_path, max_rollbacks=2)

        assert result.status == "failed"
        assert "refusing to quarantine" in result.message
        assert (tmp_path / "iter_0000100").is_dir()

    def test_refuses_and_changes_nothing_when_a_candidate_is_unverifiable(
        self, tmp_path: Path, unreadable_metadata
    ):
        """Neither skipping past nor resuming from it is safe."""
        _checkpoint(tmp_path, 100)
        _checkpoint(tmp_path, 200)
        tracker = tmp_path / "latest_checkpointed_iteration.txt"
        tracker.write_text("200\n")

        result = _run(tmp_path)

        assert result.status == "failed"
        assert "cannot verify" in result.message
        assert result.metadata["unverifiable"] == "iter_0000100"
        # Nothing was touched: the tree is exactly as it was.
        assert (tmp_path / "iter_0000200").is_dir()
        assert tracker.read_text().strip() == "200"

    def test_fails_when_no_loadable_checkpoint_is_left(self, tmp_path: Path):
        """Every candidate is genuinely incomplete, so there is nowhere to go."""
        _checkpoint(tmp_path, 100, complete=False)
        _checkpoint(tmp_path, 200)
        (tmp_path / "latest_checkpointed_iteration.txt").write_text("200\n")

        result = _run(tmp_path)

        assert result.status == "failed"
        assert "no loadable checkpoint left" in result.message
        assert result.metadata["rejected"]
        # Refused before mutating: the bad checkpoint is still in place.
        assert (tmp_path / "iter_0000200").is_dir()

    def test_handles_an_already_absent_bad_directory(self, tmp_path: Path, verified_metadata):
        _checkpoint(tmp_path, 100)
        (tmp_path / "latest_checkpointed_iteration.txt").write_text("200\n")

        result = _run(tmp_path)

        assert result.status == "success"
        assert "already absent" in result.message
        assert result.metadata["resume_iteration"] == 100
