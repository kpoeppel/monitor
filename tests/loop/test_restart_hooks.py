"""Restart hooks: a RestartAction's ``pre_command`` runs before the resubmission
and ``exclude_file`` refreshes ``slurm.sbatch.exclude`` from the live exclusion
list (the stored SlurmConfig is otherwise frozen at plan time)."""

from __future__ import annotations

from types import SimpleNamespace

from monitor.actions import (
    ActionContext,
    EventRecord,
    RestartAction,
    RestartActionConfig,
    RunCommandAction,
    RunCommandActionConfig,
)
from monitor.loop import MonitorLoop


def test_restart_action_carries_its_hooks_in_the_result():
    cfg = RestartActionConfig(reason="r", pre_command="echo {job_id}", exclude_file="/tmp/x")
    event = EventRecord(event_id="e", name="n", source="log")
    result = RestartAction(cfg).execute(ActionContext(event=event, job_metadata={"job_id": "j"}))
    assert result.special == "restart"
    assert result.metadata["pre_command"] == "echo {job_id}"
    assert result.metadata["exclude_file"] == "/tmp/x"


def test_run_command_action_renders_and_runs(tmp_path):
    out = tmp_path / "out.txt"
    cfg = RunCommandActionConfig(command=f"echo node={{node}} job={{job_id}} > {out}")
    event = EventRecord(event_id="e", name="n", source="log", payload={"node": "jpbo-001-01"})
    result = RunCommandAction(cfg).execute(
        ActionContext(event=event, job_metadata={"job_id": "j1"})
    )
    assert result.status == "success"
    assert out.read_text().strip() == "node=jpbo-001-01 job=j1"


def test_run_restart_hooks_runs_pre_command_and_refreshes_excludes(tmp_path):
    exclude = tmp_path / "exclude.txt"
    exclude.write_text("# reason\njpbo-001-01\n\njpbo-002-02  \n")
    marker = tmp_path / "scan.txt"
    loop = MonitorLoop.__new__(MonitorLoop)  # no store/clients needed for the hook itself
    loop._pending_restart_hooks = {
        "job-a": {
            "pre_command": f"echo scanned {{runtime_job_id}} {{log_path}} > {marker}",
            "pre_command_timeout_s": 30,
            "exclude_file": str(exclude),
        }
    }
    sbatch = SimpleNamespace(exclude="jpbo-001-01")
    definition = SimpleNamespace(
        metadata={},
        name="jobname",
        class_name="SlurmJobConfig",
        slurm=SimpleNamespace(sbatch=sbatch),
        log_path=str(tmp_path / "slurm-%j.log"),
    )
    job = SimpleNamespace(
        job_id="job-a",
        definition=definition,
        runtime=SimpleNamespace(runtime_job_id="4242", start_ts=1.0, attempts=1),
    )
    # isinstance(job.definition, SlurmJobConfig) guards the exclude refresh: patch it for the namespace
    import monitor.loop as loop_mod

    real = loop_mod.SlurmJobConfig
    loop_mod.SlurmJobConfig = SimpleNamespace  # type: ignore[assignment]
    try:
        loop._run_restart_hooks(job, "4242")
    finally:
        loop_mod.SlurmJobConfig = real
    assert marker.read_text().startswith("scanned 4242 ")
    assert "slurm-4242.log" in marker.read_text()
    assert sbatch.exclude == "jpbo-001-01,jpbo-002-02"
    assert "job-a" not in loop._pending_restart_hooks


def test_hooks_are_a_no_op_without_configuration():
    loop = MonitorLoop.__new__(MonitorLoop)
    loop._pending_restart_hooks = {}
    job = SimpleNamespace(
        job_id="j",
        definition=SimpleNamespace(
            metadata={},
            name="n",
            class_name="c",
            slurm=SimpleNamespace(sbatch=SimpleNamespace(exclude="a")),
        ),
        runtime=SimpleNamespace(runtime_job_id=None, start_ts=None, attempts=0),
    )
    loop._run_restart_hooks(job, None)  # must not raise
    assert job.definition.slurm.sbatch.exclude == "a"


def test_restart_hook_variables_include_log_dir(tmp_path, monkeypatch):
    """The pre_command may scan the run's whole log DIRECTORY, not just one log.

    A node-fault scan needs {log_dir} because the lines that name a faulty node are
    often written after the kill, into whichever log was open at the time.
    """
    import subprocess

    from monitor.loop import JobRecordConfig, JobRuntimeConfig, MonitorLoop

    log = tmp_path / "logs" / "slurm-7.log"
    log.parent.mkdir()
    log.write_text("x")

    loop = MonitorLoop.__new__(MonitorLoop)
    job = JobRecordConfig(
        job_id="j",
        definition=SimpleNamespace(
            name="n", slurm=SimpleNamespace(sbatch=SimpleNamespace(exclude=""))
        ),
        runtime=JobRuntimeConfig(submitted=True, runtime_job_id="7"),
    )
    loop._pending_restart_hooks = {
        "j": {"pre_command": "echo {runtime_job_id} {log_dir}", "exclude_file": ""}
    }
    loop._build_job_metadata = lambda j: {}
    loop._resolve_log_path = lambda j: log

    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    loop._run_restart_hooks(job, "7")

    assert f"7 {log.parent}" in str(seen["cmd"])


def test_pre_command_timeout_never_blocks_the_restart(tmp_path, caplog):
    """A hook that hangs must not stop the job from being resubmitted."""
    import logging

    loop = MonitorLoop.__new__(MonitorLoop)
    loop._pending_restart_hooks = {
        "job-a": {"pre_command": "sleep 5", "pre_command_timeout_s": 0.05, "exclude_file": ""}
    }
    job = SimpleNamespace(
        job_id="job-a",
        definition=SimpleNamespace(metadata={}, name="n", class_name="c"),
        runtime=SimpleNamespace(runtime_job_id="1", start_ts=1.0, attempts=1),
    )
    loop._build_job_metadata = lambda j: {}
    loop._resolve_log_path = lambda j: tmp_path / "log"

    with caplog.at_level(logging.WARNING, logger="monitor.loop"):
        loop._run_restart_hooks(job, "1")  # must not raise

    assert "timed out" in caplog.text
    assert loop._pending_restart_hooks == {}


def test_empty_exclusion_file_leaves_the_existing_list_alone(tmp_path):
    """Losing the list entirely is far worse than using a slightly stale one."""
    exclude = tmp_path / "exclude.txt"
    exclude.write_text("# nothing ruled out yet\n\n")
    loop = MonitorLoop.__new__(MonitorLoop)
    loop._pending_restart_hooks = {"job-a": {"pre_command": "", "exclude_file": str(exclude)}}
    sbatch = SimpleNamespace(exclude="node-001-01")
    job = SimpleNamespace(
        job_id="job-a",
        definition=SimpleNamespace(
            metadata={}, name="n", class_name="c", slurm=SimpleNamespace(sbatch=sbatch)
        ),
        runtime=SimpleNamespace(runtime_job_id="1", start_ts=1.0, attempts=1),
    )
    loop._build_job_metadata = lambda j: {}
    loop._resolve_log_path = lambda j: tmp_path / "log"

    import monitor.loop as loop_mod

    real = loop_mod.SlurmJobConfig
    loop_mod.SlurmJobConfig = SimpleNamespace  # type: ignore[assignment]
    try:
        loop._run_restart_hooks(job, "1")
    finally:
        loop_mod.SlurmJobConfig = real

    assert sbatch.exclude == "node-001-01"
