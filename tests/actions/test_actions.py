from __future__ import annotations

from slurm_gen import SlurmConfig

from monitor.actions import (
    ActionContext,
    LogAction,
    LogActionConfig,
    RestartAction,
    RestartActionConfig,
    CancelAction,
    CancelActionConfig,
    FinishAction,
    FinishActionConfig,
    NewJobAction,
    NewJobActionConfig,
    EventRecord,
)
from monitor.submission import LocalJobConfig, SlurmJobConfig


def _context() -> ActionContext:
    event = EventRecord(event_id="evt1", name="evt", source="log")
    return ActionContext(event=event, job_metadata={"class_name": "LocalJob", "job_name": "job1"})


def test_log_action_renders_message() -> None:
    action = LogAction(LogActionConfig(message="event {event_name}"))
    result = action.execute(_context())
    assert result.status == "success"
    assert result.message == "event evt"


def test_restart_action() -> None:
    action = RestartAction(RestartActionConfig(reason="oom"))
    result = action.execute(_context())
    assert result.special == "restart"
    assert result.status == "success"
    assert "oom" in result.message


def test_cancel_action() -> None:
    action = CancelAction(CancelActionConfig(reason="user_requested"))
    result = action.execute(_context())
    assert result.special == "cancel"
    assert result.status == "success"
    assert "user_requested" in result.message


def test_finish_action() -> None:
    action = FinishAction(FinishActionConfig(reason="completed_early"))
    result = action.execute(_context())
    assert result.special == "finish"
    assert result.status == "success"
    assert "completed_early" in result.message


def test_new_job_action_with_local_config() -> None:
    job_config = LocalJobConfig(name="new_job", command=["echo", "hello"], log_path="/tmp/new_job.log")
    action = NewJobAction(NewJobActionConfig(job_config=job_config))
    result = action.execute(_context())
    assert result.status == "success"
    assert result.action_config is not None
    assert isinstance(result.action_config, NewJobActionConfig)
    assert isinstance(result.action_config.job_config, LocalJobConfig)
    assert result.action_config.job_config.name == "new_job"


def test_new_job_action_with_slurm_config() -> None:
    job_config = SlurmJobConfig(
        name="slurm_job",
        log_path="/tmp/slurm_job.log",
        slurm=SlurmConfig(
            template_path="/tmp/template.sbatch",
            script_dir="/tmp/scripts",
            log_dir="/tmp/logs",
            command=["srun", "task"],
        ),
    )
    action = NewJobAction(NewJobActionConfig(job_config=job_config))
    result = action.execute(_context())
    assert result.status == "success"
    assert result.action_config is not None
    assert isinstance(result.action_config, NewJobActionConfig)
    assert isinstance(result.action_config.job_config, SlurmJobConfig)
    assert result.action_config.job_config.name == "slurm_job"
