from __future__ import annotations

import time
from pathlib import Path

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
    LogEvent,
    LogEventConfig,
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


def test_event_record_touch_increments_count() -> None:
    record = EventRecord(event_id="e1", name="evt", source="log")
    assert record.count == 1
    record.touch()
    assert record.count == 2
    record.touch(payload={"key": "val"})
    assert record.count == 3
    assert record.payload == {"key": "val"}


def test_action_context_variables_includes_workspace(tmp_path: Path) -> None:
    event = EventRecord(event_id="e1", name="evt", source="log")
    ctx = ActionContext(event=event, workspace=tmp_path)
    assert "workspace" in ctx.variables
    assert ctx.variables["workspace"] == str(tmp_path)


def test_log_action_debug_level(caplog) -> None:
    import logging
    action = LogAction(LogActionConfig(message="debug msg", level="debug"))
    with caplog.at_level(logging.DEBUG):
        result = action.execute(_context())
    assert result.status == "success"
    assert any("debug msg" in r.message for r in caplog.records)


def test_log_action_warning_level(caplog) -> None:
    import logging
    action = LogAction(LogActionConfig(message="warn msg", level="warning"))
    with caplog.at_level(logging.WARNING):
        result = action.execute(_context())
    assert result.status == "success"
    assert any("warn msg" in r.message for r in caplog.records)


def test_log_action_error_level(caplog) -> None:
    import logging
    action = LogAction(LogActionConfig(message="err msg", level="error"))
    with caplog.at_level(logging.ERROR):
        result = action.execute(_context())
    assert result.status == "success"
    assert any("err msg" in r.message for r in caplog.records)


def test_log_event_regex_pattern() -> None:
    event = LogEvent(LogEventConfig(name="e", pattern=r"step=(\d+)", pattern_type="regex", match_once=False))
    triggers = event.check_triggers("step=42 done\nstep=100 ok")
    assert len(triggers) == 2
    assert triggers[0]["match"] == "step=42"
    assert triggers[1]["match"] == "step=100"


def test_log_event_extract_groups_by_index() -> None:
    event = LogEvent(LogEventConfig(
        name="e",
        pattern=r"epoch=(\d+)",
        pattern_type="regex",
        extract_groups={"epoch": 1, "full": "match"},
    ))
    triggers = event.check_triggers("epoch=7")
    assert len(triggers) == 1
    assert triggers[0]["epoch"] == "7"
    assert triggers[0]["full"] == "epoch=7"


def test_log_event_extract_groups_missing_group() -> None:
    # Group 2 doesn't exist — should be silently skipped
    event = LogEvent(LogEventConfig(
        name="e",
        pattern=r"val=(\d+)",
        pattern_type="regex",
        extract_groups={"v": 1, "missing": 99},
    ))
    triggers = event.check_triggers("val=5")
    assert len(triggers) == 1
    assert triggers[0]["v"] == "5"
    assert "missing" not in triggers[0]
