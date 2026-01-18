from __future__ import annotations

from pathlib import Path

from monitor.action_queue import ActionQueue
from monitor.actions import (
    LogAction,
    LogActionConfig,
    RestartAction,
    RestartActionConfig,
    DuplicateAction,
    DuplicateActionConfig,
)
from monitor.conditions import FileExistsCondition, FileExistsConditionConfig, TimeoutCondition, TimeoutConditionConfig
from monitor.dispatcher import ActionDispatcher
from monitor.event_bindings import EventActionBinding
from monitor.events import EventRecord, EventStatus
from monitor.submission import JobRegistration, JobRuntimeState
from monitor.watcher import MonitorEvent


def _state(job_id: str = "job-1") -> JobRuntimeState:
    return JobRuntimeState(
        job_id=job_id,
        registration=JobRegistration(name="demo", command=["echo", "hi"], log_path="demo.log"),
        attempts=2,
        submitted=True,
    )


def _event_record() -> EventRecord:
    return EventRecord(event_id="evt-1", name="progress", source="log")


def _build_metadata(state: JobRuntimeState) -> dict[str, str]:
    return {
        "job_id": state.job_id,
        "job_name": state.name,
    }


def test_dispatcher_no_actions_marks_processed():
    dispatcher = ActionDispatcher(action_queue=None, build_job_metadata=_build_metadata)
    state = _state()
    record = _event_record()
    event = MonitorEvent(job_id=state.job_id, name="progress", actions=[])

    result = dispatcher.dispatch(state, event, record, workspace=None)

    assert result["restart"] is False
    assert record.status == EventStatus.PROCESSED


def test_dispatcher_condition_not_met_marks_pending(tmp_path):
    dispatcher = ActionDispatcher(action_queue=None, build_job_metadata=_build_metadata)
    state = _state()
    record = _event_record()
    missing_path = tmp_path / "missing.txt"
    condition = FileExistsCondition(FileExistsConditionConfig(path=str(missing_path)))
    binding = EventActionBinding(
        action=LogAction(LogActionConfig(message="hello")),
        mode="inline",
        conditions=[condition],
        action_id="log:progress:LogAction:0",
    )
    event = MonitorEvent(job_id=state.job_id, name="progress", actions=[binding])

    result = dispatcher.dispatch(state, event, record, workspace=None)

    assert result["queued"] == []
    assert result["results"] == []
    assert record.status == EventStatus.PENDING


def test_dispatcher_queue_action_renders_payload(tmp_path):
    queue = ActionQueue(tmp_path)
    dispatcher = ActionDispatcher(action_queue=queue, build_job_metadata=_build_metadata)
    state = _state()
    record = _event_record()
    action = LogAction(LogActionConfig(message="job {job_id}"))
    binding = EventActionBinding(action=action, mode="queue", conditions=[], action_id="log:progress:LogAction:0")
    event = MonitorEvent(job_id=state.job_id, name="progress", actions=[binding])

    result = dispatcher.dispatch(state, event, record, workspace=Path("/tmp"))

    queued = queue.list()
    assert len(queued) == 1
    assert queued[0].config["message"] == f"job {state.job_id}"
    assert result["queued"] == [queued[0].queue_id]
    assert record.status == EventStatus.PENDING


def test_dispatcher_inline_restart_and_duplicate():
    dispatcher = ActionDispatcher(action_queue=None, build_job_metadata=_build_metadata)
    state = _state()
    record = _event_record()
    restart = RestartAction(RestartActionConfig(extra_args=["--id={job_id}"]))
    duplicate = DuplicateAction(DuplicateActionConfig(name_suffix="_{job_name}"))
    event = MonitorEvent(
        job_id=state.job_id,
        name="progress",
        actions=[
            EventActionBinding(action=restart, mode="inline", conditions=[], action_id="log:progress:RestartAction:0"),
            EventActionBinding(action=duplicate, mode="inline", conditions=[], action_id="log:progress:DuplicateAction:1"),
        ],
    )

    result = dispatcher.dispatch(state, event, record, workspace=None)

    assert result["restart"] is True
    assert result["restart_adjustments"]["extra_args"] == [f"--id={state.job_id}"]
    assert result["duplicates"]
    assert result["duplicates"][0]["name_suffix"] == f"_{state.name}"


def test_dispatcher_tracks_action_state_for_conditions():
    dispatcher = ActionDispatcher(action_queue=None, build_job_metadata=_build_metadata)
    state = _state()
    record = _event_record()
    condition = TimeoutCondition(TimeoutConditionConfig(timeout_seconds=60))
    binding = EventActionBinding(
        action=LogAction(LogActionConfig(message="hello")),
        mode="inline",
        conditions=[condition],
        action_id="log:progress:LogAction:0",
    )
    event = MonitorEvent(job_id=state.job_id, name="progress", actions=[binding])

    dispatcher.dispatch(state, event, record, workspace=None)

    action_state = record.metadata.get("action_state", {}).get(binding.action_id)
    assert action_state is not None
    condition_state = action_state.get("conditions", {}).get("0")
    assert condition_state is not None
    assert "started_ts" in condition_state
