import pytest
import time
from unittest.mock import MagicMock, patch
from pathlib import Path
from monitor.conditions import (
    FileContentCondition,
    FileContentConditionConfig,
    CommandCondition,
    CommandConditionConfig,
    ConditionContext,
    AlwaysTrueCondition,
    AlwaysTrueConditionConfig,
)
from monitor.watcher import SlurmLogMonitor, SlurmLogMonitorConfig, MonitoredJob, LogEventConfig
from monitor.states import StalledStateConfig, SuccessStateConfig
from monitor.controller import (
    MonitorController,
    JobRuntimeState,
    JobRegistration,
    MonitorDecision,
    MonitorRecord,
    MonitorCycleResult,
    MonitorOutcome,
)
from monitor.watcher import MonitorEvent
from monitor.events import EventRecord, EventStatus


def test_controller_init_with_existing_events(tmp_path):
    """Controller.py missing lines: 98-101 (loading events in __init__)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.state_events = []
    slurm = MagicMock()
    state_store = MagicMock()

    # Existing event in store
    evt = EventRecord(event_id="e1", name="n1", source="s")
    evt.metadata = {"job_id": "j1"}
    state_store.load_events.return_value = {"e1": evt}
    state_store.session_path = tmp_path / "s.json"

    controller = MonitorController(monitor, slurm, state_store)
    assert "e1" in controller._event_records
    # Verify index was built
    assert ("j1", "n1") in controller._event_index


def test_controller_finalize_event_stops():
    """Controller.py missing lines: 794-803 (finalize stop decisions)."""
    controller = MonitorController(MagicMock(), MagicMock())
    controller._executor = MagicMock()
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))

    # 1. Success
    from monitor.states import SuccessState, SuccessStateConfig

    evt_succ = MagicMock(state=SuccessState(SuccessStateConfig()))
    res = controller._finalize_event(state, evt_succ, {})
    assert res.action == "success"
    controller._executor.finalize_job.assert_called_with("1")

    # 2. Crash
    from monitor.states import CrashState, CrashStateConfig

    evt_crash = MagicMock(state=CrashState(CrashStateConfig()))
    res = controller._finalize_event(state, evt_crash, {})
    assert res.action == "stop"


def test_file_content_condition_oserror_final(tmp_path):
    """Line 214-220 in conditions.py: except OSError during read_text."""
    f = tmp_path / "protected.txt"
    f.touch()

    config = FileContentConditionConfig(path=str(f), pattern="READY")
    condition = FileContentCondition(config)

    with patch.object(Path, "read_text", side_effect=OSError("denied")):
        result = condition.check(ConditionContext())
        assert result.status == "waiting"


def test_command_condition_empty_final():
    """Line 257-259 in conditions.py: if not self.config.command."""
    config = CommandConditionConfig(command=[])
    condition = CommandCondition(config)
    result = condition.check(ConditionContext())
    assert result.status == "fail"
    assert "no command supplied" in result.message


def test_always_true_condition_final():
    """Line 81-82 in conditions.py."""
    config = AlwaysTrueConditionConfig(message="ok")
    condition = AlwaysTrueCondition(config)
    result = condition.check(ConditionContext())
    assert result.passed
    assert result.message == "ok"


def test_watcher_inactivity_edge_cases_final():
    """Watcher.py missing lines: 277, 285, 318, 320, 324."""
    config = SlurmLogMonitorConfig(log_path="l")
    monitor = SlurmLogMonitor(config)

    # 277: if stall_duration is None: return []
    events = monitor._build_inactivity_events(MagicMock(), MagicMock(), None, 100)
    assert events == []

    # 318: if not self._compiled_rules: return events
    monitor._compiled_rules = []
    events = monitor._extract_events(MagicMock(), "content", "", source="log")
    assert events == []


def test_watcher_metadata_lookup_error_final():
    """Watcher.py missing lines: 456-457, 460-461 (IndexError/KeyError in _extract_metadata)."""
    from monitor.watcher import _extract_metadata, LogEventConfig

    # Setup a rule that expects a group that doesn't exist
    rule = LogEventConfig(name="n", pattern=r"(\w+)", extract_groups={"val": 2}, metadata={"m": 1})
    match = MagicMock()
    # match.group(2) will raise IndexError
    match.group.side_effect = IndexError("no such group")

    result = _extract_metadata(match, rule)
    assert result == {}


def test_watcher_evaluation_oserrors_final(tmp_path):
    """Watcher.py missing lines: 148, 201 (OSError in evaluates)."""
    log_path = tmp_path / "job.log"
    log_path.touch()

    config = SlurmLogMonitorConfig(log_path=str(log_path))
    monitor = SlurmLogMonitor(config)

    job = MonitoredJob(
        job_id="1",
        name="test",
        log_path=str(log_path),
        check_interval_seconds=60,
        state="RUNNING",
        output_paths=[str(tmp_path / "out.txt")],
    )

    # Hit 148: log_path.read_text OSError
    with patch.object(Path, "read_text", side_effect=OSError("denied")):
        outcome = monitor._evaluate_job(job, time.time())
        assert outcome.status == "pending"

    # Hit 201: output_path.read_text OSError
    (tmp_path / "out.txt").touch()
    with patch.object(Path, "read_text") as mock_read:
        mock_read.side_effect = ["log content", OSError("failed")]
        outcome = monitor._evaluate_job(job, time.time())
        assert outcome.status == "active"


def test_watcher_prefix_and_empty_logic_final():
    """Watcher.py missing lines: 320 (prefix match), 324 (no new text)."""
    config = SlurmLogMonitorConfig(log_path="l")
    monitor = SlurmLogMonitor(config)
    monitor._compiled_rules = [MagicMock()]

    job = MagicMock()

    # 320: content.startswith(previous)
    content = "ABCDEF"
    previous = "AB"
    events = monitor._extract_events(job, content, previous, source="log")

    # 324: if not new_text: return events
    events2 = monitor._extract_events(job, "SAME", "SAME", source="log")
    assert events2 == []


def test_watcher_duplicate_inactivity_final():
    """Watcher.py missing line 285: if rule.name in snapshot.triggered_inactivity_events: continue."""
    from monitor.watcher import _JobSnapshot

    config = SlurmLogMonitorConfig(
        log_path="l",
        log_events=[
            LogEventConfig(
                name="stall", pattern="", pattern_type="inactivity", state=StalledStateConfig(), metadata={"m": 1}
            )
        ],
    )
    monitor = SlurmLogMonitor(config)

    job = MonitoredJob(job_id="1", name="n", log_path="l", check_interval_seconds=1, state="running")
    snapshot = _JobSnapshot(log_content="c", last_update=time.time() - 1000)
    snapshot.triggered_inactivity_events.add("stall")

    events = monitor._build_inactivity_events(job, snapshot, 1000, 100)
    assert events == []


def test_controller_slurm_transition_timeout_final():
    """Controller.py missing lines: 516-518 (TIMEOUT state transition)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.state_events = []
    slurm = MagicMock()
    controller = MonitorController(monitor, slurm)

    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    state.last_slurm_state = "RUNNING"

    # Transition to TIMEOUT
    records = controller._capture_slurm_transitions(state, "TIMEOUT")
    assert any(e.metadata.get("slurm_state") == "TIMEOUT" for e in records)
    assert state.state.key == "timeout"


def test_controller_slurm_transition_gone_final():
    """Controller.py missing lines: 521-523 (current_state is None and previous is not None)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.state_events = []
    slurm = MagicMock()
    controller = MonitorController(monitor, slurm)

    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))
    state.last_slurm_state = "RUNNING"

    # Job disappeared from squeue
    records = controller._capture_slurm_transitions(state, None)
    assert any(e.action == "run_ended" for e in records)
    assert state.state.key == "timeout"


def test_controller_running_finish_condition_final(tmp_path):
    """Controller.py missing lines: 196-213 (finish condition in running loop)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.poll_interval_seconds = 60
    monitor.config.state_events = []
    monitor.watch_sync.return_value = {}

    slurm = MagicMock()
    slurm.squeue.return_value = {"1": "RUNNING"}

    controller = MonitorController(monitor, slurm)

    # Pre-submitted job
    from monitor.conditions import AlwaysTrueConditionConfig

    reg = JobRegistration(name="n", script_path="s", log_path="l", finish_condition=AlwaysTrueConditionConfig())
    controller.register_job("1", reg)
    state = controller._submission_manager.get_job("1")
    state.submitted = True

    # Observe -> should detect finish condition
    controller.observe_once_sync()

    # Job should be removed
    assert len(list(controller.jobs())) == 0


def test_controller_handle_state_change_final():
    """Controller.py missing lines: 302-315 (handle_state_change)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.state_events = []
    controller = MonitorController(monitor, MagicMock())
    reg = JobRegistration(name="n", script_path="s", log_path="l")
    controller.register_job("1", reg)

    # 1. Success path
    controller._build_state_event = MagicMock()
    controller._handle_monitor_event = MagicMock(return_value=(MonitorDecision(action="success", reason="r"), "note"))

    decision = controller.handle_state_change("1", "success")
    assert decision.action == "success"

    # 2. Not found path
    decision_none = controller.handle_state_change("unknown", "success")
    assert decision_none.action == "noop"

    # 3. No action result path
    controller._handle_monitor_event = MagicMock(return_value=None)
    decision_no_act = controller.handle_state_change("1", "running")
    assert decision_no_act.action == "noop"


def test_controller_event_checkpoint_final():
    """Controller.py missing lines: 666-702 (checkpoint iteration in event key/id)."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.state_events = []
    controller = MonitorController(monitor, MagicMock())
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))

    event = MagicMock()
    event.name = "checkpoint"
    event.metadata = {"checkpoint_iteration": 5}

    # _event_key with checkpoint
    key = controller._event_key("1", "checkpoint", event.metadata)
    assert key == ("1", "checkpoint:5")

    # _get_or_create_event_record with checkpoint
    record = controller._get_or_create_event_record(state, event)
    assert "checkpoint:5" in record.event_id


def test_controller_render_action_value_recursion():
    """Controller.py missing lines: 723, 726 (recursion in _render_action_value)."""
    controller = MonitorController(MagicMock(), MagicMock())
    context = MagicMock()
    context.render.side_effect = lambda x: f"rendered_{x}"

    # List recursion
    val_list = ["a", "b"]
    res_list = controller._render_action_value(val_list, context)
    assert res_list == ["rendered_a", "rendered_b"]

    # Dict recursion
    val_dict = {"k": "v"}
    res_dict = controller._render_action_value(val_dict, context)
    assert res_dict == {"k": "rendered_v"}


def test_controller_build_job_metadata_defaults():
    """Controller.py missing lines: 466, 471 (metadata default sets)."""
    controller = MonitorController(MagicMock(), MagicMock())
    reg = JobRegistration(
        name="n", script_path="s", log_path="l", inactivity_threshold_seconds=100, output_paths=["o1"]
    )
    state = JobRuntimeState(job_id="1", registration=reg)

    meta = controller._build_job_metadata(state)
    assert meta["inactivity_threshold_seconds"] == 100
    assert meta["output_paths"] == ["o1"]


def test_controller_get_or_create_existing_event():
    """Controller.py missing lines: 522-524 (existing event lookup)."""
    controller = MonitorController(MagicMock(), MagicMock())
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))

    # Pre-populate event
    evt = EventRecord(event_id="e1", name="n1", source="s")
    controller._event_records["e1"] = evt
    controller._event_index[("1", "n1")] = "e1"

    monitor_evt = MagicMock()
    monitor_evt.name = "n1"
    monitor_evt.metadata = {}

    record = controller._get_or_create_event_record(state, monitor_evt)
    assert record.event_id == "e1"
    assert record.count == 2


def test_controller_execute_actions_edge_cases():
    """Controller.py: missing lines 672, 674, 678 (action loop branches)."""
    controller = MonitorController(MagicMock(), MagicMock())

    # Mock evaluate to fail
    controller._evaluate_action_conditions = MagicMock(side_effect=["waiting", "fail"])

    binding = MagicMock(mode="inline")
    event_record = MagicMock()

    # Loop should continue/skip
    res = controller._execute_event_actions(MagicMock(), MagicMock(), event_record, None)
    assert res["results"] == []

    # RuntimeError: queue mode without action queue
    controller._action_queue = None
    binding_q = MagicMock(mode="queue")
    controller._evaluate_action_conditions = MagicMock(return_value="pass")
    monitor_evt = MagicMock(actions=[binding_q])

    with pytest.raises(RuntimeError, match="Action queue not configured"):
        controller._execute_event_actions(MagicMock(), monitor_evt, event_record, None)


def test_submission_manager_extra():
    """submission.py: clear_state and update_job."""
    from monitor.submission import SubmissionManager

    store = MagicMock()
    mgr = SubmissionManager(store)

    mgr.clear_state()
    store.clear.assert_called_once()

    reg = JobRegistration(name="n", script_path="s", log_path="l")
    state = JobRuntimeState(job_id="1", registration=reg)
    mgr.update_job(state)
    assert mgr.get_job("1") == state


def test_event_record_touch_payload():
    """events.py: touch with payload."""
    evt = EventRecord(event_id="e", name="n", source="s")
    evt.touch(payload={"new": "data"})
    assert evt.payload["new"] == "data"
    assert evt.count == 2


def test_get_state_by_name_unknown():
    """utils/states.py: unknown state."""
    from monitor.utils.states import get_state_by_name

    assert get_state_by_name("nonexistent_state") is None


def test_all_states_coverage():
    """states.py: Exercise all state classes and their keys."""
    from monitor.states import (
        SuccessState,
        SuccessStateConfig,
        CrashState,
        CrashStateConfig,
        StalledState,
        StalledStateConfig,
        TimeoutState,
        TimeoutStateConfig,
        StartedState,
        StartedStateConfig,
        UndefinedState,
        UndefinedStateConfig,
        PendingState,
        PendingStateConfig,
        BaseMonitorState,
    )

    states = [
        (SuccessState, SuccessStateConfig, "success"),
        (CrashState, CrashStateConfig, "crash"),
        (StalledState, StalledStateConfig, "stall"),
        (TimeoutState, TimeoutStateConfig, "timeout"),
        (StartedState, StartedStateConfig, "running"),
        (UndefinedState, UndefinedStateConfig, "undefined"),
        (PendingState, PendingStateConfig, "pending"),
    ]

    for cls, cfg_cls, expected_key in states:
        instance = cls(cfg_cls())
        assert instance.key == expected_key

    # BaseMonitorState key with config that has key
    base = BaseMonitorState(SuccessStateConfig())
    assert base.key == "success"


def test_controller_fallback_state_for_final():
    """Controller.py: Exercise _fallback_state_for helper."""
    from monitor.controller import _fallback_state_for
    from monitor.states import StalledState, TimeoutState, CrashState, SuccessState

    assert isinstance(_fallback_state_for("stall"), StalledState)
    assert isinstance(_fallback_state_for("timeout"), TimeoutState)
    assert isinstance(_fallback_state_for("crash"), CrashState)
    assert isinstance(_fallback_state_for("success"), SuccessState)
    assert _fallback_state_for("unknown") is None


def test_controller_evaluate_action_conditions_edge_cases():
    """Controller.py: missing lines 736-748 (evaluate action conditions)."""
    controller = MonitorController(MagicMock(), MagicMock())

    from monitor.event_bindings import EventActionBinding
    from monitor.conditions import ConditionResult

    # 1. Waiting condition
    wait_cond = MagicMock()
    wait_cond.check.return_value = ConditionResult(status="waiting", message="wait")

    binding = MagicMock(conditions=[wait_cond])
    event_record = EventRecord(event_id="e", name="n", source="s")

    res = controller._evaluate_action_conditions(binding, MagicMock(), event_record)
    assert res == "waiting"
    assert event_record.status == EventStatus.PENDING

    # 2. Failing condition
    fail_cond = MagicMock()
    fail_cond.check.return_value = ConditionResult(status="fail", message="err")

    binding_fail = MagicMock(conditions=[fail_cond])
    res_fail = controller._evaluate_action_conditions(binding_fail, MagicMock(), event_record)
    assert res_fail == "fail"
    assert event_record.status == EventStatus.FAILED


def test_controller_handled_continue_branch():
    """Controller.py missing line 225: if handled: continue in observe_once_sync loop."""
    monitor = MagicMock()
    monitor.config = MagicMock()
    monitor.config.poll_interval_seconds = 60
    monitor.config.state_events = []

    # Monitor returns event with state -> handled=True
    from monitor.states import SuccessState, SuccessStateConfig

    monitor.watch_sync.return_value = {
        "1": MonitorOutcome(
            job_id="1",
            status="active",
            last_update_seconds=0,
            metadata={},
            events=[MonitorEvent(job_id="1", name="e", state=SuccessState(SuccessStateConfig()))],
        )
    }

    slurm = MagicMock()
    slurm.squeue.return_value = {"1": "RUNNING"}

    controller = MonitorController(monitor, slurm)
    reg = JobRegistration(name="n", script_path="s", log_path="l")
    controller.register_job("1", reg)
    controller._submission_manager.get_job("1").submitted = True

    # This should trigger 'continue' because event has state
    controller.observe_once_sync()

    # Success state should have been processed and job removed
    assert len(list(controller.jobs())) == 0


def test_controller_maybe_release_event():
    """Controller.py missing lines: 718-723 (maybe_release_event)."""
    controller = MonitorController(MagicMock(), MagicMock())

    # Event record index
    controller._event_index = {("j1", "n1"): "e1"}

    # Processed status
    evt_proc = EventRecord(event_id="e1", name="n1", source="s")
    evt_proc.status = EventStatus.PROCESSED
    evt_proc.metadata = {"job_id": "j1"}

    controller._maybe_release_event("j1", evt_proc)
    assert ("j1", "n1") not in controller._event_index

    # Failed status
    controller._event_index = {("j1", "n1"): "e1"}
    evt_fail = EventRecord(event_id="e1", name="n1", source="s")
    evt_fail.status = EventStatus.FAILED
    evt_fail.metadata = {"job_id": "j1"}

    controller._maybe_release_event("j1", evt_fail)
    assert ("j1", "n1") not in controller._event_index


def test_controller_handle_monitor_event_summary():
    """Controller.py missing lines: 736-749 (handle_monitor_event summary record)."""
    controller = MonitorController(MagicMock(), MagicMock())
    state = JobRuntimeState(job_id="1", registration=JobRegistration(name="n", script_path="s", log_path="l"))

    # MonitorEvent with actions
    from monitor.event_bindings import EventActionBinding
    from monitor.actions import LogAction, LogActionConfig

    action = LogAction(LogActionConfig(message="log"))
    binding = EventActionBinding(action=action, mode="inline", conditions=[])

    event = MonitorEvent(job_id="1", name="e", metadata={}, actions=[binding])
    cycle = MonitorCycleResult()

    controller._handle_monitor_event(state, event, cycle)

    # Verify summary record added to cycle
    # Events: 1 for event itself, 1 for actions summary
    assert len(cycle.events) >= 2
    assert any(e.action == "actions" for e in cycle.events)
