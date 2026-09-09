"""Checkpoint hook: a ``checkpoint_saved`` log event runs a command when a
persistent checkpoint's iteration is a multiple of some interval.

The pieces this needs from the library are ``IterationMultipleCondition`` (fire
only every Nth checkpoint -- evaluating each one is usually too much),
``RunCommandAction`` (a side effect that is never terminal), and named-group
extraction so the command can be templated with what the log line said. The
policy that wires them together lives in a job config, so the event is built
inline here.
"""

from __future__ import annotations

import re

from monitor.actions import (
    ActionContext,
    EventRecord,
    LogEvent,
    LogEventConfig,
    RunCommandAction,
    RunCommandActionConfig,
)
from monitor.conditions import (
    ConditionContext,
    IterationMultipleCondition,
    IterationMultipleConditionConfig,
)

# Only the persistent tree, never the rolling one: the rolling checkpoints are
# overwritten and would make the hook fire on state that is already gone.
PATTERN = (
    r"successfully saved checkpoint from iteration\s+(?P<iteration>\d+) "
    r"to (?P<ckpt_dir>\S*/checkpoints)(?![\w-])"
)
EXTRACT_GROUPS = {"iteration": "iteration", "ckpt_dir": "ckpt_dir"}

PERSISTENT = (
    "[default0]:  [2026-09-06 11:19:24.346793] successfully saved checkpoint from "
    "iteration   80000 to /scratch/run/training_ckpts/checkpoints in torch_dist format"
)
ROLLING = (
    "[default0]:  [2026-09-06 15:23:00.1] successfully saved checkpoint from "
    "iteration   83375 to /scratch/run/training_ckpts/checkpoints_rolling in torch_dist format"
)


def test_pattern_matches_persistent_saves_only():
    m = re.search(PATTERN, PERSISTENT)
    assert m and m.group("iteration") == "80000"
    assert m.group("ckpt_dir").endswith("/training_ckpts/checkpoints")
    assert re.search(PATTERN, ROLLING) is None


def test_log_event_extracts_the_groups():
    cfg = LogEventConfig(
        name="checkpoint_saved",
        pattern=PATTERN,
        pattern_type="regex",
        extract_groups=EXTRACT_GROUPS,
        action=RunCommandActionConfig(command="x"),
    )
    trig = LogEvent(cfg).check_triggers(PERSISTENT + "\n" + ROLLING)
    assert len(trig) == 1
    assert trig[0]["iteration"] == "80000"
    assert trig[0]["ckpt_dir"].endswith("/checkpoints")


def test_iteration_multiple_condition():
    c = IterationMultipleCondition(IterationMultipleConditionConfig(every=4000))
    assert c.check(ConditionContext(job_metadata={"iteration": "80000"})).passed
    assert not c.check(ConditionContext(job_metadata={"iteration": "86000"})).passed
    assert not c.check(ConditionContext(job_metadata={})).passed
    assert (
        not IterationMultipleCondition(IterationMultipleConditionConfig(every=0))
        .check(ConditionContext(job_metadata={"iteration": "80000"}))
        .passed
    )


def test_run_command_is_templated_with_iteration_and_ckpt_dir(tmp_path):
    out = tmp_path / "hook.txt"
    act = RunCommandAction(
        RunCommandActionConfig(command=f"echo {{iteration}} {{ckpt_dir}} > {out}", timeout_s=30)
    )
    ev = EventRecord(
        event_id="e",
        name="checkpoint_saved",
        source="log",
        payload={"iteration": "84000", "ckpt_dir": "/x/checkpoints"},
        metadata={"job_id": "j"},
    )
    res = act.execute(ActionContext(event=ev, job_metadata={}))
    assert res.status == "success" and out.read_text().strip() == "84000 /x/checkpoints"
