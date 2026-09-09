"""Segment-end rules: ``operator_cancel``, ``sigterm_rollover``, ``time_limit_rollover``.

A job that receives SLURM's ``--signal=TERM@<margin>`` can save a checkpoint and
print its own "exiting after SIGTERM" line. The *same* line follows a deliberate
``scancel`` (SLURM sends SIGTERM first), which SLURM announces in the log as
``*** JOB <id> ON <node> CANCELLED AT <ts> ***``; the wall-clock limit adds
``DUE TO TIME LIMIT``. Telling the three apart is therefore a matter of which
rule matches FIRST: the monitor evaluates ``log_events`` in list order and the
first terminal effect wins.

The rules themselves live in a job policy, not in this library, so they are
built inline here. What is under test is the library behaviour they rely on:
patterns that discriminate between the three shapes, and list-order precedence
in ``_process_log_events``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from monitor.actions import (
    CancelActionConfig,
    LogEvent,
    LogEventConfig,
    RestartActionConfig,
)
from monitor.loop import JobFileStore, JobRecordConfig, JobRuntimeConfig, MonitorLoop
from monitor.submission import LocalJobConfig

# Verbatim shapes from production logs. NB the delivery of `--signal=TERM@N` is
# itself logged as a STEP line, never as a JOB line.
SIGNAL_STEP_LINE = (
    "slurmstepd: error: *** STEP 1691075.0 ON node-008-04 CANCELLED AT 2026-09-06T15:39:18 ***"
)
SCANCEL_JOB_LINE = (
    "srun: forcing job termination\n"
    "slurmstepd: error: *** JOB 1486215 ON node-002-01 CANCELLED AT 2026-08-25T08:21:49 ***"
)
SCANCEL_STEP_LINE = (
    "slurmstepd: error: *** STEP 1486215.0 ON node-002-01 CANCELLED AT 2026-08-25T08:21:49 ***"
)
TIME_LIMIT_LINE = (
    "slurmstepd: error: *** JOB 1537344 ON node-002-01 CANCELLED AT "
    "2026-08-30T22:59:07 DUE TO TIME LIMIT ***"
)
SIGTERM_EXIT_LINE = (
    "[default0]:[exiting program after receiving SIGTERM.] datetime: 2026-09-06 17:57:10 "
)
DURATION_EXIT_LINE = (
    "[default0]:[exiting program after 690.0159624854724 minutes] datetime: 2026-08-25 19:54:10 "
)

# A scancel names the JOB; the wall limit adds DUE TO TIME LIMIT, which must not
# read as an operator cancel.
OPERATOR_CANCEL_PATTERN = r"\*\*\* JOB \d+ ON \S+ CANCELLED AT [\d\-T:]+ \*\*\*"
TIME_LIMIT_PATTERN = r"CANCELLED AT [\d\-T:]+ DUE TO TIME LIMIT"
SIGTERM_PATTERN = "exiting program after receiving SIGTERM."
DURATION_PATTERN = "exiting program after "


def policy() -> list[LogEventConfig]:
    """The rules, in the order a job policy would list them."""
    return [
        LogEventConfig(
            name="operator_cancel",
            pattern=OPERATOR_CANCEL_PATTERN + r"(?! DUE TO TIME LIMIT)",
            pattern_type="regex",
            action=CancelActionConfig(reason="cancelled by an operator"),
        ),
        LogEventConfig(
            name="sigterm_rollover",
            pattern=SIGTERM_PATTERN,
            action=RestartActionConfig(reason="segment ended on SIGTERM"),
        ),
        LogEventConfig(
            name="time_limit_rollover",
            pattern=TIME_LIMIT_PATTERN,
            pattern_type="regex",
            action=RestartActionConfig(reason="wall-clock limit"),
        ),
        LogEventConfig(
            name="exit_duration_rollover",
            pattern=DURATION_PATTERN,
            action=RestartActionConfig(reason="timed exit"),
        ),
    ]


def first_terminal(text: str) -> str | None:
    """Name of the first rule, in policy order, whose pattern matches ``text``."""
    for cfg in policy():
        if LogEvent(cfg).check_triggers(text):
            return cfg.name
    return None


def test_operator_cancel_pattern():
    pat = policy()[0].pattern
    assert re.search(pat, SCANCEL_JOB_LINE)
    assert re.search(pat, TIME_LIMIT_LINE) is None, (
        "the wall-limit line must not read as an operator cancel"
    )
    assert re.search(pat, SCANCEL_STEP_LINE) is None, (
        "only the JOB line is needed; STEP lines follow it"
    )


def test_time_limit_pattern():
    pat = policy()[2].pattern
    assert re.search(pat, TIME_LIMIT_LINE)
    assert re.search(pat, SCANCEL_JOB_LINE) is None


def test_sigterm_pattern_is_exact():
    """The timed exit and the signalled exit share a prefix, not the whole line."""
    assert SIGTERM_PATTERN in SIGTERM_EXIT_LINE
    assert SIGTERM_PATTERN not in DURATION_EXIT_LINE


@pytest.mark.parametrize(
    "text, expected",
    [
        # the wall-clock signal: SLURM's STEP line (its delivery) and the job's line -> rollover
        (SIGTERM_EXIT_LINE, "sigterm_rollover"),
        (SIGNAL_STEP_LINE + "\n" + SIGTERM_EXIT_LINE, "sigterm_rollover"),
        # scancel: SLURM's line and the job's line in the same poll, either order -> stays cancelled
        (SCANCEL_JOB_LINE + "\n" + SIGTERM_EXIT_LINE, "operator_cancel"),
        (SIGTERM_EXIT_LINE + "\n" + SCANCEL_JOB_LINE + "\n" + SCANCEL_STEP_LINE, "operator_cancel"),
        # the wall limit without an exit checkpoint -> rollover from the last complete checkpoint
        (TIME_LIMIT_LINE, "time_limit_rollover"),
        # the timed exit is untouched
        (DURATION_EXIT_LINE, "exit_duration_rollover"),
    ],
)
def test_first_terminal_effect(text, expected):
    assert first_terminal(text) == expected


class _FakeClient:
    def __init__(self) -> None:
        self._statuses: dict[str, str] = {}
        self.cancel_calls: list[str] = []
        self.remove_calls: list[str] = []
        self.submit_calls: list[object] = []
        self._next_id = 100

    def squeue(self) -> dict[str, str]:
        return dict(self._statuses)

    def submit(self, job) -> str:
        self.submit_calls.append(job)
        self._next_id += 1
        job_id = str(self._next_id)
        self._statuses[job_id] = "RUNNING"
        return job_id

    def cancel(self, job_id: str) -> None:
        self.cancel_calls.append(job_id)

    def remove(self, job_id: str) -> None:
        self.remove_calls.append(job_id)


@pytest.mark.parametrize(
    "text, final_state",
    [
        (SCANCEL_JOB_LINE + "\n" + SIGTERM_EXIT_LINE, "cancelled"),
        (SIGNAL_STEP_LINE + "\n" + SIGTERM_EXIT_LINE, None),  # resubmitted, so still active
    ],
)
def test_loop_applies_the_first_matching_rule(tmp_path: Path, text, final_state):
    """List order decides, end to end: a scancel wins over the rollover."""
    store = JobFileStore(tmp_path / "state")
    client = _FakeClient()
    loop = MonitorLoop(store, local_client=client, poll_interval_seconds=0.1)

    log_path = tmp_path / "job_7.log"
    record = JobRecordConfig(
        job_id="job",
        definition=LocalJobConfig(
            name="job",
            command=["true"],
            log_path=str(tmp_path / "job_%j.log"),
            log_events=policy(),
        ),
        runtime=JobRuntimeConfig(submitted=True, runtime_job_id="7"),
    )
    client._statuses["7"] = "RUNNING"
    store.upsert(record)

    log_path.write_text(text + "\n", encoding="utf-8")
    loop.observe_once()

    loaded = store.load("job", include_finished=True)
    assert loaded is not None
    assert loaded.runtime.final_state == final_state
