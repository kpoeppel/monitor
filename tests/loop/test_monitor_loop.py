from __future__ import annotations

import json
import time
from pathlib import Path

from monitor.actions import (
    DuplicateActionConfig,
    FinishActionConfig,
    CancelActionConfig,
    LocalActionBackendConfig,
    RestartActionConfig,
    LogActionConfig,
    LogEventConfig,
)
from monitor.conditions import AlwaysTrueConditionConfig, FileExistsConditionConfig, CooldownConditionConfig
from monitor.conditions import TimeoutConditionConfig, CompositeConditionConfig
from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop
from monitor.submission import LocalJobRegistrationConfig


class FakeClient:
    def __init__(self) -> None:
        self.submit_calls: list[tuple] = []
        self.cancel_calls: list[str] = []
        self.remove_calls: list[str] = []
        self._counter = 0
        self._statuses: dict[str, str] = {}

    def submit(
        self,
        name,
        command,
        log_path,
        extra_args=None,
        log_to_file=None,
        log_path_current=None,
        slurm=None,
    ) -> str:
        self._counter += 1
        job_id = f"job-{self._counter}"
        self.submit_calls.append(
            (name, list(command), log_path, list(extra_args or []), log_to_file, log_path_current, slurm)
        )
        self._statuses[job_id] = "RUNNING"
        return job_id

    def submit_array(
        self,
        array_name,
        command,
        log_paths,
        task_names,
        extra_args=None,
        start_index=None,
        log_to_file=None,
        log_path_current=None,
        slurm=None,
    ):
        raise NotImplementedError

    def cancel(self, job_id: str) -> None:
        self.cancel_calls.append(job_id)
        self._statuses[job_id] = "CANCELLED"

    def remove(self, job_id: str) -> None:
        self.remove_calls.append(job_id)
        self._statuses.pop(job_id, None)

    def squeue(self) -> dict[str, str]:
        return dict(self._statuses)


def _write_log(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_monitor_loop_start_condition(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    gate = tmp_path / "ready.flag"
    record = JobRecordConfig(
        job_id="job1",
        registration=LocalJobRegistrationConfig(
            name="job1",
            command=["echo", "hi"],
            log_path=str(tmp_path / "job1_%j.log"),
            start_condition=FileExistsConditionConfig(path=str(gate)),
        ),
    )
    store.upsert(record)

    loop.observe_once()
    loaded = store.load("job1")
    assert loaded is not None
    assert loaded.runtime.submitted is False

    gate.write_text("ready", encoding="utf-8")
    loop.observe_once()
    loaded = store.load("job1")
    assert loaded is not None
    assert loaded.runtime.submitted is True
    assert client.submit_calls


def test_monitor_loop_cancel_condition(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    record = JobRecordConfig(
        job_id="job2",
        registration=LocalJobRegistrationConfig(
            name="job2",
            command=["echo", "bye"],
            log_path=str(tmp_path / "job2_%j.log"),
            cancel_condition=AlwaysTrueConditionConfig(),
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-99"
    client._statuses["job-99"] = "RUNNING"
    store.upsert(record)

    loop.observe_once()
    assert store.load("job2") is None
    assert client.cancel_calls == ["job-99"]
    assert client.remove_calls == ["job-99"]


def test_monitor_loop_restart_action(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    record = JobRecordConfig(
        job_id="job3",
        registration=LocalJobRegistrationConfig(
            name="job3",
            command=["echo", "run"],
            log_path=str(tmp_path / "job3_%j.log"),
            log_events=[
                LogEventConfig(
                    name="oom",
                    pattern="OOM",
                    action=RestartActionConfig(
                        reason="oom",
                        extra_args_append=["--retry={attempts}"],
                        backend_config=LocalActionBackendConfig(),
                    ),
                )
            ],
        ),
    )
    store.upsert(record)

    loop.observe_once()
    loaded = store.load("job3")
    assert loaded is not None
    job_id = loaded.runtime.runtime_job_id
    assert job_id is not None

    log_path = tmp_path / f"job3_{job_id}.log"
    _write_log(log_path, "OOM\n")

    loop.observe_once()
    loaded = store.load("job3")
    assert loaded is not None
    assert len(client.submit_calls) == 2
    assert client.cancel_calls == [job_id]
    assert loaded.registration.extra_args == ["--retry=1"]


def test_monitor_loop_duplicate_action(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    log_current = tmp_path / "job4_latest.log"
    record = JobRecordConfig(
        job_id="job4",
        registration=LocalJobRegistrationConfig(
            name="job4",
            command=["echo", "dup"],
            log_path=str(tmp_path / "job4_%j.log"),
            log_path_current=str(log_current),
            log_events=[
                LogEventConfig(
                    name="dup",
                    pattern="DUP",
                    action=DuplicateActionConfig(
                        name_suffix="-copy",
                        backend_config=LocalActionBackendConfig(),
                    ),
                )
            ],
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-1"
    client._statuses["job-1"] = "RUNNING"
    store.upsert(record)

    _write_log(log_current, "DUP\n")
    loop.observe_once()

    assert store.load("job4") is not None
    duplicate = store.load("job4-copy")
    assert duplicate is not None
    assert duplicate.registration.name.endswith("-copy")
    assert duplicate.registration.log_path.endswith("-copy.log")


def test_monitor_loop_finish_condition(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    finished = tmp_path / "done.txt"
    record = JobRecordConfig(
        job_id="job5",
        registration=LocalJobRegistrationConfig(
            name="job5",
            command=["echo", "done"],
            log_path=str(tmp_path / "job5_%j.log"),
            finish_condition=FileExistsConditionConfig(path=str(finished)),
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-5"
    client._statuses["job-5"] = "RUNNING"
    store.upsert(record)

    loop.observe_once()
    assert store.load("job5") is not None

    finished.write_text("done", encoding="utf-8")
    loop.observe_once()
    assert store.load("job5") is None
    assert client.remove_calls == ["job-5"]


def test_monitor_loop_cancel_action(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    log_current = tmp_path / "job6_latest.log"
    record = JobRecordConfig(
        job_id="job6",
        registration=LocalJobRegistrationConfig(
            name="job6",
            command=["echo", "cancel"],
            log_path=str(tmp_path / "job6_%j.log"),
            log_path_current=str(log_current),
            log_events=[
                LogEventConfig(
                    name="cancel",
                    pattern="CANCEL",
                    action=CancelActionConfig(
                        reason="user_cancel",
                        backend_config=LocalActionBackendConfig(),
                    ),
                )
            ],
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-6"
    client._statuses["job-6"] = "RUNNING"
    store.upsert(record)

    _write_log(log_current, "CANCEL\n")
    loop.observe_once()

    assert store.load("job6") is None
    assert client.cancel_calls == ["job-6"]
    assert client.remove_calls == ["job-6"]


def test_monitor_loop_log_path_current_used(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    log_current = tmp_path / "job7_latest.log"
    record = JobRecordConfig(
        job_id="job7",
        registration=LocalJobRegistrationConfig(
            name="job7",
            command=["echo", "finish"],
            log_path=str(tmp_path / "job7_%j.log"),
            log_path_current=str(log_current),
            log_events=[
                LogEventConfig(
                    name="finish",
                    pattern="FINISH",
                    action=FinishActionConfig(
                        reason="done",
                        backend_config=LocalActionBackendConfig(),
                    ),
                )
            ],
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-7"
    client._statuses["job-7"] = "RUNNING"
    store.upsert(record)

    _write_log(log_current, "FINISH\n")
    loop.observe_once()
    assert store.load("job7") is None


def test_monitor_loop_action_conditions(tmp_path: Path, monkeypatch) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    log_current = tmp_path / "job8_latest.log"
    record = JobRecordConfig(
        job_id="job8",
        registration=LocalJobRegistrationConfig(
            name="job8",
            command=["echo", "cond"],
            log_path=str(tmp_path / "job8_%j.log"),
            log_path_current=str(log_current),
            log_events=[
                LogEventConfig(
                    name="cool",
                    pattern="COOL",
                    action=LogActionConfig(message="cool"),
                    conditions=[CooldownConditionConfig(cooldown_seconds=60)],
                )
            ],
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-8"
    client._statuses["job-8"] = "RUNNING"
    store.upsert(record)

    _write_log(log_current, "COOL\n")
    loop.observe_once()
    state = store.load("job8")
    assert state is not None
    action_state = state.runtime.action_state["log:cool:LogAction:0"]
    last_ts = action_state["last_action_ts"]

    monkeypatch.setattr(time, "time", lambda: last_ts + 1)
    _write_log(log_current, "COOL\n")
    loop.observe_once()
    action_state = store.load("job8").runtime.action_state["log:cool:LogAction:0"]
    assert action_state["last_action_ts"] == last_ts


def test_monitor_loop_persistent_fail(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    log_current = tmp_path / "job9_latest.log"
    record = JobRecordConfig(
        job_id="job9",
        registration=LocalJobRegistrationConfig(
            name="job9",
            command=["echo", "cond"],
            log_path=str(tmp_path / "job9_%j.log"),
            log_path_current=str(log_current),
            log_events=[
                LogEventConfig(
                    name="timeout",
                    pattern="TIMEOUT",
                    action=LogActionConfig(message="timeout"),
                    conditions=[TimeoutConditionConfig(timeout_seconds=0.0, persistent_fail=True)],
                )
            ],
        ),
    )
    record.runtime.submitted = True
    record.runtime.runtime_job_id = "job-9"
    client._statuses["job-9"] = "RUNNING"
    store.upsert(record)

    _write_log(log_current, "TIMEOUT\n")
    loop.observe_once()
    state = store.load("job9")
    assert state is not None
    condition_state = state.runtime.action_state["log:timeout:LogAction:0"]["conditions"]["0"]
    assert condition_state["latched_fail"] is True


def test_monitor_loop_composite_condition(tmp_path: Path) -> None:
    store = JobFileStore(tmp_path / "state")
    client = FakeClient()
    loop = MonitorLoop(store, client, poll_interval_seconds=0.1)

    gate = tmp_path / "ready.flag"
    record = JobRecordConfig(
        job_id="job10",
        registration=LocalJobRegistrationConfig(
            name="job10",
            command=["echo", "go"],
            log_path=str(tmp_path / "job10_%j.log"),
            start_condition=CompositeConditionConfig(
                mode="all",
                conditions=[
                    FileExistsConditionConfig(path=str(gate)),
                    AlwaysTrueConditionConfig(),
                ],
            ),
        ),
    )
    store.upsert(record)
    loop.observe_once()
    assert store.load("job10").runtime.submitted is False
    gate.write_text("ok", encoding="utf-8")
    loop.observe_once()
    assert store.load("job10").runtime.submitted is True
