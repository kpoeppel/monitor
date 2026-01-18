import time

from monitor.condition_evaluator import ConditionEvaluator
from monitor.conditions import AlwaysTrueConditionConfig
from monitor.submission import JobRegistration, JobRuntimeState


def test_condition_evaluator_sets_started_ts(monkeypatch):
    t0 = 1234.0
    t1 = 4567.0
    monkeypatch.setattr(time, "time", lambda: t0)

    reg = JobRegistration(name="job", command=["s.sh"], log_path="job.log")
    state = JobRuntimeState(job_id="job-1", registration=reg)
    evaluator = ConditionEvaluator(lambda _: {})

    result = evaluator.evaluate(state, AlwaysTrueConditionConfig(), label="start")
    assert result.passed
    assert "started_ts" not in state.condition_data["start"]

    monkeypatch.setattr(time, "time", lambda: t1)
    evaluator.evaluate(state, AlwaysTrueConditionConfig(), label="start")
    assert "started_ts" not in state.condition_data["start"]


def test_condition_evaluator_timeout(monkeypatch):
    reg = JobRegistration(name="job", command=["s.sh"], log_path="job.log")
    state = JobRuntimeState(job_id="job-1", registration=reg)
    evaluator = ConditionEvaluator(lambda _: {})
    from monitor.conditions import TimeoutConditionConfig

    monkeypatch.setattr(time, "time", lambda: 10.0)
    evaluator.evaluate(state, TimeoutConditionConfig(timeout_seconds=1.0), label="start")
    assert state.condition_data["start"]["started_ts"] == 10.0
    monkeypatch.setattr(time, "time", lambda: 20.0)
    result = evaluator.evaluate(state, TimeoutConditionConfig(timeout_seconds=1.0), label="start")
    assert not result.passed
    assert "timeout" in result.message
