from __future__ import annotations

from monitor.submission import LocalJobConfig


def test_local_job_config_array_len_defaults() -> None:
    job = LocalJobConfig(
        name="job",
        command=["echo", "hi"],
        log_path="job_%j.log",
    )
    assert job.array_len == 1


def test_local_job_config_array_len_from_array_args() -> None:
    job = LocalJobConfig(
        name="job",
        command=["echo", "hi"],
        log_path="job_%j.log",
        array_args=[["--shard=0"], ["--shard=1"]],
    )
    assert job.array_len == 2
