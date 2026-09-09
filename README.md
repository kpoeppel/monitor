# Monitor

Monitor is a lightweight job monitor that watches logs and executes actions inline
based on per-job event rules. It stores one JSON file per job in a state directory
so a single monitor loop can be restarted safely.

## Architecture

- **MonitorLoop**: Synchronous loop that loads job files, evaluates conditions,
  submits/cancels jobs, and executes actions inline.
- **JobFileStore**: One `.job.json` file per job record inside a state dir.
- **JobRecordConfig**: Per-job record with a `definition` (job config) and runtime state.
- **LogEventConfig**: Log pattern + action + action conditions.
- **Actions**: `LogAction`, `NewJobAction`, `RestartAction`, `CancelAction`, `FinishAction`.
- **Clients**: `LocalCommandClient` for local execution, `SlurmClient` for SLURM
  with external `slurm_gen` script rendering and submission clients.

## Features

- Per-job log pattern matching with inline actions.
- Restart/cancel/finish actions triggered from log events.
- Start/cancel/finish conditions on each job.
- Persistent condition states (e.g., latch once a file appears).
- Resume from a state directory (one job file per job), including re-attaching
  to a session this monitor did not submit (`MonitorLoop.rehydrate()`).
- Streak-based health checks: **inactivity** (the log stopped growing) and
  **progress** (a counter in the log stopped advancing).
- Restart hooks: run a command before a resubmission, refresh the node-exclusion
  list, or defer the restart until the job has actually left the queue.
- Per-event action budgets, so one event's limit does not silently disable
  another's.

## Usage (Python)

```python
from monitor import LocalCommandClient
from monitor.actions import LogActionConfig, RestartActionConfig
from monitor.actions import LogEventConfig
from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop
from monitor.submission import LocalJobConfig

store = JobFileStore("./state")
client = LocalCommandClient()
loop = MonitorLoop(store, local_client=client, poll_interval_seconds=2)

store.upsert(
    JobRecordConfig(
        job_id="train-1",
        definition=LocalJobConfig(
            name="train-1",
            command=["bash", "./train.sh"],
            log_path="./train_%t.log",
            log_path_current="./train_latest.log",
            log_events=[
                LogEventConfig(
                    name="oom",
                    pattern="CUDA out of memory",
                    action=RestartActionConfig(
                        reason="oom",
                    ),
                ),
                LogEventConfig(
                    name="ready",
                    pattern="READY",
                    action=LogActionConfig(message="job {job_name} ready"),
                ),
            ],
        ),
    )
)

while store.load("train-1"):
    loop.observe_once()
```

## YAML App Config

Run with `scripts/run_monitor.py`:

```yaml
monitor:
  class_name: MonitorLoop
  poll_interval_seconds: 2

state_store_dir: "./state"
client:
  class_name: LocalCommandClient

jobs:
  - job_id: job1
    registration:
      class_name: LocalJob
      name: job1
      command: ["bash", "./job1.sh"]
      log_path: "./logs/job1_%t.log"
      log_path_current: "./logs/job1_latest.log"
      log_events:
        - class_name: LogEvent
          name: oom
          pattern: "CUDA out of memory"
          action:
            class_name: RestartAction
            reason: "oom"
        - class_name: LogEvent
          name: duplicate
          pattern: "DUPLICATE_JOB"
          action:
            class_name: LogAction
            message: "duplicate requested"
```

Run:

```bash
python scripts/run_monitor.py --config examples/monitor_app.yaml
```

Note: app configs define job templates, but they are not automatically inserted
into the state store. Use `monitor_control.py` (below) or create job records
yourself to enqueue work.

## Control/Status Utilities

```bash
python scripts/monitor_status.py --state-dir ./state
python scripts/monitor_control.py --state-dir ./state submit --job-json ./job.json
python scripts/monitor_control.py --state-dir ./state submit --job-yaml ./job.yaml
python scripts/monitor_control.py --state-dir ./state cancel --job-id job1
```

Cleanup completed jobs:

```bash
python scripts/monitor_cleanup.py --state-dir ./state --done-only
```

Close out sessions whose monitor was killed before it could write `final_state`
(resolved against `sacct`, dry run by default, and never touching a session with
a job still in the queue):

```bash
python scripts/retire_sessions.py --state-dir ./state          # dry run
python scripts/retire_sessions.py --state-dir ./state --apply
```

Validate a YAML config:

```bash
python scripts/check_config.py --config examples/monitor_app.yaml
```

## Testing

```bash
pytest
```

Example config parsing (no job execution) is covered by `tests/examples/test_example_configs.py`.

Cleanup is covered by `tests/scripts/test_monitor_scripts.py` (invokes `monitor_cleanup.py`).

## Log Paths

- `log_path` can include `%j` (job id) or `%t` (submission timestamp).
- `log_path_current` is a stable path (symlink) that the monitor re-points at a
  job when that job enters `RUNNING`.
- `config_path` / `config_path_current` do the same for a YAML dump of the job's
  `base_config`, written when the job is submitted.
- For arrays, `%A` is the array job id and `%a` is the task index.

**The monitor reads each job's OWN log**, never `log_path_current`. In a
dependency chain all jobs typically share one output directory, so reading
through a shared `current` symlink would make every job react to whichever job
happens to be running. The `current.*` symlinks are a tailing convenience,
maintained separately on the `RUNNING` transition.

Note: For SLURM jobs that use `slurm_gen`, the job-level `slurm` block must include
`template_path`, `script_dir`, and `log_dir` because it is parsed as a full `SlurmConfig`.

## Conditions

Conditions return boolean `passed` only; no blocking/wait states. Use:

- `TimeoutCondition` to enforce deadlines (`True` before timeout, `False` after).
- `persistent_pass` / `persistent_fail` to latch condition results.
- `MaxAttemptsCondition` caps the JOB-WIDE restart count; `MaxActionFiresCondition`
  caps how often one event's own action may run. The distinction matters on a
  long chained run: the job-wide counter is dominated by healthy wall-clock
  rollovers, so a `MaxAttemptsCondition: 4` meant as "give up after 4 stalls"
  actually disables itself a few segments in.
- `IterationMultipleCondition` fires only every Nth iteration, for hooks that
  should run on some checkpoints but not all.

## Health Checks

Two `pattern_type`s watch for a job that is stuck rather than for a line it
printed. Both accumulate a streak across polls and fire only once
`*_polls` **and** `*_timeout_s` are both satisfied.

- `inactivity` — the log did not grow. Any new output resets the streak.
- `progress` — a number captured from the log did not advance. `pattern` is
  always a regex and `progress_group` names the capturing group.

They answer different questions, and `progress` is the one that catches a job
that is **dead but noisy**: a launcher restart loop keeps emitting fresh setup
banners, so it is never inactive, but its iteration counter does not move.

`progress_mode` decides what counts as movement:

- `any_change` (default) — any different value, including the backwards jump of
  a resume from checkpoint. So a healthy restart can never trip it. Answers "is
  the training loop emitting iterations at all?"
- `furthest` — only a value higher than any seen before, so replaying work the
  run has already done is not progress. This additionally catches a restart loop
  that never reaches new ground, and its record deliberately survives a restart
  (minus the time spent waiting in the queue). Its window must exceed the time
  needed to redo the work lost to the last checkpoint.

## Restart Hooks

`RestartAction` can do more than resubmit:

- `pre_command` runs before the resubmission (e.g. a node-fault scan of the
  failed job's log), templated with the action context plus `{runtime_job_id}`,
  `{log_path}` and `{log_dir}`. Its exit code is logged, never fatal.
- `exclude_file` re-reads a node-exclusion list right before the resubmission,
  so the re-rendered sbatch carries every node excluded since plan time.
- `wait_for_job_end` defers the resubmission until the job has left the queue
  instead of cancelling it. Use it for a graceful segment end: the job prints its
  "exiting" line *before* an async checkpoint write completes, so cancelling on
  that line cuts the very checkpoint the next segment should load.
- `cancel_first` cancels now, then waits for the job to leave the queue before
  the hooks run. Use it for hangs and faults: the log is then complete, so a scan
  sees the lines written after the kill — often the ones that name the bad node.

## SLURM (slurm_gen)

Use `SlurmClient` to render scripts and submit through SLURM:

```yaml
slurm_client:
  class_name: SlurmClient
  base_client:
    class_name: SlurmClient
```

`base_client` refers to the slurm_gen client implementation (e.g., `SlurmClient`
or `FakeSlurmClient`).

Ensure `slurm_gen` is installed (or on `PYTHONPATH`) for SLURM usage.

## Array Jobs

Array jobs are supported at the client layer. When a job definition has
`array_len > 1` (or `array_args` for local jobs), MonitorLoop will submit an
array and split it into per-task job records with `job_id` suffixes like
`job1_0`, `job1_1`, etc.

Local arrays (per-task args and %a in log paths):

```python
from monitor import LocalCommandClient
from monitor.submission import LocalJobConfig

client = LocalCommandClient()
job_ids = client.submit_array(
    LocalJobConfig(
        name="train-array",
        command=["bash", "./train.sh"],
        log_path="./logs/train_%t_%a.log",
        log_path_current="./logs/train_latest_%a.log",
        array_args=[["--shard=0"], ["--shard=1"]],
    ),
    indices=[0, 1],
)
```

SLURM arrays (manual submission via slurm_gen):

```python
from monitor.slurm_client import SlurmClient
from slurm_gen import SlurmConfig

client = SlurmClient()
job_ids = client.submit_array(
    SlurmConfig(
        template_path="./templates/job.sbatch",
        script_dir="./slurm_out/scripts",
        log_dir="./slurm_out/logs",
        command=["python", "train.py"],
        array=True,
    ),
    indices=[0, 1, 2],
)
```

For array log paths, use `%A` (array job id) and `%a` (task index). When using
`log_path_current`, include `%a` so each task gets its own stable symlink.
