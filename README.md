# Monitor

Monitor is a lightweight job monitor that watches logs and executes actions inline
based on per-job event rules. It stores one JSON file per job in a state directory
so a single monitor loop can be restarted safely.

## Architecture

- **MonitorLoop**: Synchronous loop that loads job files, evaluates conditions,
  submits/cancels jobs, and executes actions inline.
- **JobFileStore**: One `.job.json` file per job registration inside a state dir.
- **JobConfig**: Per-job config (command, log paths, conditions, log events).
- **LogEventConfig**: Log pattern + action + action conditions.
- **Actions**: `LogAction`, `RunCommandAction`, `RestartAction`, `DuplicateAction`,
  `CancelAction`, `FinishAction`.
- **Clients**: `LocalCommandClient` for local execution, `SlurmJobClient` for SLURM
  with external `slurm_gen` script rendering and submission clients.

## Features

- Per-job log pattern matching with inline actions.
- Restart/duplicate actions with explicit adjustments.
- Start/cancel/finish conditions on each job.
- Persistent condition states (e.g., latch once a file appears).
- Resume from a state directory (one job file per job).

## Usage (Python)

```python
from monitor import LocalCommandClient
from monitor.actions import LogActionConfig, RestartActionConfig, LocalActionBackendConfig
from monitor.actions import LogEventConfig
from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop
from monitor.submission import LocalJobConfig

store = JobFileStore("./state")
client = LocalCommandClient()
loop = MonitorLoop(store, client, poll_interval_seconds=2)

store.upsert(
    JobRecordConfig(
        job_id="train-1",
        registration=LocalJobConfig(
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
                        extra_args_append=["--retry={attempts}"],
                        backend_config=LocalActionBackendConfig(),
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
            backend_config:
              class_name: LocalActionBackend
        - class_name: LogEvent
          name: duplicate
          pattern: "DUPLICATE_JOB"
          action:
            class_name: DuplicateAction
            name_suffix: "-copy"
            backend_config:
              class_name: LocalActionBackend
```

Run:

```bash
python scripts/run_monitor.py --config examples/monitor_app.yaml
```

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
- `log_path_current` is a stable path (symlink) updated on submission.

Note: For SLURM jobs that use `slurm_gen`, the job-level `slurm` block must include
`template_path`, `script_dir`, and `log_dir` because it is parsed as a full `SlurmConfig`.

## Conditions

Conditions return boolean `passed` only; no blocking/wait states. Use:

- `TimeoutCondition` to enforce deadlines (`True` before timeout, `False` after).
- `persistent_pass` / `persistent_fail` to latch condition results.

## SLURM (slurm_gen)

Use `SlurmJobClient` to render scripts and submit through SLURM:

```yaml
client:
  class_name: SlurmJobClient
  output_dir: "./slurm_out"
  slurm:
    template_path: "./templates/job.sbatch"
    script_dir: "./slurm_out/scripts"
    log_dir: "./slurm_out/logs"
    command: ["python", "train.py", "--profile=fast"]
    sbatch:
      gres: "gpu:1"
  slurm_client:
    class_name: SlurmClient
```

Ensure `slurm_gen` is installed (or on `PYTHONPATH`) for SLURM usage.
