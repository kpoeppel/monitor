# Monitor Specification

## Goals
- Monitor jobs via log patterns and execute actions inline.
- Keep state recoverable with a simple, inspectable layout.
- Support local and SLURM backends with minimal configuration surface.

## Core Concepts

### MonitorLoop (`src/monitor/loop.py`)
- Synchronous loop; each cycle:
  - Loads all `*.job.json` files from the state directory.
  - Evaluates start/cancel/finish conditions.
  - Submits or cancels jobs via a `JobClientProtocol`.
  - Scans log files for `LogEventConfig` matches.
  - Executes actions inline.

### JobFileStore
- One file per job (`{job_id}.job.json`) in `state_dir`.
- Each file stores a `JobRecordConfig`:
  - `registration`: static job definition.
  - `runtime`: attempts, job id, cursor, action condition state, etc.

### JobRegistrationConfig (`src/monitor/submission.py`)
- Per-job configuration:
  - `name`, `command`, `log_path`, `log_path_current`
  - `log_events` (list of `LogEventConfig`)
  - `start_condition`, `cancel_condition`, `finish_condition`
  - `slurm` (optional)

### LogEventConfig (`src/monitor/actions.py`)
- Defines a log pattern and a single action to execute.
- Includes optional action conditions via `conditions`.

### Actions (`src/monitor/actions.py`)
- `LogAction`, `RunCommandAction`, `RestartAction`, `DuplicateAction`,
  `CancelAction`, `FinishAction`.
- Restart/duplicate actions emit adjustments for re-submission.
- Backend-specific behavior is gated via `backend_config` and `job_kind`.

### Conditions (`src/monitor/conditions.py`)
- Always boolean (`passed: bool`).
- Use `TimeoutCondition` for deadline gating.
- `persistent_pass` / `persistent_fail` latch outcomes per action.

## Log Paths
- `log_path` may include `%j` (job id) and `%t` (submission timestamp).
- `log_path_current` is a stable symlink updated on submission.

## State Layout
- `state_dir/{job_id}.job.json`
- No separate event or queue folders.

## YAML Shape (Job)

```yaml
job_id: "job1"
registration:
  class_name: LocalJobRegistration
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
```

## Backends
- `LocalCommandClient` executes processes locally.
- `SlurmGenClient` renders sbatch scripts via `slurm_gen` and submits with a
  configured SLURM client.
