# Monitor Specification

## Goals
- Provide a reliable state machine for long-running jobs.
- Decouple monitoring logic from job submission.
- Support resume/recovery via persistence.

## Components

### Controller (`controller.py`)
- Main entry point.
- Coordinates between `BaseMonitor` (log watcher) and `BaseSlurmClient` (queue checker).
- Manages the lifecycle of `JobRuntimeState`.

### Watcher (`watcher.py`)
- Generic file-based monitoring.
- Can check for specific patterns in log files.
- Log/inactivity/state events are configured per job via `JobRegistration`. Monitor config only carries the poll interval.

### Action Queue (`action_queue.py`)
- File-backed queue for deferred action execution.
- Stores one JSON file per queued action under event-named directories.
- Status lifecycle: `pending` -> `running` -> `done`/`failed`.
- Recovery: on startup, `running` entries are reset to `pending` for reprocessing.

### Persistence (`persistence/`)
- `MonitorStateStore`: Saves job state to disk (JSON/SQLite).
- Uses atomic writes to avoid partial state files on crash.
- Schema versioning: records include `schema_version` (current: `1`); unknown versions should be treated as best-effort compatible.

### Actions (`actions.py`)
- Triggerable actions like "stop", "restart", "email".
- `RestartAction` may include `adjustments` (e.g., `command`, `log_path`, `extra_args`) applied before resubmission.
- `DuplicateAction` requests a new job instance with optional adjustments; duplicate jobs should use unique `log_path`.

### Log Paths
- `log_path` may be a template (e.g., `%j`) and is not overwritten on resubmission.
- Local execution supports `%t` for a submission timestamp, resolved when the command starts.
- `log_path_current` is a stable symlink path updated on submission to point at the resolved `log_path`.
