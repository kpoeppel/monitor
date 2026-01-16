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

### Persistence (`persistence/`)
- `MonitorStateStore`: Saves job state to disk (JSON/SQLite).

### Actions (`actions.py`)
- Triggerable actions like "stop", "restart", "email".
