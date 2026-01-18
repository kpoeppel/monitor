# Improvement Plan

This file tracks remaining improvements after collapsing the architecture into
`MonitorLoop` with per-job event/action configs.

## Done

- Removed controller/watcher/executor/action-queue layers.
- Simplified persistence to one job file per job.
- Enforced boolean-only conditions with persistence flags.
- Added backend-gated actions via `backend_config`.
- Updated examples and docs to the new MonitorLoop flow.

## Next

- Rebuild tests around job files + MonitorLoop (unit + integration).
- Add integration tests for `monitor_control.py` and `monitor_status.py`.
- Expand YAML examples for restart/duplicate + slurm_gen overrides.
