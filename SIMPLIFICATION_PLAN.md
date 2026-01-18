# Simplification Plan

This plan tracks the simplifications that collapse the monitor into a single
loop with per-job event/action configs and a minimal persistence layout.

## Completed

- Removed controller/watcher/executor/action-queue layers in favor of `MonitorLoop`.
- Consolidated persistence into one file per job (`{job_id}.job.json`).
- Made conditions boolean-only with optional persistence (`persistent_pass` / `persistent_fail`).
- Moved event+action definitions into per-job `LogEventConfig`.
- Unified action types across backends with `backend_config` gating.

## Remaining

- Rebuild test suite for the new MonitorLoop + job file layout.
- Add integration tests for scripts (`monitor_control.py`, `monitor_status.py`).
- Expand examples to cover restart/duplicate + slurm_gen adjustments.
