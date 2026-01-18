# Structure Complexity Review

## High-Level Structure (Summary)
- `MonitorLoop` loads job files from a state directory and evaluates start/cancel/finish conditions.
- Log events are configured per job and executed inline as Event+Action pairs.
- State is stored as one JSON file per job (`{job_id}.job.json`) for easy cleanup.
- Separate clients exist for local execution and SLURM submission; slurm_gen is only for script generation.

## Unnecessary Complexity / Potential Simplifications

- **Event/action layering**: We still have `LogEventConfig` + `EventActionConfig` + `EventActionBinding` + `BaseMonitorAction`. This is flexible but deep; a single `EventActionSpec` could reduce indirection.
- **Mixed config parsing paths**: Monitor config parsing is separate from job registration parsing; a single entry point would be simpler.
- **Job duplication + restart adjustments**: Multiple adjustment knobs (`extra_args`, `extra_args_append`, `log_path_current`, `slurm`) are flexible but harder to reason about.
- **Local vs Slurm differences**: Shared action types rely on runtime checks. More explicit backend subtypes would reduce ambiguity.
- **Path semantics**: `log_path` templates + `log_path_current` symlink + local `%t` expansions are powerful but can be hard to reason about.

## Potentially Unavoidable Complexity

- **Cross‑backend support**: Supporting local execution and SLURM requires some divergence (clients, submission logic, log handling).
- **Recovery & resumability**: Persisting state + event history is essential for crash recovery and requires structured storage.

## Implementation Plan (Proposed)

1. **Unify event/action into a single spec** ✅
   - Replace `LogEventConfig` + `EventActionConfig` + `EventActionBinding` with a single `EventActionSpec` record (event + action in one).
   - Migrate job configs and parsing to use the unified spec.
   - Update MonitorLoop to consume `EventActionSpec` directly.
   - Tests: update integration/unit tests that assert event parsing or bindings.
   - Docs: update README/spec/examples to show the unified format.

2. **Condition evaluation tracking per EventAction** ✅
   - Introduce per‑EventAction condition state tracking (e.g., timeout start time).
   - Model the timeout as a stateful event type bound to the EventAction, with state stored per action instance.
   - Align naming: treat this as an EventAction condition rather than a global condition entry.
   - Tests: add coverage for timeout state persistence and recovery.
   - Docs: specify how stateful conditions are stored and how they reset.

3. **Persistence layout simplification** ✅
   - Keep **one file per job registration** (as requested) to simplify cleanup later.
   - Fold event/action state into that per‑job file, avoid separate per‑event files unless needed for large queues.
   - Ensure action queue state (if retained) is stored under the same job file or a single queue file keyed by job.
   - Migration: add backwards‑compatible loader to map old layout into new file structure.
   - Tests: update persistence tests to validate the new file structure and migration.
   - Docs: update persistence/state layout diagrams and examples.

4. **Reduce config parsing split** ✅
   - Move monitor config to compoconf (minimal schema) or create a single parse entry point for all configs.
   - Ensure job registration and event/action specs share one parser flow.
   - Tests: verify YAML parsing across examples still works.
   - Docs: consolidate config parsing description in README/spec.

7. **Enforce non-blocking conditions** ✅
   - Remove `blocking` options from conditions; all checks must return within the monitor loop (<0.1s).
   - Ensure file/glob/content conditions return `waiting` when not satisfied, never sleep.
   - Use `TimeoutCondition` (or a shared timeout wrapper) to enforce deadlines without blocking.
   - Tests: update condition tests and YAML examples to remove `blocking`.
   - Docs: clarify non-blocking condition semantics and timeout usage.

8. **Simplify condition semantics to boolean** ✅
   - Replace tri-state (`pass`/`waiting`/`fail`) with boolean results plus optional message/metadata.
   - Interpret `False` as "not yet" for start/finish/cancel and EventAction gating; avoid marking events failed on `False`.
   - Keep `TimeoutCondition` as the explicit deadline guard (returns `True` before deadline, `False` after).
   - Add optional `persistent_pass` / `persistent_fail` flags to condition configs to latch results per EventAction.
   - Tests: update condition/dispatcher tests for boolean semantics and persistence.
   - Docs: document boolean semantics, timeout usage, and persistence flags.

9. **Clarify backend‑specific actions** ✅
   - Use a shared action type with `backend_config` and job_kind validation.

10. **Transition/state simplification** ✅
   - Removed controller/transition layers in favor of a single MonitorLoop.
