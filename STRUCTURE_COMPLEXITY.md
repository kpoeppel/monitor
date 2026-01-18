# Structure Complexity Review

## High-Level Structure (Summary)
- Monitor controller coordinates state, conditions, and actions; it delegates to a watcher (log monitor), executor (job submission), and persistence.
- Jobs are now explicit (`LocalJobRegistration`, `SlurmJobRegistration`) and define all event/action logic.
- Monitor config is intentionally lean (poll interval), state is persisted for recovery.
- Separate clients exist for local execution and SLURM submission; slurm_gen is used only for script generation.

## Unnecessary Complexity / Potential Simplifications

- **Event/action layering**: We still have `LogEventConfig` + `EventActionConfig` + `EventActionBinding` + `BaseMonitorAction`. This is flexible but deep. A single `EventActionSpec` (event + action in one record) could reduce indirection.
- **Mixed config parsing paths**: Monitor config uses manual instantiation, while job configs use compoconf. This split makes the mental model harder. Consider a single config parse pipeline (even if minimal).
- **Action queue + inline actions**: Two execution modes (queue vs inline) are powerful but add branching paths and persistence logic. If most use is inline, queue could be optional or moved to a plugin.
- **Job duplication + restart adjustments**: Multiple adjustment paths (`extra_args`, `extra_args_append`, `log_path_current`, `slurm` overrides) create combinatorial behavior. A single `adjustments` object with explicit patch semantics could reduce surface area.
- **Local vs Slurm differences**: We still allow fields (e.g., `slurm`) on shared base types, and rely on runtime checks. More explicit per‑backend action types would reduce ambiguity.
- **State transitions**: `TransitionManager` and the synthetic event mapping add a layer of logic that is non-trivial to reason about. Consider making transitions explicit in job config or simplifying outcomes.
- **Condition evaluation tracking**: `condition_data` + `started_ts` tracking per label is subtle and global. If only used for timeouts, a dedicated timeout condition type could be clearer.
- **Path semantics**: `log_path` templates + `log_path_current` symlink + local `%t` expansions are powerful but complex. If most users only need one stable path, consider a simplified default path policy.
- **Persistence layout**: Jobs/events are stored in separate folders plus `config.json` and action queue files. This is robust but complex to inspect manually. A single consolidated state file might be enough for non‑queue users.

## Potentially Unavoidable Complexity

- **Cross‑backend support**: Supporting local execution and SLURM requires some divergence (clients, submission logic, log handling).
- **Recovery & resumability**: Persisting state + event history is essential for crash recovery and requires structured storage.

## Implementation Plan (Proposed)

1. **Unify event/action into a single spec** ✅
   - Replace `LogEventConfig` + `EventActionConfig` + `EventActionBinding` with a single `EventActionSpec` record (event + action in one).
   - Migrate job configs and parsing to use the unified spec.
   - Update watcher/controller to consume `EventActionSpec` directly.
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
   - Split action types more explicitly by backend (local vs slurm) to avoid runtime ambiguity.
   - Ensure restart/duplicate actions validate against job type.
   - Tests: add type‑specific action parsing and execution tests.
   - Docs: document which actions are valid for each job type.

10. **Transition/state simplification** ⚠️
   - Re‑evaluate `TransitionManager` and synthetic event mapping; consider explicit transitions on job config if needed.
   - If kept, document transition rules in a single place and reduce indirection.
   - Tests: update transition-related tests for the new model.
   - Docs: update state machine explanation.
