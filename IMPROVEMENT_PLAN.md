# Codebase Consistency and Improvement Plan

This document outlines identified inconsistencies in the `monitor` project following recent refactors for persistence, action queuing, and coverage optimization, along with a roadmap for structural improvements.

## 1. Identified Inconsistencies

### 1.1 State Instantiation Logic
There are currently three different ways monitor states (e.g., `SuccessState`, `StalledState`) are instantiated:
*   **`monitor.watcher._fallback_state_for`**: Uses a hardcoded mapping and direct instantiation.
*   **Direct Config instantiation**: Via `cfg.state.instantiate(MonitorStateInterface)`.

**Issue**: Logic for mapping string modes (stall, success, crash) to state objects is fragmented, leading to maintenance overhead.

### 1.2 Path Expansion Redundancy
*   The logic to expand SLURM-style tokens (`%j`, `%A`, `%a`) in log paths is duplicated in:
    *   `monitor.controller.MonitorController._expand_log_path`
    *   `monitor.submission.SubmissionManager._expand_log_path`
*   `MonitorController` sometimes uses `SubmissionManager._expand_log_path` and sometimes its own implementation.

### 1.3 Event and Result Models
*   `ActionResult` and `EventRecord` are central to the system but were recently moved between `actions.py` and `events.py`.
*   The relationship between `EventStatus` (the lifecycle of the event record) and `ActionResult.status` (the outcome of a specific action execution) is slightly blurred in the `update_event` logic.

### 1.4 God Object Tendencies
*   `MonitorController` has grown significantly (approx. 600 lines). It now handles:
    *   SLURM state transitions.
    *   Monitor cycle coordination.
    *   Event-to-Action mapping.
    *   Action execution (both inline and queued).
    *   Persistence coordination.
    *   Condition checking (Start/Cancel/Finish).

## 2. Proposed Improvements

### 2.1 Centralized Registry
*   **Unified State Registry**: Consolidate `get_state_by_name` and `_fallback_state_for` into a single registry or factory within `monitor.states`.
*   **Path Utility**: Move all log path expansion logic to `monitor.utils.paths` to ensure consistent behavior across the CLI, Controller, and Submission Manager.

### 2.2 Decoupling the Controller
*   **TransitionManager**: Extract SLURM state transition logic and "mode classification" (Success/Crash/Stall) into a dedicated class.
*   **ActionDispatcher**: Move the logic that renders action configurations and decides between `queue` vs `inline` execution into a service.
*   **ConditionEvaluator**: Streamline the repetitive condition checking (Start/Cancel/Finish) used in both `_process_pending_submissions` and the main loop.

### 2.3 Persistence Robustness
*   **Atomic Writes**: The `MonitorStateStore` currently uses simple `write_text`. For a long-running monitor, using temporary files and atomic moves would prevent state corruption during crashes.
*   **Schema Versioning**: Add a version field to `session.json` and job records to handle future changes in the `StoredJob` or `EventRecord` schemas.

### 2.4 Test Consolidation
*   The `tests/` directory contains several "gap fill" and "comprehensive" files created during coverage pushes.
*   **Action**: Consolidate `test_controller.py`, `test_controller_lifecycle.py`, and `test_controller_edge_cases.py` into a structured test suite organized by functionality.

## 3. Implementation Roadmap

1.  **Phase 1: Utilities**: Create `monitor.utils.paths` and unify state instantiation.
2.  **Phase 2: Refactor Persistence**: Implement atomic writes and move path expansion to the utility.
3.  **Phase 3: Controller Decomposition**: Extract `TransitionManager` and `ConditionEvaluator`.
4.  **Phase 4: Cleanup**: Update `SPEC.md` to match the current implementation (Action Queue, etc.) and consolidate tests.

## 4. Repo Component Map (Current)

### 4.1 Core Workflow
*   **Controller**: `monitor.controller.MonitorController` coordinates watcher + client, handles events/actions, persistence.
*   **Watcher**: `monitor.watcher.SlurmLogMonitor` (and `NullMonitor`) produce `MonitorOutcome` + `MonitorEvent`s.
*   **Executor**: `monitor.executor.Executor` implements submit/restart/stop/finalize job operations.
*   **Submission**: `monitor.submission.SubmissionManager` stores `JobRuntimeState`, uses `MonitorStateStore` for recovery.
*   **Job Client**: `monitor.job_client_protocol.JobClientProtocol` defines submit/squeue/cancel/remove contract.

### 4.2 Domain Models
*   **States**: `monitor.states` defines `BaseMonitorState` variants and `get_state`.
*   **Events**: `monitor.events` defines `EventRecord`, `EventStatus`, `ActionResult`.
*   **Actions**: `monitor.actions` defines `BaseMonitorAction` plus concrete actions.
*   **Conditions**: `monitor.conditions` defines `MonitorConditionInterface` and gate logic.

### 4.3 Infrastructure
*   **Persistence**: `monitor.persistence.state_store.MonitorStateStore` stores jobs + events with atomic writes.
*   **Action Queue**: `monitor.action_queue.ActionQueue` provides file-backed async action staging.
*   **Utilities**: `monitor.utils.*` for run helpers and path handling.

## 5. Continuation: Concrete Improvement Backlog

### 5.1 State/Event Hygiene
*   **Event status separation**: Clarify the boundary between `EventStatus` and `ActionResult.status` in `BaseMonitorAction.update_event`.
*   **Event identity**: Centralize event ID construction and index maintenance so `MonitorController` and `ActionQueue` share the same rules.
*   **State mapping**: Move `get_state` mapping into a small registry map and expose a single `resolve_state(key: str)` helper for controller + watcher.

### 5.2 Controller Decomposition (Targeted Cuts)
*   **TransitionManager**: Lift `_classify_mode`, `_capture_slurm_transitions`, and state-event fallback into `monitor.transition`.
*   **ActionDispatcher**: Lift event-to-action decision logic and queue/inline branching into `monitor.dispatcher`.
*   **ConditionEvaluator**: Centralize start/cancel/finish evaluation to remove repeated code in controller and tests.

### 5.3 Persistence + Queue Robustness
*   **Schema versioning**: Add `schema_version` to session and per-job/per-event records; document migration behavior. (Implemented v1)
*   **Queue atomic writes**: Mirror `MonitorStateStore._atomic_write` behavior in `ActionQueue._write`.
*   **Recovery behavior**: Define queue recovery rules for `running` entries after crash (reset to pending).

### 5.4 Test Consolidation + Coverage Targets
*   **Test layout**: Reorganize tests into `tests/controller/`, `tests/persistence/`, `tests/actions/`, `tests/conditions/`.
*   **Behavioral coverage**: Add focused tests for state resolution, action queue recovery, and atomic writes.

### 5.5 Spec + Examples Alignment
*   **SPEC.md**: Expand to include Action Queue, EventRecord lifecycle, and persistence schema.
*   **Examples**: Update `examples/local_monitoring_example.py` to include queue usage and action execution paths. (Updated)

### 5.6 Further Simplification + Deduplication
*   **Template rendering**: Deduplicate `{var}` replacement helpers used in actions/conditions into `monitor.utils`.
*   **Legacy wrappers**: Audit remaining controller legacy wrappers for removal or relocation.
*   **Condition handling**: Extract a single helper for start/cancel/finish condition evaluation to shrink controller flow.
*   **Event identity helper**: Centralize event key + event ID construction in `monitor.events` to remove controller-only logic.
