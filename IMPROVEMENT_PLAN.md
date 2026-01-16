# Codebase Consistency and Improvement Plan

This document outlines identified inconsistencies in the `monitor` project following recent refactors for persistence, action queuing, and coverage optimization, along with a roadmap for structural improvements.

## 1. Identified Inconsistencies

### 1.1 State Instantiation Logic
There are currently three different ways monitor states (e.g., `SuccessState`, `StalledState`) are instantiated:
*   **`monitor.utils.states.get_state_by_name`**: Uses string manipulation and `compoconf.parse_config`.
*   **`monitor.watcher._fallback_state_for`**: Uses a hardcoded mapping and direct instantiation.
*   **`monitor.controller._fallback_state_for`**: (Recently removed/merged) but similar logic exists in the state event configuration handling.
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
