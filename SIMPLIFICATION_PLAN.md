# Simplification Plan

This plan focuses on where the codebase can be simplified, and where it should not be simplified (with rationale).

## 1) Feasible Simplifications

### 1.1 Controller Size (High Impact)
- **Extract TransitionManager**: Move SLURM transition detection + state classification out of `MonitorController`. (Done)
- **Extract ActionDispatcher**: Move action-queue vs inline execution branching out of controller. (Done)
- **Extract ConditionEvaluator**: Deduplicate the start/cancel/finish condition checks into a helper or small service. (Done)
- **Why feasible**: These are already cohesive slices with clear inputs/outputs and no external API changes required.

### 1.2 Event Identity + Lifecycle (Low Risk)
- **Centralize event key/id**: Already mostly done; keep consolidation in `monitor.events`.
- **Collapse redundant helpers**: Remove remaining legacy wrappers once no tests rely on them.
- **Why feasible**: Pure internal helpers with direct tests.

### 1.3 Template Rendering Duplication (Done)
- **Shared template helper**: Already deduplicated via `monitor.utils.template`.
- **Why feasible**: Pure utility used by actions/conditions.

### 1.4 Action Queue + Persistence Consistency (Medium)
- **Single atomic write helper**: Align `ActionQueue._write` with `MonitorStateStore._atomic_write`. (Done)
- **Formal recovery policy**: Already reset `running` to `pending`; document and test. (Done)
- **Why feasible**: File IO is local and implementation is self-contained.

### 1.5 Test File Structure (Low Effort)
- **Group by domain**: `tests/controller`, `tests/persistence`, `tests/actions`, `tests/conditions`. (Done)
- **Why feasible**: Mechanical moves + import updates.

## 2) Potential Simplifications with Caveats

### 2.1 Merge Watcher + Controller State Logic
- **Idea**: Move state-event creation into watcher to keep controller thin.
- **Caveat**: Controller needs job metadata, persistence, and action dispatch context.
- **Recommendation**: Keep state-event creation in controller or add a small mediator.

### 2.2 Drop Legacy APIs
- **Idea**: Remove legacy compatibility layers in `MonitorStateStore` and controller wrappers.
- **Caveat**: Tests and downstream callers may rely on them.
- **Recommendation**: Deprecate first, remove once consumers are updated.

### 2.3 Merge Action and Condition Systems
- **Idea**: Unified “rule” system for actions + conditions.
- **Caveat**: Would complicate config parsing (`compoconf`) and break external configs.
- **Recommendation**: Not worth it unless breaking changes are acceptable.

## 3) Simplifications That Are Likely Unfeasible (for now)

### 3.1 Replace `MonitorController` with a Stateless Pipeline
- **Why unfeasible**: The controller coordinates persistence, action queue state, and job lifecycle over time.
- **Risk**: Would require a new state store abstraction and significant API changes.

### 3.2 Remove Persistence Layer
- **Why unfeasible**: Crash recovery is a core requirement; store is used across controller and submission manager.
- **Risk**: Would regress recovery guarantees and tests.

### 3.3 Collapse Monitor/Executor Separation
- **Why unfeasible**: Clear responsibility boundaries (observe vs execute) help portability across SLURM/local.
- **Risk**: Tight coupling would make local/slurm backends harder to maintain.

## 4) Suggested Next Simplification Sequence

1. Extract `ConditionEvaluator` and replace repeated checks in controller. (Done)
2. Extract `TransitionManager` and reduce controller size. (Done)
3. Extract `ActionDispatcher` for queue/inline action handling. (Done)
4. Remove remaining legacy wrappers after confirming no external usage. (Pending)
