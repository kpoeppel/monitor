# Monitor

A library for monitoring job execution state and triggering actions based on events.

## Workflow

The monitoring workflow is managed by a few key components that separate the concerns of job submission, observation, and execution.

1.  **`SubmissionManager`**:
    *   **Role**: Handles the initial registration of jobs and maintains their state.
    *   **Functionality**: When a job is registered, the `SubmissionManager` tracks its `JobRuntimeState` (ID, name, attempts, etc.) and persists this information to a `MonitorStateStore` for crash recovery.

2.  **`SlurmLogMonitor` (Watcher)**:
    *   **Role**: Observes the system for events.
    *   **Functionality**: This component watches log files for specific patterns (e.g., errors, completion markers) and checks the SLURM queue for job status changes. It generates `MonitorEvent` objects when something noteworthy occurs.

3.  **`MonitorController`**:
    *   **Role**: The central coordinator that orchestrates the workflow.
    *   **Functionality**: In its main observation loop (`observe_once_sync`), the controller:
        *   Gathers job information from the `SubmissionManager`.
        *   Asks the `Watcher` to check for events related to those jobs.
        *   Receives `MonitorEvent` objects from the watcher.
        *   Classifies the overall state of each job (e.g., `running`, `stall`, `crash`).
        *   Delegates the handling of these events and state changes to the `Executor`.

4.  **`Executor`**:
    *   **Role**: Acts on the decisions made by the controller.
    *   **Functionality**: Based on the events, the `Executor` performs actions such as:
        *   Restarting a job (`restart_job`), which includes waiting for any defined `start_condition`.
        *   Stopping a job.
        *   Marking a job as successfully completed.

This separation ensures that job state is cleanly managed, monitoring logic is isolated, and execution actions are handled by a dedicated component.

## Features

- **State Machine**: Track job states (Pending, Started, Stalled, Crash, Success, Timeout).
- **Event Loop**: Watch for log events (regex patterns) or job state changes.
- **Action Queue**: Queue actions (like email notifications) or execute them inline (like restarts).
- **Persistence**: Persist job state across orchestrator restarts using JSON store.
- **Flexible Backend**: Works with SLURM, local commands, or custom job execution systems.

## Installation

```bash
# Basic installation (for local command execution)
pip install monitor

# With SLURM support
pip install monitor[slurm]
```

## Dependencies

- `compoconf`: For configuration management.
- `slurm_gen` (optional): For SLURM integration. Install with `pip install monitor[slurm]`.

## Usage

### Option 1: Local Command Execution (No SLURM Required)

Monitor local bash scripts with event-driven actions:

```python
from monitor import LocalCommandClient
from monitor.controller import MonitorController, JobRegistration
from monitor.watcher import SlurmLogMonitor, SlurmLogMonitorConfig, LogEventConfig
from monitor.states import CrashStateConfig

# 1. Configure the Monitor
error_rule = LogEventConfig(
    name="cuda_oom",
    pattern="CUDA out of memory",
    state=CrashStateConfig(key="crash"),
    metadata={"reason": "OOM"}
)

monitor_config = SlurmLogMonitorConfig(
    log_events=[error_rule],
    poll_interval_seconds=10
)
monitor = SlurmLogMonitor(monitor_config)

# 2. Setup Controller with Local Client
local_client = LocalCommandClient()
controller = MonitorController(monitor, local_client, state_store=None)

# 3. Register a Local Script to Watch
controller.register_job(
    job_id="1",
    registration=JobRegistration(
        name="local-training",
        script_path="./train.sh",
        log_path="./train.log"
    )
)

# 4. Run Observation Loop
result = controller.observe_once_sync()
decision = result.decisions.get("1")

if decision and decision.action == "stop":
    print(f"Job stopped! Reason: {decision.reason}")
```

### Option 2: SLURM Cluster Execution

Monitor SLURM jobs on a cluster (requires `pip install monitor[slurm]`):

```python
from slurm_gen.client import FakeSlurmClient, FakeSlurmClientConfig
from monitor.controller import MonitorController, JobRegistration
from monitor.watcher import SlurmLogMonitor, SlurmLogMonitorConfig, LogEventConfig
from monitor.states import CrashStateConfig

# 1. Configure the Monitor
# Define what to look for in the logs
error_rule = LogEventConfig(
    name="cuda_oom",
    pattern="CUDA out of memory",
    state=CrashStateConfig(key="crash"),
    metadata={"reason": "OOM"}
)

monitor_config = SlurmLogMonitorConfig(
    log_events=[error_rule],
    poll_interval_seconds=10
)
monitor = SlurmLogMonitor(monitor_config)

# 2. Setup the Controller
# (In production, use a real SlurmClient and MonitorStateStore)
slurm_client = MagicMock(spec=BaseSlurmClient)
slurm_client.squeue.return_value = {"12345": "RUNNING"}

controller = MonitorController(monitor, slurm_client, state_store=None)

# 3. Register a Job to Watch
controller.register_job(
    job_id="12345",
    registration=JobRegistration(
        name="training-job",
        script_path="train.sbatch",
        log_path="train.log"
    )
)

# 4. Run the Observation Loop (Simulation)
# Simulate log content appearing
with open("train.log", "w") as f:
    f.write("Training started...\n")

# First cycle: Active
result = controller.observe_once_sync()
print(f"Cycle 1 Events: {len(result.events)}") 

# Simulate an error
with open("train.log", "a") as f:
    f.write("Error: CUDA out of memory\n")

# Second cycle: Crash detected
result = controller.observe_once_sync()
decision = result.decisions.get("12345")

if decision and decision.action == "stop":
    print(f"Job stopped! Reason: {decision.reason}")
    # Output: Job stopped! Reason: OOM
```

### Option 3: Custom Job Client

Implement your own job client for other batch systems (PBS, LSF, Kubernetes, etc.):

```python
from monitor.job_client_protocol import JobClientProtocol

class MyCustomClient(JobClientProtocol):
    """Custom implementation for your batch system."""

    def submit(self, name: str, script_path: str, log_path: str) -> str:
        # Your implementation
        ...

    def submit_array(self, array_name: str, script_path: str,
                     log_paths: list[str], task_names: list[str]) -> list[str]:
        # Your implementation
        ...

    def cancel(self, job_id: str) -> None:
        # Your implementation
        ...

    def remove(self, job_id: str) -> None:
        # Your implementation
        ...

    def squeue(self) -> dict[str, str]:
        # Your implementation
        ...

    def job_ids_by_name(self, name: str) -> list[str]:
        # Your implementation
        ...

    def get_job(self, job_id: str):
        # Your implementation
        ...


# Use with MonitorController
custom_client = MyCustomClient()
controller = MonitorController(monitor, custom_client)
```

See `examples/local_monitoring_example.py` for a complete working example of local monitoring.

## Testing

Run the tests using `pytest`. Note that `slurm_gen` must be in the `PYTHONPATH` for integration tests.

```bash
# Test with SLURM support
PYTHONPATH=src:../slurm_gen/src pytest tests/

# Test only local client (no slurm_gen needed)
PYTHONPATH=src pytest tests/test_local_client.py
```
