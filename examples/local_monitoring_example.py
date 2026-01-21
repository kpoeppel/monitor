"""Example: Monitor local bash scripts with automatic error recovery.

This example demonstrates using monitor with LocalCommandClient to:
1. Execute a local training script
2. Detect errors in logs
3. Automatically restart on specific errors
4. Track state across restarts

Use case: Local development or single-machine workflows that need
robust error handling and monitoring.
"""

import time
import shutil
import logging
from pathlib import Path
from monitor import LocalCommandClient
from monitor.actions import LogActionConfig, RestartActionConfig, FinishActionConfig
from monitor.actions import LogEventConfig
from monitor.loop import JobFileStore, JobRecordConfig, MonitorLoop
from monitor.submission import LocalJobConfig
from monitor.conditions import MaxAttemptsConditionConfig


def main():
    """Run a monitored local workflow."""
    logging.basicConfig(level=logging.INFO)

    # Create a test script that simulates training with potential errors
    script_path = Path("./example_train.sh")
    script_path.write_text("""#!/bin/bash
set -e

echo "Starting training..."
sleep 1

# Simulate random failures (for demo purposes)
if [ -f "./fail_marker" ]; then
    echo "ERROR: CUDA out of memory"
    rm ./fail_marker
    exit 1
fi

echo "Epoch 1/10 - loss: 0.5"
sleep 1
echo "Epoch 2/10 - loss: 0.4"
sleep 1
echo "Training completed successfully"
""")
    script_path.chmod(0o755)

    # Create fail marker to trigger first failure
    Path("./fail_marker").touch()

    # Setup local client + monitor loop
    local_client = LocalCommandClient()
    store = JobFileStore("./monitor_state")
    loop = MonitorLoop(store, local_client=local_client, poll_interval_seconds=1)

    # Register the job
    log_path = "./training_%t.log"
    log_path_current = "./training_latest.log"
    job_id = "local-training"
    store.upsert(
        JobRecordConfig(
            job_id=job_id,
            definition=LocalJobConfig(
                name="local-training",
                command=["bash", str(script_path)],
                log_path=log_path,
                log_path_current=log_path_current,
                log_events=[
                    LogEventConfig(
                        name="oom_error",
                        pattern="CUDA out of memory",
                        metadata={"reason": "OOM", "recoverable": True},
                        action=RestartActionConfig(reason="OOM detected - restarting"),
                        condition=MaxAttemptsConditionConfig(max_attempts=3),
                    ),
                    LogEventConfig(
                        name="training_started",
                        pattern="Starting training",
                        action=LogActionConfig(message="Training started for {job_name}"),
                    ),
                    LogEventConfig(
                        name="training_complete",
                        pattern="Training completed successfully",
                        action=FinishActionConfig(reason="Training completed"),
                    ),
                ],
            ),
        )
    )

    print(f"Started job {job_id}")
    print(f"Monitoring log: {log_path_current}")
    print("-" * 50)

    # Run monitoring loop until job is marked finished
    while True:
        loop.observe_once()
        job = store.load(job_id, include_finished=True)
        if job and job.runtime.final_state is not None:
            print(f"Job finished with state: {job.runtime.final_state}")
            print(f"Total attempts: {job.runtime.attempts}")
            break
        time.sleep(2)

    # Cleanup example files
    script_path.unlink(missing_ok=True)
    for path in Path(".").glob("training_*.log"):
        path.unlink(missing_ok=True)
    Path(log_path_current).unlink(missing_ok=True)
    Path("./fail_marker").unlink(missing_ok=True)
    shutil.rmtree(store.root, ignore_errors=True)


if __name__ == "__main__":
    main()
