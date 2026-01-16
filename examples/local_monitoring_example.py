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
from pathlib import Path
from monitor import LocalCommandClient
from monitor.controller import MonitorController, JobRegistration
from monitor.watcher import SlurmLogMonitor, SlurmLogMonitorConfig, LogEventConfig
from monitor.states import (
    CrashStateConfig,
    StartedStateConfig,
    SuccessStateConfig,
)


def main():
    """Run a monitored local workflow."""

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

    # Configure monitoring rules
    monitor_config = SlurmLogMonitorConfig(
        log_events=[
            LogEventConfig(
                name="oom_error",
                pattern="CUDA out of memory",
                state=CrashStateConfig(key="crash"),
                metadata={"reason": "OOM", "recoverable": True}
            ),
            LogEventConfig(
                name="training_started",
                pattern="Starting training",
                state=StartedStateConfig(key="started"),
            ),
            LogEventConfig(
                name="training_complete",
                pattern="Training completed successfully",
                state=SuccessStateConfig(key="success"),
            ),
        ],
        poll_interval_seconds=1,
    )

    monitor = SlurmLogMonitor(monitor_config)

    # Setup local client
    local_client = LocalCommandClient()
    controller = MonitorController(monitor, local_client)

    # Register the job
    log_path = "./training.log"
    job_id = local_client.submit(
        name="local-training",
        script_path=str(script_path),
        log_path=log_path,
    )

    controller.register_job(
        job_id=job_id,
        registration=JobRegistration(
            name="local-training",
            script_path=str(script_path),
            log_path=log_path,
        ),
    )

    print(f"Started job {job_id}")
    print(f"Monitoring log: {log_path}")
    print("-" * 50)

    # Monitor loop
    max_attempts = 3
    attempt = 1

    while True:
        result = controller.observe_once_sync()

        # Check for events
        for event in result.events:
            print(f"Event: {event.name} - {event.metadata}")

        # Check for decisions
        decision = result.decisions.get(job_id)
        if decision:
            print(f"\nDecision for {job_id}: {decision.action}")
            print(f"Reason: {decision.reason}")

            if decision.action == "stop":
                # Check if we should retry
                if "OOM" in decision.reason and attempt < max_attempts:
                    print(f"\nRetrying (attempt {attempt + 1}/{max_attempts})...")
                    attempt += 1

                    # Restart the job
                    new_job_id = local_client.submit(
                        name="local-training",
                        script_path=str(script_path),
                        log_path=log_path,
                    )

                    controller.register_job(
                        job_id=new_job_id,
                        registration=JobRegistration(
                            name="local-training",
                            script_path=str(script_path),
                            log_path=log_path,
                        ),
                    )

                    job_id = new_job_id
                    print(f"New job_id: {job_id}")
                else:
                    print("\nMax attempts reached or non-recoverable error. Stopping.")
                    break

        # Check job status
        statuses = local_client.squeue()
        status = statuses.get(job_id, "UNKNOWN")

        if status == "COMPLETED":
            print("\n✓ Job completed successfully!")
            break
        elif status == "FAILED" and not decision:
            print("\n✗ Job failed without triggering monitor event.")
            break

        time.sleep(2)

    # Cleanup
    local_client.cleanup()

    # Show final log
    print("\n" + "=" * 50)
    print("Final log contents:")
    print("=" * 50)
    if Path(log_path).exists():
        print(Path(log_path).read_text())

    # Cleanup example files
    script_path.unlink(missing_ok=True)
    Path(log_path).unlink(missing_ok=True)
    Path("./fail_marker").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
