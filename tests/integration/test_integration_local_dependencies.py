import time
import pytest
from pathlib import Path
from monitor.controller import MonitorController
from monitor.local_client import LocalCommandClient
from monitor.watcher import SlurmLogMonitor, SlurmLogMonitorConfig, LogEventConfig
from monitor.submission import JobRegistration
from monitor.conditions import FileExistsConditionConfig, FileExistsCondition
from monitor.persistence import MonitorStateStore
from monitor.action_queue import ActionQueue
from monitor.actions import LogActionConfig

def test_local_file_dependency(tmp_path):
    # Setup
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    # We use SlurmLogMonitor but configured for local files implicitly by what it watches
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    trigger_file = tmp_path / "trigger.txt"
    
    # Job 1: Producer - sleeps then creates file
    producer_script = tmp_path / "producer.sh"
    producer_script.write_text(f"sleep 2 && touch {trigger_file} && echo 'Producer done'")
    
    reg1 = JobRegistration(
        name="producer",
        command=["bash", str(producer_script)],
        log_path=str(tmp_path / "producer.log"),
    )
    controller.register_job("job1", reg1)
    
    # Job 2: Consumer - waits for file
    consumer_script = tmp_path / "consumer.sh"
    consumer_script.write_text("sleep 2 && echo 'Consumer running'")
    
    condition_config = FileExistsConditionConfig(path=str(trigger_file))
    reg2 = JobRegistration(
        name="consumer",
        command=["bash", str(consumer_script)],
        log_path=str(tmp_path / "consumer.log"),
        start_condition=condition_config
    )
    controller.register_job("job2", reg2)
    
    # Initial state check
    jobs = {j.name: j for j in controller.jobs()}
    assert not jobs["producer"].submitted
    assert not jobs["consumer"].submitted
    
    # Run loop 1: Should start Job 1 (no condition), but Job 2 waits
    controller.observe_once_sync()
    
    jobs = {j.name: j for j in controller.jobs()}
    assert jobs["producer"].submitted
    assert not jobs["consumer"].submitted # Condition not met yet
    
    # Wait for Job 1 to finish and create file (sleep 2s in script + buffer)
    # We simulate time passing by running the loop multiple times
    # In a real integration test we might need to actually wait
    
    start_time = time.time()
    job2_started = False
    
    while time.time() - start_time < 15:
        controller.observe_once_sync()
        current_jobs = list(controller.jobs())
        jobs = {j.name: j for j in current_jobs}
        
        # Check if Job 2 started
        if "consumer" in jobs and jobs["consumer"].submitted:
            job2_started = True
            break
        
    assert job2_started, "Job 2 should have started after trigger file was created"
    assert trigger_file.exists()

def test_local_file_content_dependency(tmp_path):
    # Setup
    state_dir = tmp_path / "state_content"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    trigger_file = tmp_path / "content_trigger.txt"
    trigger_file.touch() # Create empty file first
    
    # Job 1: Producer - appends "READY" after delay
    producer_script = tmp_path / "producer_content.sh"
    producer_script.write_text(f"sleep 2 && echo 'READY' > {trigger_file}")
    
    reg1 = JobRegistration(
        name="producer",
        command=["bash", str(producer_script)],
        log_path=str(tmp_path / "producer.log"),
    )
    controller.register_job("job1", reg1)
    
    # Job 2: Consumer - waits for "READY" in file
    from monitor.conditions import FileContentConditionConfig
    
    # Create script file first
    consumer_script = tmp_path / "consumer_content.sh"
    consumer_script.write_text("sleep 2 && echo 'Consumer running'")

    condition_config = FileContentConditionConfig(path=str(trigger_file), pattern="READY", mode="contains")
    reg2 = JobRegistration(
        name="consumer",
        command=["bash", str(consumer_script)],
        log_path=str(tmp_path / "consumer.log"),
        start_condition=condition_config
    )
    controller.register_job("job2", reg2)
    
    # Run loop - Consumer should NOT start initially (file empty)
    controller.observe_once_sync()
    jobs = {j.name: j for j in controller.jobs()}
    assert jobs["producer"].submitted
    assert not jobs["consumer"].submitted
    
    # Wait for producer to write content
    start_time = time.time()
    job2_started = False
    
    while time.time() - start_time < 15:
        controller.observe_once_sync()
        current_jobs = list(controller.jobs())
        jobs = {j.name: j for j in current_jobs}
        
        if "consumer" in jobs and jobs["consumer"].submitted:
            job2_started = True
            break
        
        time.sleep(0.5)
        
    assert job2_started, "Job 2 should have started after content appeared"

def test_cancel_condition_dependency(tmp_path):
    # Setup
    state_dir = tmp_path / "state_cancel"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    start_trigger = tmp_path / "never_created.txt"
    cancel_trigger = tmp_path / "cancel.txt"
    
    # Job 1: Canceller - creates cancel file
    canceller_script = tmp_path / "canceller.sh"
    canceller_script.write_text(f"sleep 1 && touch {cancel_trigger}")
    
    reg1 = JobRegistration(
        name="canceller",
        command=["bash", str(canceller_script)],
        log_path=str(tmp_path / "canceller.log"),
    )
    controller.register_job("job1", reg1)
    
    # Job 2: Victim - waits for start_trigger (never comes), but has cancel_trigger
    from monitor.conditions import FileExistsConditionConfig
    
    start_cond = FileExistsConditionConfig(path=str(start_trigger))
    cancel_cond = FileExistsConditionConfig(path=str(cancel_trigger))
    
    reg2 = JobRegistration(
        name="victim",
        command=["bash", "-lc", "echo 'I should not run'"],
        log_path=str(tmp_path / "victim.log"),
        start_condition=start_cond,
        cancel_condition=cancel_cond
    )
    controller.register_job("job2", reg2)
    
    # Run loop
    controller.observe_once_sync()
    jobs = {j.name: j for j in controller.jobs()}
    assert jobs["canceller"].submitted
    assert "victim" in jobs
    assert not jobs["victim"].submitted
    
    # Wait for cancel trigger
    start_time = time.time()
    victim_cancelled = False
    
    while time.time() - start_time < 10:
        controller.observe_once_sync()
        current_jobs = list(controller.jobs())
        jobs = {j.name: j for j in current_jobs}
        
        # Check if victim is removed (cancelled)
        if "victim" not in jobs:
            victim_cancelled = True
            break
        
        time.sleep(0.5)
        
    assert victim_cancelled, "Job 'victim' should have been cancelled/removed"

def test_complex_dependency_chain(tmp_path):
    """
    Test scenario with 3 jobs:
    - Starter: Runs long (5s), triggers Victim start.
    - Canceler: Runs short (2s), triggers Victim cancel.
    - Victim: Waits for Starter (start_cond) AND Canceler (cancel_cond).
    
    Expected: Victim is cancelled because Canceler finishes before Starter.
    """
    # Setup
    state_dir = tmp_path / "state_complex"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    start_trigger = tmp_path / "start_trigger.txt"
    cancel_trigger = tmp_path / "cancel_trigger.txt"
    
    # Job 1: Starter - sleeps 5s then touch start_trigger
    starter_script = tmp_path / "starter.sh"
    starter_script.write_text(f"sleep 5 && touch {start_trigger} && echo 'Starter done'")
    
    reg1 = JobRegistration(
        name="starter",
        command=["bash", str(starter_script)],
        log_path=str(tmp_path / "starter.log"),
    )
    controller.register_job("job1", reg1)
    
    # Job 2: Canceler - sleeps 2s then touch cancel_trigger
    canceler_script = tmp_path / "canceler.sh"
    canceler_script.write_text(f"sleep 2 && touch {cancel_trigger} && echo 'Canceler done'")
    
    reg2 = JobRegistration(
        name="canceler",
        command=["bash", str(canceler_script)],
        log_path=str(tmp_path / "canceler.log"),
    )
    controller.register_job("job2", reg2)
    
    # Job 3: Victim - waits for start_trigger, cancelled by cancel_trigger
    from monitor.conditions import FileExistsConditionConfig
    
    start_cond = FileExistsConditionConfig(path=str(start_trigger))
    cancel_cond = FileExistsConditionConfig(path=str(cancel_trigger))
    
    victim_script = tmp_path / "victim.sh"
    victim_script.write_text("echo 'Victim running'")
    
    reg3 = JobRegistration(
        name="victim",
        command=["bash", str(victim_script)],
        log_path=str(tmp_path / "victim.log"),
        start_condition=start_cond,
        cancel_condition=cancel_cond
    )
    controller.register_job("job3", reg3)
    
    # Start checking
    start_time = time.time()
    victim_started = False
    victim_cancelled = False
    
    # Loop for up to 10 seconds
    while time.time() - start_time < 10:
        controller.observe_once_sync()
        current_jobs = {j.name: j for j in controller.jobs()}
        
        # Check if victim started
        if "victim" in current_jobs and current_jobs["victim"].submitted:
            victim_started = True
            break
            
        # Check if victim cancelled (removed from jobs)
        if "victim" not in current_jobs:
            victim_cancelled = True
            break
            
        time.sleep(0.5)
        
    assert not victim_started, "Victim should NOT have started"
    assert victim_cancelled, "Victim should have been cancelled"
    
    # Ensure cancel trigger exists (Canceler ran)
    assert cancel_trigger.exists()
    
    # Wait remaining time to ensure Starter also finishes (optional check)
    # We just want to confirm Victim didn't run.

def test_idempotency_finish_condition(tmp_path):
    """
    Test that a job is NOT submitted if its finish_condition is already met.
    """
    state_dir = tmp_path / "state_idempotency"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    output_file = tmp_path / "result.txt"
    output_file.write_text("I am already done")
    
    from monitor.conditions import FileExistsConditionConfig
    
    finish_cond = FileExistsConditionConfig(path=str(output_file))
    
    reg = JobRegistration(
        name="idempotent_job",
        command=["bash", "-lc", "echo 'Running...'"], # Should not run
        log_path=str(tmp_path / "run.log"),
        finish_condition=finish_cond
    )
    
    controller.register_job("job1", reg)
    
    # Run loop
    controller.observe_once_sync()
    
    # Check that job is NOT in the list (removed immediately)
    jobs = list(controller.jobs())
    assert len(jobs) == 0, "Job should have been removed because finish condition was met"
    
    # Verify it didn't run (log file shouldn't be created by script execution)
    # The LocalCommandClient mocks execution, but if it ran, it would be in "submitted" state before removal?
    # In _process_pending_submissions, we remove it BEFORE submit.
    # So client.submit should NOT have been called.
    # We can't easily check client.submit calls here as it is not a mock in this integration test (it's LocalCommandClient).
    # But checking len(jobs) == 0 is sufficient proof it was removed.

def test_cancel_condition_running(tmp_path):
    """
    Test that a RUNNING job is cancelled if its cancel_condition becomes met.
    """
    state_dir = tmp_path / "state_cancel_running"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))
    
    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    
    controller = MonitorController(monitor, client, state_store=state_store)
    
    cancel_trigger = tmp_path / "cancel.txt"
    
    # Job: Runs for a while (10s), but cancel trigger appears after 2s (via another "external" process/script)
    # Here we simulate trigger creation by the test logic
    script = tmp_path / "long_job.sh"
    script.write_text("sleep 10 && echo 'Done'")
    
    from monitor.conditions import FileExistsConditionConfig
    
    cancel_cond = FileExistsConditionConfig(path=str(cancel_trigger))
    
    reg = JobRegistration(
        name="running_victim",
        command=["bash", str(script)],
        log_path=str(tmp_path / "job.log"),
        cancel_condition=cancel_cond
    )
    
    controller.register_job("job1", reg)
    
    # 1. Start the job (trigger doesn't exist)
    controller.observe_once_sync()
    jobs = {j.name: j for j in controller.jobs()}
    assert jobs["running_victim"].submitted
    
    # 2. Verify it stays running
    controller.observe_once_sync()
    jobs = {j.name: j for j in controller.jobs()}
    assert jobs["running_victim"].submitted
    
    # 3. Create cancel trigger
    cancel_trigger.touch()
    
    # 4. Run loop -> Should cancel the job
    controller.observe_once_sync()
    
    # Check that job is removed (stop_job -> finalize_job -> remove_job)
    # Or at least in 'crash' state?
    # finalize_job removes it from submission_manager.
    jobs = list(controller.jobs())
    assert len(jobs) == 0, "Job should have been removed after cancellation"


def test_action_queue_recovery_on_restart(tmp_path):
    state_dir = tmp_path / "state_queue_recovery"
    state_dir.mkdir()
    state_store = MonitorStateStore(str(state_dir))

    monitor_config = SlurmLogMonitorConfig(poll_interval_seconds=0.1)
    monitor = SlurmLogMonitor(monitor_config)
    client = LocalCommandClient()
    controller = MonitorController(monitor, client, state_store=state_store)

    script = tmp_path / "job.sh"
    script.write_text("echo 'ERROR: boom'")
    log_path = tmp_path / "job.log"

    controller.register_job(
        "job1",
        JobRegistration(
            name="queued_job",
            command=["bash", str(script)],
            log_path=str(log_path),
            log_events=[
                LogEventConfig(
                    name="boom",
                    pattern="ERROR",
                    mode="queue",
                    action=LogActionConfig(message="queued {event_name}"),
                )
            ],
        ),
    )

    queue_path = state_store.session_path.with_suffix(".actions")
    action_queue = ActionQueue(queue_path)

    start = time.time()
    queued_action = None
    while time.time() - start < 3:
        controller.observe_once_sync()
        queued_action = action_queue.claim_next()
        if queued_action is not None:
            break
        time.sleep(0.1)

    assert queued_action is not None, "Expected a queued action to be created"
    assert action_queue.load(queued_action.queue_id).status == "running"

    # Recreate controller to trigger recovery.
    MonitorController(monitor, client, state_store=state_store)
    recovered_queue = ActionQueue(queue_path)
    reloaded = recovered_queue.load(queued_action.queue_id)
    assert reloaded is not None
    assert reloaded.status == "pending"
