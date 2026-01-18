# Tests Changed

- Removed `test_run_autoexp_action` from `tests/actions/test_actions.py` because `RunAutoExpAction` was removed; existing `RunCommandAction` covers command execution with overrides.
- Removed `test_store_schema_version_on_session` from `tests/persistence/test_persistence.py` because session persistence helpers were removed.
- Removed `test_publish_event_action` from `tests/actions/test_actions.py` because `PublishEventAction` was removed.
- Removed `test_job_ids_by_name`, `test_get_job`, `test_get_job_not_found`, and `test_failed_job_status` from `tests/integration/test_local_client.py` because `LocalCommandClient` no longer exposes those methods and they were removed from `JobClientProtocol`.
- Removed legacy controller/watcher/persistence/action-queue tests (e.g., `tests/controller/test_controller.py`, `tests/watcher/test_watcher.py`, `tests/persistence/test_persistence.py`) because those modules were deleted in favor of `MonitorLoop` with per-job JSON state.
- Removed outdated condition and action tests (e.g., `tests/conditions/test_conditions.py`, `tests/actions/test_actions.py`) to rebuild coverage around boolean conditions and inline action handling in the new loop.
