# Tests Changed

- Removed `test_run_autoexp_action` from `tests/actions/test_actions.py` because `RunAutoExpAction` was removed; existing `RunCommandAction` covers command execution with overrides.
- Removed `test_store_schema_version_on_session` from `tests/persistence/test_persistence.py` because session persistence helpers were removed.
- Removed `test_publish_event_action` from `tests/actions/test_actions.py` because `PublishEventAction` was removed.
- Removed `test_job_ids_by_name`, `test_get_job`, `test_get_job_not_found`, and `test_failed_job_status` from `tests/integration/test_local_client.py` because `LocalCommandClient` no longer exposes those methods and they were removed from `JobClientProtocol`.
