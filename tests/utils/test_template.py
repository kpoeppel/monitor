from __future__ import annotations

from monitor.utils.template import replace_braced_keys


def test_replace_braced_keys() -> None:
    rendered = replace_braced_keys("job {job_id} {missing}", {"job_id": "1"})
    assert rendered == "job 1 {missing}"
