from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_check_config_script(tmp_path: Path) -> None:
    config_path = Path("examples/monitor_app.yaml")
    script = Path("scripts/check_config.py")
    result = subprocess.run(
        [sys.executable, str(script), "--config", str(config_path), "--state-dir", str(tmp_path / "state")],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == "ok"
