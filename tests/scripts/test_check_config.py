from __future__ import annotations

import subprocess
import sys
import os
from pathlib import Path

_MONITOR_ROOT = Path(__file__).resolve().parent.parent.parent
_SCRIPTS_DIR = _MONITOR_ROOT / "scripts"
_EXAMPLES_DIR = _MONITOR_ROOT / "examples"
_SLURM_GEN_SRC = _MONITOR_ROOT.parent / "slurm_gen" / "src"
_ENV = {
    **os.environ,
    "PYTHONPATH": os.pathsep.join(
        [str(_MONITOR_ROOT / "src")]
        + ([str(_SLURM_GEN_SRC)] if _SLURM_GEN_SRC.exists() else [])
    ),
}


def test_check_config_script(tmp_path: Path) -> None:
    config_path = _EXAMPLES_DIR / "monitor_app.yaml"
    script = _SCRIPTS_DIR / "check_config.py"
    result = subprocess.run(
        [sys.executable, str(script), "--config", str(config_path), "--state-dir", str(tmp_path / "state")],
        check=True,
        capture_output=True,
        text=True,
        env=_ENV,
    )
    assert result.stdout.strip() == "ok"
