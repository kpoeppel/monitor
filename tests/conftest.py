from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SLURM_GEN_SRC = ROOT.parent / "slurm_gen" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if SLURM_GEN_SRC.exists() and str(SLURM_GEN_SRC) not in sys.path:
    sys.path.insert(0, str(SLURM_GEN_SRC))

# Imported for its side effect, not its names: monitor.submission runs @register
# on the job configs, so they exist in the registry before tests are collected.
try:
    from monitor.submission import LocalJobConfig, SlurmJobConfig  # noqa: F401
except ModuleNotFoundError:
    pass
