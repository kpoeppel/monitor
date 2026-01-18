from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SLURM_GEN_SRC = ROOT / "slurm_gen" / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if SLURM_GEN_SRC.exists() and str(SLURM_GEN_SRC) not in sys.path:
    sys.path.insert(0, str(SLURM_GEN_SRC))
