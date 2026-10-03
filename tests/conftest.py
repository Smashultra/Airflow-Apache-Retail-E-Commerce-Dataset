"""Make the same mounted modules importable in host and Docker tests."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for folder in (ROOT, ROOT / "scripts", ROOT / "dags"):
    sys.path.insert(0, str(folder))
