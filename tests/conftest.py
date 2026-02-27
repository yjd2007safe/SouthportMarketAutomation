"""Pytest bootstrap for stable local imports.

This keeps the repository root and ``src`` directory importable regardless of
where pytest is invoked from.
"""

from __future__ import annotations

import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"

for _path in (_REPO_ROOT, _SRC_DIR):
    _path_str = str(_path)
    if _path_str not in sys.path:
        sys.path.insert(0, _path_str)
