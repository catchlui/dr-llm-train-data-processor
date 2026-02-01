from __future__ import annotations

import os
import sys
from pathlib import Path


def ensure_vendor_dolma_on_path(repo_root: str | os.PathLike | None = None) -> Path | None:
    """
    If `vendor/dolma/python` exists, prepend it to sys.path so that
    `import dolma` resolves to the vendored clone without requiring `pip install dolma`.

    Returns the path added, or None if not found.
    """
    if repo_root is None:
        repo_root = Path(__file__).resolve().parents[2]
    root = Path(repo_root)
    candidate = root / "vendor" / "dolma" / "python"
    if candidate.exists() and candidate.is_dir():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        return candidate
    return None

