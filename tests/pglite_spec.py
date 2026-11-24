"""The ``pglite`` conformance backend (kept here so ``conftest.py`` needs one
line): PostgreSQL in WebAssembly, one data directory per database, in a
Node.js worker process (``npm ci --prefix pglite-worker``)."""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any

_n = itertools.count(1)
_template: list[Path] = []


def make(tmp_path: Path) -> Any:
    """A fresh database. initdb costs ~0.8 s, so every database after the
    first in a session is a copy of a template cluster (``template=``)."""
    from dblayer.backends.pglite_worker import PGliteWorker

    return PGliteWorker(tmp_path / f"pg{next(_n)}", template=template())


def template() -> Path | None:
    """A data directory initialised once per test session (by a worker run
    on it), copied for each new database; None until made."""
    if _template:
        return _template[0]
    import subprocess
    import tempfile

    from dblayer.backends.pglite_worker import find_node, find_script

    import atexit
    import shutil

    base = Path(tempfile.mkdtemp(prefix="dblayer-pglite-template-"))
    atexit.register(shutil.rmtree, base, True)
    target = base / "template"
    script = Path(find_script()).parent.parent / "scripts" / "init_template.mjs"
    subprocess.run([find_node(), str(script), str(target)], check=True, capture_output=True, timeout=120)
    _template.append(target)
    return target


def available() -> str | None:
    from dblayer.backends.pglite_worker import unavailable

    return unavailable()
