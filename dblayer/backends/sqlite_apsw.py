"""Experimental in-process SQLite backend on apsw (``pip install
dblayer[apsw]``), an alternative to ``SqliteInProcess``.

Work in progress: see the module as it grows.
"""

from __future__ import annotations

__all__ = ["SqliteApsw", "apsw_unavailable"]


def apsw_unavailable() -> str | None:
    """``None`` when apsw can be imported, else a skip/unavailable reason."""
    try:
        import apsw  # noqa: F401
    except ImportError:
        return "apsw is not installed (uv run --extra apsw ...)"
    return None


class SqliteApsw:
    """Placeholder; the implementation follows."""
