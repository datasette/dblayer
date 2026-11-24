"""A minimal Datasette-like demo app for dblayer.

Run it with ``uv run --all-extras --group demo python -m dblayer_demo``.
It uses only dblayer's public API: introspection, ``Database.execute`` on the
read path, and the named operations to seed its demo data.
"""

from .app import create_app

__all__ = ["create_app"]
