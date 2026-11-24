"""Settings for one run of the demo app."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["DemoConfig", "POSTGRES_DSN_ENV", "POSTGRES_DEFAULT_DSN"]

POSTGRES_DSN_ENV = "DBLAYER_TEST_POSTGRES"
POSTGRES_DEFAULT_DSN = "postgresql:///dblayer_test?host=/tmp"


def _env_dsn() -> str:
    return os.environ.get(POSTGRES_DSN_ENV, POSTGRES_DEFAULT_DSN)


@dataclass(frozen=True)
class DemoConfig:
    data_dir: Path = Path("demo-data")
    postgres_dsn: str = field(default_factory=_env_dsn)
    """``--postgres-dsn``, else ``$DBLAYER_TEST_POSTGRES``, else the local
    server's socket in /tmp as the current OS user. Must name the
    ``dblayer_test`` database."""
    fleet_size: int = 1000
    """Databases in the fleet (0 for none)."""
    fleet_max_open_connections: int = 40
    """The registry's global cap, far below ``fleet_size``."""
    fleet_max_threads: int = 8
    fleet_idle_timeout: float = 12.0
    """Seconds before an unused connection closes; with its last connection
    gone a database is asleep."""
