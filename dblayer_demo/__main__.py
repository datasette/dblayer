"""``python -m dblayer_demo [--port 8001] [--data-dir demo-data] [--postgres-dsn DSN]``"""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .app import create_app
from .config import POSTGRES_DEFAULT_DSN, POSTGRES_DSN_ENV, DemoConfig, _env_dsn


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m dblayer_demo", description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("demo-data"),
        help="where the demo SQLite and DuckDB files are created (default: demo-data)",
    )
    parser.add_argument(
        "--postgres-dsn",
        default=None,
        help=f"PostgreSQL DSN naming the dblayer_test database (default: ${POSTGRES_DSN_ENV}, "
        f"else {POSTGRES_DEFAULT_DSN}, which connects as the current OS user)",
    )
    args = parser.parse_args(argv)
    config = DemoConfig(data_dir=args.data_dir, postgres_dsn=args.postgres_dsn or _env_dsn())
    uvicorn.run(create_app(config), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
