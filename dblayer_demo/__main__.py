"""``python -m dblayer_demo [--port 8001] [--data-dir demo-data]``"""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .app import create_app


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
    args = parser.parse_args(argv)
    uvicorn.run(create_app(args.data_dir), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
