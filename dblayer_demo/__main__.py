"""``python -m dblayer_demo [--port 8001] [--host ADDR ...] [--data-dir demo-data] [--postgres-dsn DSN]``"""

from __future__ import annotations

import argparse
import socket
from pathlib import Path

import uvicorn

from .app import create_app
from .config import POSTGRES_DEFAULT_DSN, POSTGRES_DSN_ENV, DemoConfig, _env_dsn


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m dblayer_demo", description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument(
        "--host",
        action="append",
        default=None,
        help="address to listen on (default 127.0.0.1); repeat to listen on several, "
        "e.g. --host 127.0.0.1 --host 100.64.0.1, in one server process",
    )
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
    parser.add_argument(
        "--fleet-size", type=int, default=1000, help="databases in the fleet (default 1000; 0 for none)"
    )
    args = parser.parse_args(argv)
    config = DemoConfig(
        data_dir=args.data_dir,
        postgres_dsn=args.postgres_dsn or _env_dsn(),
        fleet_size=args.fleet_size,
    )
    hosts: list[str] = args.host or ["127.0.0.1"]
    app = create_app(config)
    if len(hosts) == 1:
        uvicorn.run(app, host=hosts[0], port=args.port)
        return
    # Several addresses, one process: the databases must not be opened by two
    # servers at once (DuckDB locks its file per process), so bind every
    # address here and hand the sockets to a single uvicorn server.
    sockets = [_listen(host, args.port) for host in hosts]
    server = uvicorn.Server(uvicorn.Config(app, host=hosts[0], port=args.port))
    for host in hosts:
        print(f"dblayer demo listening on http://{host}:{args.port}")
    server.run(sockets=sockets)


def _listen(host: str, port: int) -> socket.socket:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.set_inheritable(True)
    return sock


if __name__ == "__main__":
    main()
