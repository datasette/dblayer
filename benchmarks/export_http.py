"""End-to-end export over HTTP with ``curl``: ``python -m benchmarks.export --http``.

1. ``benchmarks/handoff_server.py`` (a minimal asyncio HTTP/1.1 server) on
   the C worker, one server process per C-worker transport: fallback,
   native over codec frames, raw frames, the fifo side channel, and the
   socket handoff.
2. The demo app under uvicorn: ``/sqlite-cworker/-/query.csv`` (and
   DuckDB, PostgreSQL) with the backend's bytes against ``_native=0``.

Server CPU is read with ``ps -o cputime`` before and after each request
(centisecond resolution; the C worker is a separate process and not
included). Ports 8793 to 8799 are used; 8001 and 8765 are left alone.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

from .export import WIDE_DUCKDB, WIDE_PG, WIDE_SQLITE, sqlite_path

ROOT = Path(__file__).resolve().parent.parent


def cputime(pid: int) -> float:
    out = subprocess.run(["ps", "-o", "cputime=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    parts = out.replace("-", ":").split(":")
    secs = 0.0
    for p in parts:
        secs = secs * 60 + float(p)
    return secs


def wait_port(port: int, timeout: float = 60) -> None:
    import socket

    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"port {port} did not open")


def curl(url: str) -> tuple[float, int, int]:
    """(total seconds, bytes, curl exit code)."""
    p = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{time_total} %{size_download}", url],
                       capture_output=True, text=True)
    t, n = p.stdout.split()
    return float(t), int(n), p.returncode


def measure(pid: int, url: str, repeat: int = 3) -> tuple[float, float, int]:
    best = None
    for _ in range(repeat):
        c0 = cputime(pid)
        t, n, rc = curl(url)
        time.sleep(0.05)
        cpu = cputime(pid) - c0
        if rc != 0:
            raise RuntimeError(f"curl failed ({rc}) for {url}")
        if best is None or t < best[0]:
            best = (t, cpu, n)
    assert best is not None
    return best


def main() -> None:
    n = int(os.environ.get("BENCH_ROWS", "1000000"))
    db = sqlite_path(n)
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    procs: list[subprocess.Popen[bytes]] = []
    print(f"# load average at start: {os.getloadavg()}")
    print("server\tmode\tformat\twall_s\tserver_cpu_s\tMB\tMB_s\tload1")
    try:
        servers = {}
        for i, transport in enumerate(["raw", "frames", "fifo"]):
            port = 8793 + i
            p = subprocess.Popen([sys.executable, "-m", "benchmarks.handoff_server", str(db), "--port", str(port),
                                  "--transport", transport], env=env, cwd=ROOT,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            procs.append(p)
            servers[transport] = (port, p.pid)
        for port, _ in servers.values():
            wait_port(port)
        sql = "select * from wide order by 1"
        for fmt in ("csv", "json"):
            runs = [("fallback", "raw"), ("datasette-like", None), ("native", "frames"), ("native", "raw"),
                    ("native", "fifo"), ("handoff", "raw")]
            for mode, transport in runs:
                if transport is None:
                    continue
                port, pid = servers[transport]
                q = urllib.parse.urlencode({"sql": sql, "format": fmt, "mode": mode})
                t, cpu, nbytes = measure(pid, f"http://127.0.0.1:{port}/export?{q}")
                label = f"{mode}/{transport}" if mode == "native" else mode
                print(f"handoff_server\t{label}\t{fmt}\t{t:.3f}\t{cpu:.2f}\t{nbytes / 1e6:.1f}\t"
                      f"{nbytes / t / 1e6:.0f}\t{os.getloadavg()[0]:.1f}", flush=True)
        # the demo app under uvicorn
        data = Path(tempfile.mkdtemp(prefix="dblayer-bench-demo-"))
        port = 8797
        p = subprocess.Popen([sys.executable, "-m", "dblayer_demo", "--port", str(port), "--data-dir", str(data),
                              "--fleet-size", "0"], env=env, cwd=ROOT,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)
        wait_port(port, 120)
        gen = {"sqlite-cworker": WIDE_SQLITE, "duckdb": WIDE_DUCKDB, "postgres-psycopg": WIDE_PG}
        for slug, tmpl in gen.items():
            for fmt in ("csv", "json"):
                for native in ("1", "0"):
                    q = urllib.parse.urlencode({"sql": tmpl.format(n=n), "_size": "max", "_native": native})
                    try:
                        t, cpu, nbytes = measure(p.pid, f"http://127.0.0.1:{port}/{slug}/-/query.{fmt}?{q}", repeat=2)
                    except RuntimeError as e:
                        print(f"uvicorn-demo\t{slug}\t{fmt}\tfailed: {e}")
                        continue
                    mode = "native" if native == "1" else "fallback"
                    print(f"uvicorn-demo/{slug}\t{mode}\t{fmt}\t{t:.3f}\t{cpu:.2f}\t{nbytes / 1e6:.1f}\t"
                          f"{nbytes / t / 1e6:.0f}\t{os.getloadavg()[0]:.1f}", flush=True)
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
