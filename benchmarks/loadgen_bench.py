"""The two multi-tenant SQLite workers (C ``dblayer-cworker --multi`` and the
Rust port) driven by a load generator that is not Python
(``sqlite-rs-worker/examples/loadgen.rs``), so the parent's event loop is not
the limit and the worker's own CPU per request shows.

    cargo build --release --example loadgen --manifest-path sqlite-rs-worker/Cargo.toml
    uv run python benchmarks/loadgen_bench.py [--quick] [--json out.json]

For each round, clients in (1, 4, 16, 64) and codec, both workers run in
alternating order (one request in flight per client, each client on its
own database, one-row read by primary key). Medians over rounds of
requests/s, worker CPU (cores) and worker CPU per request (µs), with the
load average of each run.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
LOADGEN = ROOT / "sqlite-rs-worker/target/release/examples/loadgen"
BINARIES = {
    "c": Path(os.environ.get("DBLAYER_CWORKER", ROOT / "cworker/build/dblayer-cworker")),
    "rust": Path(os.environ.get("DBLAYER_RSWORKER", ROOT / "sqlite-rs-worker/target/release/dblayer-sqlite-rsworker")),
}
CLIENTS = (1, 4, 16, 64)


def run_one(binary: Path, clients: int, secs: float, codec: str) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="dblayer-loadgen-") as d:
        load = os.getloadavg()[0]
        p = subprocess.run([str(LOADGEN), str(binary), str(clients), str(secs), d, codec],
                           capture_output=True, text=True, timeout=secs * 10 + 120, check=True)
        r = json.loads(p.stdout.strip().splitlines()[-1])
        r["load1"] = round(load, 2)
        return r


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--rounds", type=int)
    ap.add_argument("--seconds", type=float)
    ap.add_argument("--codecs", default="msgpack,json")
    ap.add_argument("--json", type=Path)
    a = ap.parse_args()
    rounds = a.rounds or (1 if a.quick else 3)
    secs = a.seconds or (2.0 if a.quick else 8.0)
    missing = [f"{k}: {v}" for k, v in {"loadgen": LOADGEN, **BINARIES}.items() if not v.exists()]
    if missing:
        sys.exit("not built: " + "; ".join(missing))
    raw: dict[str, list[dict[str, Any]]] = {}
    for rnd in range(rounds):
        for codec in a.codecs.split(","):
            for clients in CLIENTS:
                order = ["c", "rust"] if (rnd + clients) % 2 == 0 else ["rust", "c"]
                for impl in order:
                    r = run_one(BINARIES[impl], clients, secs, codec)
                    raw.setdefault(f"{impl}/{codec}/{clients}", []).append(r)
                    print(f"round {rnd + 1} {impl} {codec} {clients}: {json.dumps(r)}", flush=True)
    summary: dict[str, dict[str, Any]] = {}
    for key, rs in raw.items():
        summary[key] = {
            "rps": statistics.median(r["rps"] for r in rs),
            "worker_cpu_cores": statistics.median(r["worker_cpu_cores"] for r in rs),
            "worker_cpu_us_per_request": statistics.median(r["worker_cpu_us_per_request"] for r in rs),
            "driver_cpu_cores": statistics.median(r["driver_cpu_cores"] for r in rs),
            "load1": [r["load1"] for r in rs],
        }
    print(f"\n{rounds} round(s) of {secs:g} s per cell; medians\n")
    print("| codec | clients | C req/s | Rust req/s | Rust / C | C worker CPU (cores) | Rust worker CPU (cores) "
          "| C CPU µs/req | Rust CPU µs/req |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for codec in a.codecs.split(","):
        for clients in CLIENTS:
            c, r = summary[f"c/{codec}/{clients}"], summary[f"rust/{codec}/{clients}"]
            print(f"| {codec} | {clients} | {c['rps']:,.0f} | {r['rps']:,.0f} | {r['rps'] / c['rps']:.2f} | "
                  f"{c['worker_cpu_cores']:.2f} | {r['worker_cpu_cores']:.2f} | "
                  f"{c['worker_cpu_us_per_request']:.1f} | {r['worker_cpu_us_per_request']:.1f} |")
    if a.json:
        a.json.write_text(json.dumps({"summary": summary, "runs": raw, "rounds": rounds, "seconds": secs}, indent=1))


if __name__ == "__main__":
    main()
