"""Wake cost and query cost, reported separately, for every scale-test
backend (``tests/scale/harness.py``'s ``SCALE_BACKENDS``), in one session
under identical settings.

    uv run --all-extras python benchmarks/scale_wake_query.py [--n 200] [--json out.json]

The scale test reports requests per second for a mixed load, where a
request that lands on an asleep database pays for the wake and the query
together. This separates them. For each backend, ``--n`` databases are
registered in one ``Registry`` (same cap, threads and settings for all);
then, per database:

* ``cold``: the first ``select count(*) from t`` on an asleep database
  (wake plus query);
* ``warm``: the same statement again on the now-awake database, repeated
  ``--repeat`` times (query only);
* ``sleep``: ``db.sleep()``.

``wake = median(cold) - median(warm)``. Medians and p90 in milliseconds.
Backends whose binary or package is missing are reported as skipped with
the reason. The load average is recorded before and after each backend.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dblayer import Database, Registry  # noqa: E402
from tests.scale.harness import SCALE_BACKENDS, ScaleBackend  # noqa: E402

SQL = "select count(*) from t"


def _pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


async def measure(spec: ScaleBackend, root: Path, n: int, repeat: int) -> dict[str, Any]:
    paths = spec.create_files(root / spec.id, n)
    registry = Registry(max_open_connections=64, max_threads=8, idle_timeout=None)
    cold: list[float] = []
    warm: list[float] = []
    sleep: list[float] = []
    try:
        dbs = [registry.add(f"t{i}/db", Database(spec.make(p))) for i, p in enumerate(paths)]
        # One throwaway wake so one-off costs (imports, a pool's first
        # process start, module compile) are not charged to database 0.
        await dbs[-1].execute(SQL)
        await dbs[-1].sleep()
        for db in dbs[:-1]:
            t0 = time.perf_counter()
            await db.execute(SQL)
            cold.append(time.perf_counter() - t0)
            for _ in range(repeat):
                t0 = time.perf_counter()
                await db.execute(SQL)
                warm.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            await db.sleep()
            sleep.append(time.perf_counter() - t0)
    finally:
        await registry.close()
        for pools in ("_cpools", "_rspools"):
            import tests.scale.harness as h

            for pool in getattr(h, pools):
                close = getattr(pool, "close", None)
                if close is not None:
                    r = close()
                    if asyncio.iscoroutine(r):
                        await r
            getattr(h, pools).clear()
    ms = 1000.0
    out = {
        "databases": len(cold),
        "cold_median_ms": statistics.median(cold) * ms,
        "cold_p90_ms": _pct(cold, 0.9) * ms,
        "warm_median_ms": statistics.median(warm) * ms,
        "warm_p90_ms": _pct(warm, 0.9) * ms,
        "sleep_median_ms": statistics.median(sleep) * ms,
    }
    out["wake_ms"] = out["cold_median_ms"] - out["warm_median_ms"]
    return out


async def main_async(n: int, repeat: int, only: list[str] | None) -> dict[str, Any]:
    results: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="dblayer-wake-query-") as tmp:
        for spec in SCALE_BACKENDS:
            if only and spec.id not in only:
                continue
            reason = spec.available()
            if reason:
                results[spec.id] = {"skipped": reason}
                print(f"{spec.id}: skipped: {reason}", flush=True)
                continue
            before = os.getloadavg()[0]
            try:
                r = await measure(spec, Path(tmp), n, repeat)
            except Exception as e:  # one backend failing must not lose the others
                r = {"error": f"{type(e).__name__}: {e}"}
            r["load1_before"] = before
            r["load1_after"] = os.getloadavg()[0]
            results[spec.id] = r
            print(f"{spec.id}: {json.dumps(r)}", flush=True)
    return results


def print_table(results: dict[str, Any]) -> None:
    cols = ["wake_ms", "warm_median_ms", "warm_p90_ms", "cold_median_ms", "cold_p90_ms", "sleep_median_ms",
            "load1_before", "load1_after"]
    print("\n| backend | " + " | ".join(cols) + " |")
    print("| --- |" + " --- |" * len(cols))
    for name, r in results.items():
        if "skipped" in r or "error" in r:
            print(f"| {name} | " + (r.get("skipped") or r.get("error")) + " |" + " |" * (len(cols) - 1))
            continue
        print(f"| {name} | " + " | ".join(f"{r[c]:.3f}" if c.endswith("ms") else f"{r[c]:.2f}" for c in cols) + " |")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=200, help="databases per backend")
    p.add_argument("--repeat", type=int, default=5, help="warm queries per database")
    p.add_argument("--only", help="comma-separated backend ids")
    p.add_argument("--json", type=Path)
    a = p.parse_args()
    results = asyncio.run(main_async(a.n, a.repeat, a.only.split(",") if a.only else None))
    print_table(results)
    if a.json:
        a.json.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
