"""Run every benchmark in turn, one at a time, and write one set of results.

    uv run --all-extras --group demo python benchmarks/run_all.py [--quick] [--out DIR] [--only a,b] [--list]

Each topic (an existing benchmark script, or the scale tests through their
harness) runs alone in a subprocess with its own timeout; the whole process
group is killed on timeout, so one hang or crash costs only that topic. A
topic whose backend is not built or installed is recorded as skipped with
the reason, never left out silently.

Results go to ``DIR/<UTC date>-<commit>[-quick]/`` (default ``DIR`` is
``benchmarks/results``):

* ``results.jsonl``: one JSON object per measurement. Every line carries
  the topic, the measurement's key path and value, and the run's context:
  machine (CPU model, cores, memory, kernel, filesystem of the data
  directory), git commit, tool and driver versions, and the 1-minute load
  average before and after the topic;
* ``results.md``: the same run for people, one section per topic with its
  table (the script's own Markdown table where it prints one, else a table
  built from its JSON, else its output verbatim);
* ``logs/<topic>.out`` and ``.err``: each script's full output;
  ``raw/<topic>.json``: each script's own JSON, where it writes one.

Comparisons should be made within one results directory: every topic in it
ran in one session on one machine under the same settings. ``--quick`` is a
few-minute trial of the mechanism; its figures are not final numbers.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
PG_DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")


# -- availability checks: None if the topic can run, else the reason ----------------------


def _import(mod: str) -> str | None:
    try:
        __import__(mod)
    except Exception as e:  # ImportError, or a broken binary wheel
        return f"Python package {mod!r} not importable: {type(e).__name__}: {e}"
    return None


def _backend_check(module: str, fn: str) -> Callable[[], str | None]:
    def check() -> str | None:
        sys.path.insert(0, str(ROOT))
        try:
            m = __import__(f"dblayer.backends.{module}", fromlist=[fn])
            return getattr(m, fn)()
        except Exception as e:
            return f"{module}.{fn}() failed: {type(e).__name__}: {e}"

    return check


cworker = _backend_check("sqlite_cworker", "binary_unavailable")
rsworker = _backend_check("sqlite_rspool", "binary_unavailable")
wasm = _backend_check("sqlite_wasm", "module_unavailable")
nodeworker = _backend_check("sqlite_nodeworker", "unavailable")
pglite = _backend_check("pglite_worker", "unavailable")


def postgres() -> str | None:
    if r := _import("psycopg"):
        return r
    import psycopg

    try:
        with psycopg.connect(PG_DSN, connect_timeout=5) as c:
            c.execute("select 1")
    except Exception as e:
        return f"PostgreSQL not reachable at $DBLAYER_TEST_POSTGRES: {type(e).__name__}: {e}"
    return None


def all_of(*checks: Callable[[], str | None]) -> Callable[[], str | None]:
    def check() -> str | None:
        reasons = [r for c in checks if (r := c())]
        return "; ".join(reasons) or None

    return check


# -- topics ------------------------------------------------------------------------------------


@dataclass
class Topic:
    name: str
    title: str
    args: list[str]
    """Command after the interpreter; ``{json}`` is replaced by the raw JSON path."""
    quick: list[str] = field(default_factory=list)
    full: list[str] = field(default_factory=list)
    timeout: tuple[int, int] = (600, 3600)
    """Seconds: (quick, full)."""
    requires: Callable[[], str | None] = lambda: None
    env: dict[str, str] = field(default_factory=dict)
    quick_env: dict[str, str] = field(default_factory=dict)
    module: bool = False
    pytest: bool = False


TOPICS: list[Topic] = [
    Topic("worker_overhead", "Worker process boundary against in-process SQLite",
          ["benchmarks/worker_overhead.py", "--json", "{json}"], quick=["--quick"], timeout=(900, 3600)),
    Topic("cpool", "Multi-tenant C pool (sqlite-cpool)",
          ["benchmarks/cpool_bench.py", "--json", "{json}"], quick=["--quick"], requires=cworker),
    Topic("rspool", "Multi-tenant C worker against its Rust port (sqlite-cpool, sqlite-rspool)",
          ["benchmarks/rspool_bench.py", "--json", "{json}"], quick=["--quick", "--rounds", "2"],
          requires=all_of(cworker, rsworker)),
    Topic("wasm", "SQLite in WebAssembly (sqlite-wasm)",
          ["benchmarks/wasm_overhead.py", "--json", "{json}"], quick=["--quick"], requires=all_of(wasm, lambda: _import("wasmtime"))),
    Topic("nodeworker", "SQLite-WASM in a Node.js worker (sqlite-nodeworker)",
          ["benchmarks/nodeworker_bench.py", "--json", "{json}"], quick=["--quick"], requires=nodeworker),
    Topic("duckdb_memory", "DuckDB memory after sleep, in-process against workers",
          ["benchmarks/duckdb_memory.py", "all", "--json", "{json}", "--max-load", "2"],
          quick=["--n", "20", "--cycles", "1", "--rows", "20000"], timeout=(900, 5400),
          requires=lambda: _import("duckdb")),
    Topic("postgres_drivers", "PostgreSQL drivers: asyncpg against psycopg 3",
          ["benchmarks/bench_postgres_drivers.py"], quick=["--runs", "2"],
          requires=all_of(postgres, lambda: _import("asyncpg"))),
    Topic("pglite", "PGlite worker against psycopg and the SQLite workers",
          ["benchmarks/pglite_bench.py"], quick=["--rows", "10000", "--awake", "4"], timeout=(900, 3600),
          requires=pglite),
    Topic("clickhouse", "ClickHouse (chDB) against DuckDB and SQLite",
          ["benchmarks/clickhouse.py"], quick=["--rows", "100000"], full=["--big"], timeout=(900, 3600),
          requires=all_of(lambda: _import("chdb"), lambda: _import("duckdb"))),
    Topic("export", "db.export(): native serialisation against the Python fallback",
          ["benchmarks.export", "--out", "{jsonl}"], quick=["--quick", "--sizes", "10000,100000"],
          module=True, timeout=(900, 5400)),
    Topic("scale_wake_query", "Scale backends: wake cost and query cost, separately",
          ["benchmarks/scale_wake_query.py", "--json", "{json}"], quick=["--n", "100"], full=["--n", "1000"],
          timeout=(900, 3600)),
    Topic("scale_tests", "Scale tests (tests/scale, through their harness)",
          ["-m", "pytest", "tests/scale", "-m", "scale", "-s", "-q", "-p", "no:cacheprovider", "--timeout=1800"],
          quick_env={"DBLAYER_SCALE_N": "300"}, timeout=(1200, 5400), pytest=True),
]


# -- context: machine, commit, versions ----------------------------------------------------------


def _run(cmd: list[str], **kw: Any) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=30, **kw)
        return (p.stdout or p.stderr).strip()
    except Exception as e:
        return f"unavailable ({type(e).__name__})"


def machine_description(data_dir: Path) -> dict[str, Any]:
    d: dict[str, Any] = {
        "platform": platform.platform(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "cpu_count": os.cpu_count(),
        "hostname_is_fly": bool(os.environ.get("FLY_MACHINE_ID")),
    }
    if Path("/proc/cpuinfo").exists():
        info = Path("/proc/cpuinfo").read_text()
        m = re.search(r"^model name\s*:\s*(.+)$", info, re.M)
        d["cpu_model"] = m.group(1).strip() if m else None
        mem = re.search(r"^MemTotal:\s*(\d+) kB", Path("/proc/meminfo").read_text(), re.M)
        d["memory_gb"] = round(int(mem.group(1)) / 1024 / 1024, 1) if mem else None
        d["filesystem"] = _run(["findmnt", "-n", "-o", "SOURCE,FSTYPE,OPTIONS", "-T", str(data_dir)])
        d["tmp_filesystem"] = _run(["findmnt", "-n", "-o", "SOURCE,FSTYPE", "-T", os.environ.get("TMPDIR", "/tmp")])
        d["fly_vm_size"] = os.environ.get("FLY_VM_SIZE") or os.environ.get("BENCH_VM_SIZE")
        d["fly_region"] = os.environ.get("FLY_REGION")
    elif sys.platform == "darwin":
        d["cpu_model"] = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        d["memory_gb"] = round(int(_run(["sysctl", "-n", "hw.memsize"]) or 0) / 2**30, 1)
        d["filesystem"] = _run(["df", "-h", str(data_dir)]).splitlines()[-1]
    return d


def git_commit() -> dict[str, str]:
    return {
        "commit": _run(["git", "rev-parse", "HEAD"], cwd=ROOT),
        "describe": _run(["git", "log", "-1", "--format=%h %cI %s"], cwd=ROOT),
        "dirty": _run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT) != "",
    }


def versions() -> dict[str, Any]:
    import sqlite3

    v: dict[str, Any] = {
        "python": sys.version.split()[0],
        "sqlite (python sqlite3)": sqlite3.sqlite_version,
        "uv": _run(["uv", "--version"]),
        "node": _run(["node", "--version"]),
        "rustc": _run(["rustc", "--version"]),
        "cargo": _run(["cargo", "--version"]),
        "cc": _run(["cc", "--version"]).splitlines()[0] if shutil.which("cc") else None,
        "postgres (psql client)": _run(["psql", "--version"]),
    }
    for pkg in ("duckdb", "asyncpg", "psycopg", "psycopg-binary", "apsw", "wasmtime", "chdb", "msgpack",
                "pytest", "starlette", "uvicorn"):
        try:
            v[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            v[pkg] = None
    try:
        import psycopg

        with psycopg.connect(PG_DSN, connect_timeout=5) as c:
            v["postgres server"] = c.execute("show server_version").fetchone()[0]
        v["libpq"] = psycopg.pq.version()
    except Exception as e:
        v["postgres server"] = f"unavailable ({type(e).__name__})"
    for name, path in (("sqlite (C worker)", ROOT / "cworker/build/dblayer-cworker"),
                       ("sqlite-rsworker", ROOT / "sqlite-rs-worker/target/release/dblayer-sqlite-rsworker"),
                       ("duckdb-rsworker", ROOT / "duckdb-rs-worker/target/release/dblayer-duckdb-rsworker"),
                       ("duckdb-cppworker", ROOT / "duckdb-cpp-worker/build/dblayer-duckdb-worker")):
        v[name] = _run([str(path), "--version"]) if path.exists() else "not built"
    return v


def load1() -> float:
    return round(os.getloadavg()[0], 2)


# -- output adapters ----------------------------------------------------------------------------


def flatten(obj: Any, prefix: str = "") -> list[tuple[str, Any]]:
    """Leaf values of nested JSON as (dotted key path, value)."""
    if isinstance(obj, dict):
        out: list[tuple[str, Any]] = []
        for k, v in obj.items():
            out += flatten(v, f"{prefix}.{k}" if prefix else str(k))
        return out
    if isinstance(obj, list) and obj and all(isinstance(x, (dict, list)) for x in obj):
        out = []
        for i, v in enumerate(obj):
            out += flatten(v, f"{prefix}[{i}]")
        return out
    return [(prefix, obj)]


MD_TABLE = re.compile(r"(?:^\|.*\|[ \t]*\n){2,}", re.M)


def markdown_tables(text: str) -> list[str]:
    return [m.group(0).rstrip() for m in MD_TABLE.finditer(text) if "---" in m.group(0)]


def tsv_table(text: str) -> str | None:
    """export.py prints a tab-separated table: convert it to Markdown."""
    rows = [line.split("\t") for line in text.splitlines() if "\t" in line]
    if len(rows) < 2:
        return None
    w = len(rows[0])
    rows = [r for r in rows if len(r) == w]
    return "\n".join(["| " + " | ".join(rows[0]) + " |", "|" + " --- |" * w]
                     + ["| " + " | ".join(r) + " |" for r in rows[1:]])


def json_table(obj: Any) -> str | None:
    """A table from {name: {flat measures}} JSON (e.g. scale_wake_query)."""
    if not isinstance(obj, dict) or not obj or not all(isinstance(v, dict) for v in obj.values()):
        return None
    cols: list[str] = []
    for v in obj.values():
        for k, x in v.items():
            if not isinstance(x, (dict, list)) and k not in cols:
                cols.append(k)
    if len(cols) > 14:
        return None

    def cell(x: Any) -> str:
        if x is None:
            return ""
        if isinstance(x, float):
            return f"{x:.4g}"
        return str(x).replace("|", "/").replace("\n", " ")

    lines = ["| | " + " | ".join(cols) + " |", "| --- |" + " --- |" * len(cols)]
    lines += [f"| {k} | " + " | ".join(cell(v.get(c)) for c in cols) + " |" for k, v in obj.items()]
    return "\n".join(lines)


SCALE_BLOCK = re.compile(r"^\[(?:scale|threads)[^\]]*\].*?(?=^\[|^[=.FEsx]|\Z)", re.M | re.S)


# -- running ------------------------------------------------------------------------------------


def run_topic(t: Topic, quick: bool, out: Path) -> dict[str, Any]:
    raw = out / "raw" / f"{t.name}.json"
    jsonl = out / "raw" / f"{t.name}.jsonl"
    args = [a.replace("{json}", str(raw)).replace("{jsonl}", str(jsonl)) for a in t.args]
    args += t.quick if quick else t.full
    cmd = [PY, "-m", *args] if t.module else [PY, *args]
    env = dict(os.environ, PYTHONUNBUFFERED="1", **t.env, **(t.quick_env if quick else {}))
    timeout = t.timeout[0 if quick else 1]
    rec: dict[str, Any] = {"topic": t.name, "command": " ".join(cmd[1:]), "timeout_s": timeout}
    reason = t.requires()
    if reason:
        rec.update(status="skipped", reason=reason)
        return rec
    rec["load1_before"] = load1()
    t0 = time.monotonic()
    stdout_f = open(out / "logs" / f"{t.name}.out", "w")
    stderr_f = open(out / "logs" / f"{t.name}.err", "w")
    p = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=stdout_f, stderr=stderr_f, start_new_session=True)
    try:
        rc = p.wait(timeout=timeout)
        rec["status"] = "ok" if rc == 0 else "failed"
        rec["returncode"] = rc
    except subprocess.TimeoutExpired:
        rec["status"] = "timeout"
        try:
            os.killpg(p.pid, signal.SIGTERM)
            p.wait(10)
        except Exception:
            pass
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()
    finally:
        stdout_f.close()
        stderr_f.close()
        # Anything the topic left behind in its process group (workers).
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    rec["seconds"] = round(time.monotonic() - t0, 1)
    rec["load1_after"] = load1()
    if rec["status"] != "ok":
        rec["reason"] = (out / "logs" / f"{t.name}.err").read_text()[-2000:]
    return rec


def measurements(t: Topic, out: Path) -> tuple[list[tuple[str, Any]], str]:
    """(measurements, Markdown body) from a finished topic's outputs."""
    text = (out / "logs" / f"{t.name}.out").read_text(errors="replace")
    raw = out / "raw" / f"{t.name}.json"
    jsonl = out / "raw" / f"{t.name}.jsonl"
    ms: list[tuple[str, Any]] = []
    body: list[str] = []
    obj = None
    if raw.exists():
        try:
            obj = json.loads(raw.read_text())
            ms = flatten(obj)
        except ValueError:
            pass
    if jsonl.exists():
        rows = [json.loads(line) for line in jsonl.read_text().splitlines() if line.strip()]
        for i, r in enumerate(rows):
            key = "/".join(str(r.get(k)) for k in ("backend", "table", "rows", "format", "mode", "transport"))
            ms += [(f"{key}.{k}", v) for k, v in r.items()]
    if t.pytest:
        blocks = [b.strip() for b in SCALE_BLOCK.findall(text)]
        for b in blocks:
            ms.append((b.split("]")[0].lstrip("["), b))
        tail = [line for line in text.splitlines() if re.search(r"\d+ (passed|failed|skipped)", line)]
        body.append("```\n" + ("\n\n".join(blocks) or "(no figures printed)") + "\n```")
        if tail:
            body.append("pytest: `" + tail[-1].strip("= ") + "`")
        skips = [line for line in text.splitlines() if line.startswith("SKIPPED")]
        if skips:
            body.append("\n".join(f"- {s}" for s in skips))
        return ms, "\n\n".join(body)
    tables = markdown_tables(text)
    if tables:
        body += tables
    elif (tsv := tsv_table(text)) is not None:
        body.append(tsv)
    elif (jt := json_table(obj)) is not None:
        body.append(jt)
    if not ms:  # no JSON: keep the printed output as one measurement
        ms = [("stdout", text)]
    if not tables or t.name in ("clickhouse", "pglite", "duckdb_memory", "nodeworker", "cpool", "wasm"):
        # Scripts whose figures are partly outside a table: show the output too.
        body.append("<details><summary>output</summary>\n\n```\n" + text.strip()[-12000:] + "\n```\n</details>")
    return ms, "\n\n".join(body)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--quick", action="store_true", help="a few-minute trial of the mechanism")
    ap.add_argument("--out", type=Path, default=ROOT / "benchmarks" / "results")
    ap.add_argument("--only", help="comma-separated topic names")
    ap.add_argument("--skip", help="comma-separated topic names to leave out (recorded as skipped)")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--label", default="", help="free text recorded in every line, e.g. 'trial'")
    ap.add_argument("--settle", type=float, default=None,
                    help="before each topic, wait up to this many seconds for the 1-minute load average to fall "
                         "below --settle-load (default: 0 with --quick, 90 otherwise)")
    ap.add_argument("--settle-load", type=float, default=0.5)
    a = ap.parse_args()
    if a.settle is None:
        a.settle = 0 if a.quick else 90
    if a.list:
        for t in TOPICS:
            print(f"{t.name:18} {t.title}")
        return
    only = set(a.only.split(",")) if a.only else None
    skip = set(a.skip.split(",")) if a.skip else set()
    commit = git_commit()
    stamp = dt.datetime.now(dt.UTC)
    out = a.out / f"{stamp:%Y-%m-%dT%H%MZ}-{commit['commit'][:10]}{'-quick' if a.quick else ''}"
    (out / "logs").mkdir(parents=True, exist_ok=True)
    (out / "raw").mkdir(exist_ok=True)
    context = {
        "run": {"started": stamp.isoformat(timespec="seconds"), "mode": "quick" if a.quick else "full",
                "label": a.label, "results_dir": out.name},
        "git": commit,
        "machine": machine_description(out),
        "versions": versions(),
    }
    (out / "context.json").write_text(json.dumps(context, indent=2))
    print(json.dumps(context, indent=2), flush=True)
    jsonl = open(out / "results.jsonl", "w")
    sections: list[str] = []
    summary: list[dict[str, Any]] = []
    for t in TOPICS:
        if only is not None and t.name not in only:
            continue
        if t.name in skip:
            rec = {"topic": t.name, "status": "skipped", "reason": "left out with --skip"}
        else:
            settle_until = time.monotonic() + a.settle
            while load1() > a.settle_load and time.monotonic() < settle_until:
                time.sleep(5)
            print(f"\n=== {t.name}: {t.title} (load {load1()})", flush=True)
            rec = run_topic(t, a.quick, out)
        print(f"=== {t.name}: {rec['status']} in {rec.get('seconds', 0)} s"
              + (f": {rec['reason'][-300:]}" if rec.get("reason") else ""), flush=True)
        summary.append(rec)
        base = {**rec, **context}
        if rec["status"] in ("ok", "failed", "timeout") and (out / "logs" / f"{t.name}.out").exists():
            ms, body = measurements(t, out)
        else:
            ms, body = [], ""
        jsonl.write(json.dumps({**base, "key": "_status", "value": rec["status"]}) + "\n")
        for k, v in ms:
            jsonl.write(json.dumps({**base, "key": k, "value": v}, default=str) + "\n")
        jsonl.flush()
        head = f"## {t.title}\n\n`{t.name}`: **{rec['status']}**"
        if "seconds" in rec:
            head += f" in {rec['seconds']} s, load average (1 min) {rec['load1_before']} before, {rec['load1_after']} after"
        if rec.get("reason"):
            reason = rec["reason"].strip().replace("\n", "\n    ")
            head += f"\n\nReason:\n\n    {reason[-1500:]}"
        sections.append(head + ("\n\n" + body if body else ""))
    jsonl.close()
    m, g, v = context["machine"], context["git"], context["versions"]
    md = [f"# dblayer benchmarks: {out.name}", ""]
    if a.label:
        md += [f"**{a.label}**", ""]
    md += [
        f"- Mode: {context['run']['mode']}; started {context['run']['started']}",
        f"- Commit: `{g['commit']}` ({g['describe']}){' (dirty tree)' if g['dirty'] else ''}",
        f"- Machine: {m.get('cpu_model')}, {m.get('cpu_count')} CPUs, {m.get('memory_gb')} GB, "
        f"kernel {m.get('kernel')}, {m.get('arch')}"
        + (f", Fly {m.get('fly_vm_size')} in {m.get('fly_region')}" if m.get("fly_region") else ""),
        f"- Data filesystem: `{m.get('filesystem')}`; temporary files: `{m.get('tmp_filesystem', '')}`",
        "- Versions: " + "; ".join(f"{k} {val}" for k, val in v.items() if val),
        "",
        "Every topic ran alone, one after another, in this one session. Compare figures within this file only.",
        "",
        "| topic | status | seconds | load before | load after | reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in summary:
        reason = (r.get("reason") or "").strip().splitlines()
        md.append(f"| {r['topic']} | {r['status']} | {r.get('seconds', '')} | {r.get('load1_before', '')} | "
                  f"{r.get('load1_after', '')} | {(reason[-1] if reason else '')[:200].replace('|', '/')} |")
    md += ["", *("\n".join([s, ""]) for s in sections)]
    (out / "results.md").write_text("\n".join(md))
    print(f"\nwrote {out}/results.md and results.jsonl", flush=True)
    print(f"RESULTS_DIR={out}", flush=True)


if __name__ == "__main__":
    main()
