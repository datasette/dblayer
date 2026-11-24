"""Split the suite into backend families for CI: ``DBLAYER_CI_SHARD=workers``
runs only the tests of that family (``core,duckdb``: of those families). Every collected test belongs to exactly
one family, so the shard jobs together run the whole suite once.

A test's family comes from its backend parameter when it has one (the
conformance ``backend_spec``, the scale tests' ``scale_backend``, the demo's
database slug, ...), else from the first family token found in its node id
(file path, test name and parameter ids), else ``core``. So a new test file
lands in a family by its name, and anything unrecognised runs in ``core``.

Families, and what their CI job builds or installs:

* ``core``: the API, the dialects, in-process SQLite (stdlib and apsw) and
  PostgreSQL (asyncpg and psycopg). Needs a PostgreSQL server only.
* ``workers``: the SQLite worker processes (Python, C, the multi-tenant C
  and Rust pools) and the worker protocol. Needs ``make -C cworker``.
* ``duckdb``: in-process DuckDB and the DuckDB workers. Needs nothing built
  for the Python ones (the C++ and Rust workers have their own jobs).
* ``wasm-node``: SQLite in WebAssembly, under wasmtime and in Node.js.
  Needs ``make -C cworker wasm`` and ``npm ci --prefix nodeworker``.
* ``pglite``: PostgreSQL in WebAssembly in Node.js. Needs ``npm ci --prefix
  pglite-worker`` (and a PostgreSQL server for its differential test).
* ``clickhouse``: chDB. Needs nothing built.
"""

from __future__ import annotations

import os
import re

import pytest

FAMILIES = ("core", "workers", "duckdb", "wasm-node", "pglite", "clickhouse")

BACKEND_FAMILY = {
    "sqlite": "core",
    "sqlite-memory": "core",
    "sqlite-apsw": "core",
    "postgres": "core",
    "postgres-psycopg": "core",
    "sqlite-worker": "workers",
    "sqlite-worker-msgpack": "workers",
    "sqlite-cworker": "workers",
    "sqlite-cworker-json": "workers",
    "sqlite-cpool": "workers",
    "sqlite-rspool": "workers",
    "cworker": "workers",
    "duckdb": "duckdb",
    "duckdb-worker": "duckdb",
    "duckdb-cppworker": "duckdb",
    "duckdb-rsworker": "duckdb",
    "sqlite-wasm": "wasm-node",
    "sqlite-nodeworker": "wasm-node",
    "pglite": "pglite",
    "clickhouse": "clickhouse",
}

# Parameters naming a backend directly: (fixture name, value -> backend id).
_STRING_PARAMS = {
    "client": lambda v: v,  # tests/demo: the database slug
    "ndb": lambda v: v,  # tests/test_export_parity_edges.py
    "make_worker": lambda v: {"node": "sqlite-nodeworker"}.get(v, "sqlite-worker"),  # tests/faults
    "native_impl": lambda v: "sqlite-cworker",  # tests/native_impl.py: C or Rust native worker
}

# Searched in order in the lowercased node id; the first match wins.
_TOKENS = [
    (re.compile(r"pglite"), "pglite"),
    (re.compile(r"clickhouse|chdb"), "clickhouse"),
    (re.compile(r"duckdb"), "duckdb"),
    (re.compile(r"nodeworker|node-worker|wasm"), "wasm-node"),
    (re.compile(r"worker|cpool|c-pool|rspool|rust-pool|fleet"), "workers"),
]


def _backend_id(item: pytest.Item) -> str | None:
    callspec = getattr(item, "callspec", None)
    if callspec is None:
        return None
    for name, value in callspec.params.items():
        spec_id = getattr(value, "id", None)
        if isinstance(spec_id, str) and name in ("backend_spec", "scale_backend"):
            return spec_id
        if name in _STRING_PARAMS and isinstance(value, str):
            return _STRING_PARAMS[name](value)
    return None


def family(item: pytest.Item) -> str:
    backend = _backend_id(item)
    if backend is not None:
        if backend in BACKEND_FAMILY:
            return BACKEND_FAMILY[backend]
        text = backend.lower()  # a backend added since: classify it by name
    else:
        text = item.nodeid.lower()
    for pattern, fam in _TOKENS:
        if pattern.search(text):
            return fam
    return "core"


def select(config: pytest.Config, items: list[pytest.Item]) -> None:
    shard = os.environ.get("DBLAYER_CI_SHARD")
    if not shard:
        return
    wanted = {f.strip() for f in shard.split(",")}
    if wanted - set(FAMILIES):
        raise pytest.UsageError(f"DBLAYER_CI_SHARD={shard!r}: expected families from {', '.join(FAMILIES)}")
    keep, drop = [], []
    for item in items:
        (keep if family(item) in wanted else drop).append(item)
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep
