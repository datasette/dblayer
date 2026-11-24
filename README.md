# dblayer
Experimental prototype for a new Database backend abstraction

The design is described in [dblayer-prototype-spec.md](dblayer-prototype-spec.md).
Milestone 1 (core API and the in-process SQLite backend) is implemented.

## Running the tests

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12 or later:

```bash
uv run pytest
```

- `tests/test_params.py` covers the placeholder rewriter for every target style.
- `tests/conformance/` is the backend conformance suite. Every test that takes
  the `db`, `make_db`, `backend` or `make_backend` fixture runs once per entry in
  `BACKENDS` in `tests/conftest.py`; add a backend by adding an entry there.
  Mark tests with `@pytest.mark.requires("flag")` or
  `@pytest.mark.requires_not("flag")` to skip by capability flag.
- `tests/conformance/test_sqlite_specific.py` holds tests that only make sense
  for SQLite.

## Demo app

`dblayer_demo/` is a small Datasette-like Starlette app built only on
dblayer's public API: a homepage listing four databases (in-process SQLite,
SQLite in a worker process, PostgreSQL, DuckDB) holding the same seeded
dataset, a page per database, paginated table pages and a read-only SQL box
(`/<database>/-/query?sql=...`, with `:name` parameters).

```bash
uv run --all-extras --group demo python -m dblayer_demo   # http://127.0.0.1:8001/
```

Options: `--port`, `--host`, `--data-dir` (default `demo-data/`, git-ignored),
`--postgres-dsn` and `--fleet-size`. PostgreSQL uses `--postgres-dsn`, else
`$DBLAYER_TEST_POSTGRES`, else `postgresql:///dblayer_test?host=/tmp` (which
connects as the current OS user, so a GUI-launched process may need the
option). It refuses any database other than `dblayer_test`, and touches only
the `dblayer_demo` schema and the fleet's `dblayer_demo_fleet_NNN` schemas. A
backend that cannot run here is listed as unavailable, with the reason.

### The fleet: 1,000 databases behind one Registry

`/-/fleet` (linked from the homepage) lists a further 1,000 databases served
through one `Registry`: 800 in-process SQLite, 150 DuckDB, 30 PostgreSQL
schemas and 20 SQLite-worker (unavailable until that backend exists). They
share a global cap of 40 open connections, 8 threads and a 12 s idle timeout.
Every database starts cold and is built in `on_wake` on first use (a copy
of a seeded template file, or a newly seeded schema, each with its own
branch name and slice of reviews), so startup takes well under a second
once the templates exist. Each one browses like the featured four.

- The homepage, fleet page and `/-/fanout` show live stats from
  `registry.stats()` and the process (threads, open fds, RSS), also at
  `/-/stats.json`.
- `/-/fanout?n=200` runs 200 concurrent reads on databases picked with a
  Zipf-like skew and reports latency, the peak open connections against the
  cap, and how the stats moved (`n` is capped at 500; `/-/fanout.json` for
  JSON). `/-/fanout?n=100&action=evict` makes 100 asleep databases cold
  again.

Tests are in `tests/demo/`:
`uv run --all-extras --group demo pytest tests/demo`.
