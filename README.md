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

### Timing

The suite must pass on a loaded machine or a shared CI runner, so tests wait
for the event they check (`helpers.wait_until`, an `asyncio.Event`, a hook)
rather than sleeping a fixed time, and idle-timeout tests move the clock by
hand (`helpers.IdleClock`). Hang guards allow `helpers.EVENTUALLY_S` (10 s).

The spec's latency promises (cancel releases the write slot within 50 ms,
DuckDB interrupts within 100 ms of the time limit, deadline expiry) are
checked by `helpers.check_latency`: the operation must complete within the
generous functional bound, and the measured figure is printed (`pytest -s`)
and listed in a "spec latencies" section at the end of the run. The spec's
tight figure is asserted only with `DBLAYER_STRICT_TIMING=1`:

```bash
DBLAYER_STRICT_TIMING=1 uv run --all-extras pytest
```

Use strict mode on a quiet machine when measuring; expect it to fail under
load.

### The C worker (experimental)

`cworker/` is a SQLite worker process written in C that speaks the worker
protocol ([docs/worker-protocol.md](docs/worker-protocol.md)) and starts in a
few milliseconds instead of ~85. Build it once (needs a C compiler, `curl`
and `unzip`; it downloads a pinned, checksummed SQLite amalgamation into the
git-ignored `cworker/vendor/`):

```bash
make -C cworker
```

Its tests (`sqlite-cworker` in the conformance suite, `tests/worker/test_cworker.py`,
the `c-worker` fault tests) skip with that command as the reason until the
binary exists. `DBLAYER_CWORKER=/path/to/dblayer-cworker` points the tests
and `SqliteCWorker` at another build; `make -C cworker SANITIZE=1` builds one
with AddressSanitizer and UBSan in `cworker/build-asan/`, and `make -C cworker
SQLITE=system BUILD=build-system` links the system `libsqlite3` instead.

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
through one `Registry`: 600 in-process SQLite, 200 SQLite in a C worker
process, 100 in-process DuckDB, 50 DuckDB in a worker process, 30 PostgreSQL
schemas and 20 SQLite in a Python worker process. The C worker members show
as unavailable, with the build command, until `make -C cworker` has run. They
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
