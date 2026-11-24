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

Options: `--port`, `--host`, `--data-dir` (default `demo-data/`, git-ignored).
PostgreSQL uses `$DBLAYER_TEST_POSTGRES` (default
`postgresql:///dblayer_test?host=/tmp`), refuses any database other than
`dblayer_test`, and touches only the `dblayer_demo` schema. A backend that
cannot run here is listed as unavailable, with the reason. Its tests are in
`tests/demo/` and run with `uv run --all-extras --group demo pytest tests/demo`.
