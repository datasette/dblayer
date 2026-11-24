# DuckDB worker in C++ (experimental)

`dblayer-duckdb-worker` is a DuckDB worker process written in C++17 that
speaks dblayer's worker protocol ([docs/worker-protocol.md](../docs/worker-protocol.md))
and links the official prebuilt `libduckdb` through its C API. The parent
side is `dblayer.backends.duckdb_cppworker.DuckDBCppWorker`, registered as
`duckdb-cppworker` in the conformance suite.

```bash
make -C duckdb-cpp-worker            # build/dblayer-duckdb-worker
make -C duckdb-cpp-worker SANITIZE=1 # build-asan/, ASan + UBSan
```

The first build runs `fetch-libduckdb.sh`, which downloads libduckdb 2.0.1
(the version of the `duckdb` Python package, so files are interchangeable)
for this platform into the git-ignored `vendor/` and checks it against the
SHA-256 GitHub publishes for the release asset. The binary finds the library
through an rpath relative to itself; no environment variable is needed.
Python finds the binary through `binary=`, then `$DBLAYER_DUCKDB_CPP_WORKER`,
then `build/dblayer-duckdb-worker`.

## Layout

| File | What |
| --- | --- |
| `src/worker.cpp` | `main`: options, stdout kept private (fd 1 goes to stderr), `RLIMIT_AS` |
| `src/server.cpp` | framing, routing, a thread per connection, the timer thread, transactions and deadlines, `hello`/`open`/`close`/`sleep`/`_debug`, shutdown |
| `src/exec.cpp` | statements (`DuckDBConnection._check` rules), execute/batch/execute_many, streams, the appender `insert_rows` |
| `src/values.cpp` | result vectors to wire values exactly as the Python package returns them; parameters bound as the package binds Python values |
| `src/types.cpp` | `LogicalType::ToString()` (the C API has none) |
| `src/schema.cpp` | introspection and `schema_version`, as `_read_schema()` |
| `src/errors.cpp` | DuckDB messages to the Python exception class and dblayer error |
| `src/wire.cpp` | frames and the protocol's ext value types, on the C worker's `cworker/src/codec.c` (compiled in) |

## Behaviour

* **Threads.** The main thread reads frames and acts on `cancel` and `more`
  at once. Each `open` starts a thread that owns a `duckdb_connection` and
  runs that connection's requests in order; different connections of one
  database run concurrently (`hello` says `"concurrent": true`, and
  `ConcurrentWorkerProcess` on the parent multiplexes requests). A point
  read is not held up by a long query on another connection, which the
  Python worker cannot do.
* **Time limits, cancels, deadlines**: `duckdb_interrupt()` again every 5 ms
  until the statement returns (DuckDB drops an interrupt that arrives before
  a statement has started, or on an idle connection); a statement refuses
  to start once its request is cancelled.
* **Statements** as the Python backend runs them: one per call
  (`MULTIPLE_STATEMENTS`), BEGIN/COMMIT refused (`TRANSACTION_STATEMENT`),
  read connections limited to SELECT/EXPLAIN/PRAGMA/CALL and run inside
  `BEGIN TRANSACTION READ ONLY`, COMMIT of a transaction aborted by an
  earlier error refused (`TRANSACTION_ABORTED`) instead of DuckDB's silent
  rollback, DML results as row counts.
* **`insert_rows`** runs in the worker through DuckDB's appender
  (`"ops": ["insert_rows"]`): 7x faster than the default path for 10,000
  rows. It declines (the parent runs the default) for ignore/replace, a
  missing table and columns the table lacks.
* **Settings**: `memory_limit`, `threads`, `enable_external_access=false`,
  extra `config`, then `lock_configuration`.
* **End of input**: cancel what runs, roll back, close, exit (after 5 s
  regardless).

## What the C API made hard

* No type names: `LogicalType::ToString()` is reimplemented, with DuckDB's
  keyword list (`duckdb_keywords()`) for quoting struct fields.
* Statement types only after `duckdb_prepare` (binding), so a read
  connection given a write that does not bind falls back to the leading
  keyword to refuse it as READ_ONLY first; `PRAGMA` prepares as SELECT or
  SET and is mapped back by keyword.
* Errors from `duckdb_prepare`/`duckdb_extract_statements` carry only a
  message; the exception class comes from its `"<Type> Error: "` prefix.
  The appender's errors have no prefix but have an error type.
* No access to a value's Python form: every conversion the Python package
  makes (BIGNUM and BIT as `str`, INTERVAL months as 30 days, infinities as
  `date.max`, maps with nested keys as `{"key": [...], "value": [...]}`,
  TIMESTAMPTZ in the session's `TimeZone`) is redone, checked by
  `tests/worker/test_duckdb_cppworker.py` against in-process DuckDB.
* An unbounded recursive CTE yields its first rows only after seconds of
  work through the C API (both the classic API and the 2.0 `duckdb_v2_*`
  one); the Python package, built on engine internals (`ClientContext::Submit`),
  gets them in well under a second. With `max_rows` the worker runs
  `SELECT * FROM (sql) LIMIT max_rows + 1` when that keeps the column
  names; streams have no such bound (two export tests are expected failures).
