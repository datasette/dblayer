# dblayer build notes

Append-only log kept while building the prototype from `dblayer-prototype-spec.md`.
Each entry is tagged: **Found** (facts discovered), **Built** (what now exists),
**Decision** (a choice made, especially where it departs from the spec) or
**Idea** (something to discuss later, not acted on).

## 2026-11-23: before any code

### Found: environment

- Machine is macOS (Darwin 27.2, arm64). The spec assumes Linux in two places: `/proc/self/fd` and `RLIMIT_AS`.
- System `python3` is 3.9.6, below the spec's 3.11 floor. uv 0.13.0 has CPython 3.14.3 and 3.15.0 installed.
- PostgreSQL 18.3 (Homebrew) runs locally on the `/tmp:5432` socket. Role `simon` is a superuser. `asyncpg` 0.32.0 connects.
- The `duckdb` Python package is 2.0.1. There is no `duckdb` CLI. The spec's DuckDB section is written "from memory" and predates checking against 2.x, so every "to verify" there needs checking against this version.
- Docker is installed, so Linux-only tests can run in a container if wanted.
- `~/dev/datasette` is at `e245b406 Release 1.0a44` and is used as a read-only reference.

### Decision: how the work is run

- All Python runs through `uv run python` / `uv run pytest`.
- Opus 5.5 subagents write the code, one milestone each. Milestones 1 to 3 run in sequence because they share the core; 4 to 6 (worker, PostgreSQL, DuckDB) can run in parallel.
- Commits are made early and often, in sensible chunks, and pushed to `main` on GitHub (the repo is dedicated to this prototype). Subagents commit and push their own work whenever there is credible progress with passing tests; I commit notes and review fixes.

### Decision: departures from the spec so far

- **File-descriptor counting.** The spec says count entries in `/proc/self/fd`. A helper uses that path when it exists and `/dev/fd` otherwise, so the leak tests run natively on macOS and Linux.
- **PostgreSQL test server.** The spec says the fixture starts a server in a container. Tests instead use a dedicated database, `dblayer_test`, on the local server, with a fresh schema per test. This fits the spec's own model (one `Database` = one schema) and is much faster. The other databases on that server are never touched. A container fixture can be added later for CI.

### Idea: things noticed while reading the spec

- **`execute_write` has no `time_limit_ms`.** The Limits table says the time limit applies to "every read and write statement", but the method table gives `execute_write(sql, params, *, returning_limit)`. Writes probably want a time limit too, since a runaway `UPDATE` holds the only write slot.
- **Deadline expiry while a statement is running.** On in-process SQLite the transaction pins a connection, and a statement may be executing on a pool thread when the deadline fires. Rollback then needs `interrupt()` first, then `ROLLBACK` once the thread returns. The spec describes the deadline only between calls.
- **Cheaper, more exact SQLite change hint.** Size and mtime of the file and `-wal` can miss a change within one mtime tick, and they move on every data write, not just schema writes. Worth measuring how often the hint fires falsely under a write-heavy load, since each false hit costs a connection for `PRAGMA schema_version`.
- **`schema` message shape.** The wire-protocol table gives `schema` only a `tx` field, but the worked example sends `"table": "posts"`. Needs one definition: whole-database schema, with an optional table filter.
- **Python floor.** `sqlite3.Connection.autocommit` (3.12+) is the clean way to switch off implicit transactions. With a 3.11 floor the backend must use `isolation_level=None`. Raising the floor to 3.12 would simplify this; to discuss.

### Decision: Python floor raised to 3.12

Simon agreed to raise the floor from the spec's 3.11 to 3.12. The SQLite backend uses `sqlite3.connect(autocommit=True)` to switch off implicit transactions, in place of `isolation_level=None`. The milestone 1 subagent was told mid-run.

## 2026-11-23: milestone 1 done (core API and in-process SQLite)

### Built

- Commit `4d317bd`. About 2,500 lines of library code and a 374-test suite (3 capability-gated skips on the in-memory variant). I re-ran it myself: passes on CPython 3.15 (uv's default here) and on 3.12, in under 3 seconds.
- `Database` with `execute`, `stream`, `execute_write`, all introspection methods, `sleep()` and `close()`. Creating one opens nothing.
- `core/params.py`: a hand-written scanner that rewrites `:name` and `?` to four target styles, with 115 test cases. Postgres and DuckDB output is unit-tested but has not yet run against the real engines.
- `core/pool.py`: checkout/return pool, 3 readers and one writer per database. The write slot is a FIFO `asyncio.Lock`; there is no writer thread.
- `backends/sqlite.py`: all ten connection methods, progress-handler time limits, init hook, named in-memory databases.
- Conformance tests are parametrised by a `BACKENDS` list in `tests/conftest.py`; a new backend is one entry. Tests skip by capability with `@pytest.mark.requires("flag")`.

### Decision: departures from the spec in milestone 1

- **Limits take a `DEFAULT` sentinel.** Each `Database` holds default limits; a call can override them, and `None` means "no limit". The spec only showed per-call limits.
- **`execute_write` takes `time_limit_ms`.** This resolves the inconsistency noted earlier.
- **Extra modules:** `errors.py`, `core/results.py`, `core/database.py`, `core/executor.py`, to avoid import cycles.
- **Extra exceptions:** `ParameterError` and `TableNotFound`, both subclasses of `QueryError`.
- **Extra capability flag:** `returning`.
- **Error mapping lives on the dialect** (`Dialect.map_exception`), because the backend protocol had no hook for it.
- **Stream time limit is a budget of time spent inside the engine**, not wall-clock time, so a slow consumer does not use it up. The spec did not say which.
- **`change_hint()` is synchronous.** It only stats files, so making it a coroutine bought nothing.
- **`Column` is a `str` subclass** with a `.type` attribute, so `result.columns == ("id", "title")` holds as the spec's example shows.
- **In-memory databases** report `sleep_releases_everything=False` and keep a private anchor connection in the backend.

### Found

- Python's `sqlite3` reports `rowcount=-1` for `WITH ... INSERT`; the backend falls back to `select changes(), last_insert_rowid()`.
- SQLite gives no declared type for result columns, so `Column.type` is `None` on this backend.
- A subagent's `git push` was refused by the permission classifier in its own session, though the commit did reach GitHub. Later subagents are told to commit, try one push, and leave it if refused.

### Idea: open problems to discuss

- **`last_pk` is the SQLite rowid.** It differs from the primary key for non-integer keys and is stale after an upsert that took the update path. A portable `last_pk` probably has to come from `RETURNING` on the primary-key columns. Worth deciding whether `last_pk` should exist at all, or whether callers should ask for `RETURNING`.
- **Abandoned streams.** Breaking out of `async for chunk in db.stream(...)` returns the connection only when the generator is finalised. An open stream also holds a SQLite read transaction. An `async with db.stream(...) as rows:` form would make release deterministic.
- **Sleep while connections are checked out** closes them on return but never calls `backend.close()`. Harmless for SQLite, wrong for DuckDB, whose instance must be closed to release the file. Milestone 3 needs to fix this.
- **A `Database` is bound to the event loop that first used it**, because the pool's asyncio primitives are. Datasette tests create many loops; needs a decision.
- **Cancellation waits for the thread.** A cancelled call interrupts the statement and waits for the worker thread before re-raising, so a connection is never used by two threads. A query that ignores the interrupt therefore delays cancellation. This is the in-process case the worker backend exists to fix.
- **Creating a missing file on first read.** With the default `create=True`, reading a database whose file does not exist creates an empty file. Datasette would probably prefer an error for reads.
- **No type checker has been run** over the code yet.
