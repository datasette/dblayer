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

## 2026-11-23: milestone 2 done (transactions and named operations)

### Built

- Eleven commits, `8c75c5e` to `bba6973`. I re-ran the suite: 828 passed, 10 skipped, on Python 3.12 and 3.15. The timing-sensitive transaction and deadline tests passed three runs in a row.
- `db.transaction(mode, deadline_ms)`, `db.batch()`, every named operation on both `db` and `tx`, `db.staged_import()` and `db.sweep_staged()`.
- Default operations are plain `async def op(tx, **args)` functions in `core/operations.py`, written against a five-member `TxLike` interface. Arguments are normalised to plain data first, so the same call can cross a process boundary.
- Override dispatch: a connection that implements `run_operation` is tried first; it may return `NotImplemented` to fall back. `Database(native_operations=False)` forces the default path, and the `ops_db` test fixture runs every operation test both ways.
- `alter_table` on SQLite: native `RENAME COLUMN`, otherwise a rebuild that replays indexes and triggers.

### Found

- **A view naming the target breaks drop-and-rename on SQLite** ("error in view v: no such table"). `PRAGMA legacy_alter_table=ON` around the rename fixes it.
- **sqlite-utils `transform` has gaps we should not copy:** it does not recreate triggers, silently loses CHECK constraints, STRICT and WITHOUT ROWID, and its `defer_foreign_keys` approach misbehaves inside a transaction with `foreign_keys=ON` (silent cascade deletes on DROP, or a COMMIT failure with an empty `foreign_key_check`). Worth reporting upstream after confirming with a minimal script.
- **Shared-cache in-memory databases cannot give snapshot reads**, since their readers run `read_uncommitted`.
- **In rollback-journal mode an open read transaction blocks a writer's COMMIT** for up to `busy_timeout`.
- **`sqlite3_interrupt` on an idle connection does nothing**, so a deadline that fires while a statement is still queued for a thread would be missed. The expiry task re-interrupts every 20 ms until the call returns.

### Decision: departures from the spec in milestone 2

- **`last_pk` is `None` unless this statement inserted a row** (found by comparing `last_insert_rowid()` before and after). It is the rowid, so it equals the primary key only for rowid tables.
- **`db.stream()` returns a `RowStream`** usable with `async for` or `async with`, so the connection is released deterministically.
- **`ignore` and `replace` use portable `ON CONFLICT`**, not SQLite's `OR IGNORE` / `OR REPLACE`. `replace` needs a primary key and updates in place, keeping the rowid; SQLite's native form deletes and reinserts.
- **Nesting is stricter than the spec:** any `db.transaction()` inside an open transaction on the same database raises `NestedWriteError`, including a read inside a read.
- **Read transactions take their snapshot at the first statement**, as PostgreSQL's REPEATABLE READ does.
- **Deadline:** counted from BEGIN, not from the wait for the write slot. Commit is never interrupted. A clean exit after expiry raises `TransactionExpired`.
- **Rebuilds refuse rather than lose data:** tables with CHECK, generated columns, STRICT or WITHOUT ROWID raise `NotSupported`.
- **Staging table name carries its creation time** (`_dblayer_import_<unix-seconds>_<hex8>`), so the orphan sweep needs only a table listing.
- **`replace` rebuilds indexes after the rename, inside the swap transaction.** The spec says to build them on staging first, but index names are global in SQLite and cannot be renamed. The cost is that the swap holds the write slot for the index build.
- **New capability flag `snapshot_reads`**, new exception `TransactionClosed` (parent of `TransactionExpired`), and an optional `execute_batch` backend method so `batch()` is one hop.
- **`staged_import` takes `pk`, `alter` and `columns`** as well as `mode`.

### Idea: to discuss

- **Rename `last_pk` to `last_rowid`** and have operations return real primary keys through `RETURNING`.
- **Use the `memdb` VFS for in-memory databases** (`file:/name?vfs=memdb`) in place of shared cache. It should give real locking and isolation and could remove the `snapshot_reads` flag. Untested.
- **WAL by default** for writable SQLite files, which removes readers blocking a writer's COMMIT.
- **Savepoints are more important than "stretch goal".** PostgreSQL aborts the whole transaction when any statement fails, where SQLite carries on. Wrapping each `tx` call in a savepoint on PostgreSQL would make `tx` behave the same on every backend.
- **A read inside a read transaction can exhaust the read pool and hang** when `max_read_connections=1`. Routing `db.execute()` through the task's open read transaction would fix it, and is close to open question 8 (a request's reads sharing one snapshot).
- **Operations on a `tx` shared between tasks are not atomic**; a per-operation lock would fix it.
- **The orphan sweep can delete a live import owned by another process** if it is older than `max_age`. A heartbeat row or an advisory lock would close this.
- **Process note:** vague "commit when a chunk passes" briefs produced one commit per milestone. A named list of commit points plus a 10-minute ceiling produced eleven.

## Upstream suggestions log (sqlite-utils and Datasette)

Kept from 2026-11-23 on. Each entry is a credible bug fix or feature change for another project, with how well it has been checked. "Subagent-reported" means a subagent saw it while building; nobody has yet written a minimal reproduction against the upstream project itself.

### sqlite-utils

- **`transform()` drops triggers.** Dropping the old table drops its triggers and `transform` does not recreate them. dblayer's rebuild replays them from `sqlite_master`. Subagent-reported.
- **`transform()` silently loses CHECK constraints, `STRICT` and `WITHOUT ROWID`.** The new table is built from column definitions only. Suggest either carrying them over or refusing with an error; dblayer refuses. Subagent-reported.
- **`transform()` inside a transaction with `foreign_keys=ON`.** The `defer_foreign_keys` approach either cascade-deletes child rows when the old table is dropped, or fails at COMMIT with "FOREIGN KEY constraint failed" although `PRAGMA foreign_key_check` is empty. Subagent-reported on SQLite 3.53; needs a minimal script before filing.
- **Drop-and-rename fails when a view names the table** ("error in view v: no such table: main.p"). `PRAGMA legacy_alter_table=ON` around the rename avoids it. Applies to `transform()` on a table with a dependent view. Subagent-reported.
- **Prefer native `ALTER TABLE ... RENAME COLUMN` for pure renames.** SQLite rewrites indexes, triggers, views and child foreign keys itself, which a rebuild does not. Feature suggestion.

### Datasette

- **`rowcount` is -1 for `WITH ... INSERT/UPDATE/DELETE`** in Python's `sqlite3`. Anything in Datasette that reports rows affected from `cursor.rowcount` is wrong for CTE-prefixed writes; `select changes()` on the same connection gives the right number. Subagent-reported; not yet checked against Datasette's write API.
- **A time limit or cancel that arrives before a statement starts is lost,** because `sqlite3_interrupt` on an idle connection is a no-op. Relevant to any future "cancel this query" feature in Datasette, which would need to re-interrupt until the call returns. Subagent-reported.
- **Readers can block a writer's COMMIT in rollback-journal mode** for up to `busy_timeout`. Suggests Datasette should recommend or default to WAL for mutable databases. Subagent-reported.

## 2026-11-23: continuous integration

### Built

- `.github/workflows/test.yml` runs on every push to `main` and on pull requests. Three jobs: Linux on Python 3.12, 3.13 and 3.14 with a PostgreSQL 18 service container and all optional backends installed; macOS on 3.14; and a "stdlib-only" Linux job with no extras, which proves the core and SQLite backends need only the standard library.
- First green run: all five jobs passed. This is the first time the suite has run on Linux, so the `/proc/self/fd` branch of the file-descriptor helper is now exercised too.

### Found

- `astral-sh/setup-uv` publishes no floating major tag (`@v10` does not resolve), so it is pinned to `v10.4.0`.
- With subagents pushing every few minutes, `cancel-in-progress: true` meant no run ever finished. It is now `false`: the running run completes and only the newest queued one is kept.

### Idea

- CI gives a Linux machine for the worker backend's hard-limit tests (`RLIMIT_AS`, kill), which cannot run on this Mac. Milestone 4's fault tests should be written to run there.
