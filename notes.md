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

## 2026-11-23: DuckDB backend done (milestone 6)

Subagent-reported; I have not yet re-run the combined suite, because milestone 3 and PostgreSQL are still landing in the same tree.

### Built

- Ten commits, `24f9499` to `ecabbf6`. `dblayer/backends/duckdb.py`, a `DuckDBDialect`, a native `insert_rows` using the appender, and the backend registered in the conformance suite.
- Reported result with all extras: 1,726 passed, 46 skipped. DuckDB alone: 404 passed, 8 skipped, every skip from a capability flag.
- `scripts/duckdb_verify.py` holds the experiments behind the table below; `scripts/bench_duckdb_insert.py` holds the insert benchmark.

### Found: the spec's "to verify" items, on DuckDB 2.0.1

| Spec assumption | Observed |
| --- | --- |
| One process cannot hold a file read-only and read-write at once | True. Another process cannot open the file in either mode while it is open. Closing the parent connection releases the lock; closing a cursor does not. |
| Optimistic concurrency; conflicts abort | True. Two transactions updating the same row: the second fails at once. Different rows and appends do not conflict. |
| DDL is transactional | Yes, including CREATE INDEX, CREATE VIEW and ALTER. |
| `interrupt()` from another thread | Lands in about 5 ms. On an idle cursor it does nothing. It aborts the open transaction. |
| `SET memory_limit` / `SET threads` | Work, but apply to the whole instance. Defaults on this Mac were 102 GiB and 18 threads. |
| Some ALTER forms fail with dependent indexes | Worse than assumed: with any `CREATE INDEX` on the table, every ALTER except ADD COLUMN and SET DEFAULT fails, including RENAME TO. |
| CREATE statements recoverable | Yes, from the `sql` column of `duckdb_tables()`, `duckdb_indexes()` and `duckdb_views()`. |
| `$name` parameters | Work, but names are case-insensitive and extra keys are an error. |
| WAL file name | `<file>.wal`, removed by the checkpoint on close. |

Other facts: no savepoints, no triggers, no partial indexes; table names are case-insensitive; block comments nest; there is no catalog version counter.

- **DuckDB turns COMMIT of an aborted transaction into a silent rollback.** Any error aborts the transaction, even a parse error. Without a guard, a caller who catches an `IntegrityError` and exits the block cleanly loses every write and is told it committed. Verified with minimal scripts; worth reporting to DuckDB.
- **A time limit or cancel aborts the whole write transaction** on DuckDB, since interrupt does.
- **`executemany` costs about 0.2 ms a row.** Insert of 10,000 five-column rows: executemany 1,448 ms, multi-row VALUES 200 ms, appender 8.0 ms, Arrow table 4.4 ms.
- **Interrupt lands 2 to 6 ms after the time limit**, well inside the spec's 100 ms.
- **Conflict rate with the write queue off** (8 writers, 25 transactions each): 0 of 200 for appends and for updates spread over 1,000 rows; 162 to 166 of 200 for one hot row. No update was lost.
- **An awake DuckDB instance costs about 5 MB of resident memory.** Sleeping returned only about a quarter of that to the OS, though repeated wake and sleep cycles stay flat.
- **Without `enable_external_access=false`, arbitrary SQL can read local files** (`read_csv`, `COPY`, `ATTACH`).

### Decision: departures from the spec

- **Read-only is enforced twice:** read calls accept only SELECT, EXPLAIN, PRAGMA and CALL, and each runs inside `BEGIN TRANSACTION READ ONLY` (about 40 µs), which also blocks writes hidden in a SELECT such as `nextval()`.
- **COMMIT after an earlier error raises** `QueryError` with code `TRANSACTION_ABORTED` and rolls back, so the silent-rollback case above cannot happen through dblayer.
- **New capability flags:** `statement_errors_abort_transaction` (true on DuckDB) and `replace_keeps_views`.
- **Native insert uses the appender**, not Arrow: as fast at the default batch size and needs no pyarrow.
- **Dialect decides per table whether native ALTER is possible** (`native_alter_for`), because trying the ALTER and catching the failure would abort the transaction.
- **Defaults:** `memory_limit="256MB"`, `threads=2`, external access off, configuration locked after open.
- **One shared interrupter thread with a heap** enforces time limits, in place of a timer per call.
- **Portable `integer` maps to BIGINT** on DuckDB, whose INTEGER is 32-bit.
- **The rewriter refuses parameters that differ only in case** (`:a` with `:A`).
- **One statement per call:** DuckDB would run `a; b` as two statements, so the backend rejects it.

### Idea: to discuss

- **"A failed statement aborts the transaction" is now true of two of four backends** (PostgreSQL and DuckDB), and DuckDB has no savepoints to paper over it. The portable contract for `tx` may have to be "after any error, the transaction is dead", with SQLite made stricter to match, not the other backends made looser.
- **A time limit inside a transaction kills the transaction on DuckDB.** Same question: should that be the contract everywhere?
- **Arrow path for large staged imports** when pyarrow is installed (twice as fast at 100,000 rows).
- **Appender upsert for `upsert_rows`**, declined for batches with duplicate keys, where it keeps the first row and `executemany` keeps the last.
- **Queue-off mode needs a "writers wait for row locks" capability** if it is ever more than an experiment: hot-row updates conflict on DuckDB where PostgreSQL waits.

### Unverified or fragile

- Index columns are parsed from `CREATE INDEX` text; `WriteConflict` mapping matches on message text.
- Appender value conversion was not checked for every type (datetimes, decimals).
- One timing failure was seen once under load in the SQLite cancel test (138 ms against its 50 ms bound). That bound comes from the spec's fault table and may be too tight for a shared CI machine.

### Upstream suggestions (added to the log above)

- **Datasette, design note:** a DuckDB backend that accepts arbitrary SQL must open with `enable_external_access=false` and `lock_configuration=true`, or SQL can read local files. Verified by the subagent on DuckDB 2.0.1.
- **DuckDB itself (not sqlite-utils or Datasette):** COMMIT of an aborted transaction reports success while rolling back. Verified with minimal scripts.

## 2026-11-23: PostgreSQL backend done (milestone 5)

Subagent-reported; combined suite not yet re-run by me while milestone 3 is still landing.

### Built

- Nineteen commits, `2a1a0fe` to `f27f8cb`. `dblayer/backends/postgres.py` on `asyncpg`, a `PostgresDialect`, `COPY` and `INSERT ... ON CONFLICT` overrides, and 38 PostgreSQL-specific tests covering the spec's five "must demonstrate" items.
- Reported: 409 passed and 20 skipped for PostgreSQL, every skip from a capability flag.
- `tests/pg.py` refuses any DSN that does not name `dblayer_test`, gives each test its own schema and drops it afterwards.

### Found

- **Untyped parameters fail on PostgreSQL.** The server types a parameter it cannot infer as `text`, and asyncpg then refuses a Python int: `select :a` with `{"a": 1}` fails. The backend now adds casts. This is the largest single portability gap for "SQL passes through unchanged".
- **asyncpg's statement cache breaks after DDL** ("cached plan must not change result type"), and inside a transaction it does not retry.
- **asyncpg schedules a timer per cached statement** unless `max_cached_statement_lifetime=0`.
- **`executemany` reports no row counts** on asyncpg.
- **COMMIT of an aborted transaction silently rolls back** on PostgreSQL too (the server answers ROLLBACK). Same trap as DuckDB.
- **`idle_in_transaction_session_timeout` ends the session**, so it works as the server-side deadline the spec wanted.
- **PostgreSQL refuses to drop a table that a view or foreign key depends on**, so staged `replace` fails cleanly there when a view exists.
- **`arr[:n]` is ambiguous:** the rewriter reads `:n` as a parameter, so a slice ending at a parameter needs a space.
- **A core pool race:** if `sleep()` ran while a connection was still being opened, that connection was kept and the backend never released. Fixed in `ec73867`.
- **Costs on the local socket:** `schema_version()` 0.6 ms; full `schema()` for 50 tables 2.9 ms; a savepoint per call adds 30 to 40 µs, roughly 45% on a trivial statement.

### Decision: departures from the spec

- **No asyncpg pool.** The spec asked for one with minimum size zero. The core pool already pools connections; a second pool underneath would have its own idle policy.
- **Savepoint per call inside a transaction, on by default,** so a failed statement does not kill the transaction and `tx` behaves as on SQLite. `statement_savepoints=False` turns it off. A named operation runs in one savepoint, so it is atomic.
- **Number of concurrent writers is a pool property driven by `concurrent_writers`**: one for SQLite and DuckDB, up to the pool size for PostgreSQL.
- **Read connections are opened read-only** (`default_transaction_read_only=on`), the equivalent of SQLite's `mode=ro`.
- **`alter_table` is all native ALTER,** including primary-key changes.
- **`execute_many` counts rows** by wrapping the statement in a CTE with `RETURNING 1`.
- **`refresh_schema()` exists on every backend**, not only PostgreSQL.
- **Server deadline** is set in the same round trip as BEGIN, at the deadline plus one second of grace.
- **The backend uses three private asyncpg APIs** (`_prepare`, `_execute`, the transport socket), checked against asyncpg 0.32. That is a fragile dependency.

### Idea: to discuss

- **Is asyncpg the right driver?** (Open question 1.) The private-API use, the statement-cache trouble after DDL, the missing `executemany` counts and the strict parameter typing are all asyncpg-specific. A spike on psycopg 3 would show whether it removes them.
- **Savepoints: now three answers on three engines.** SQLite carries on after an error, PostgreSQL does with savepoints at a 45% cost on small statements, DuckDB cannot. Needs one decision about the portable contract.
- **Pool health check** so connections the server has killed are never handed out.
- **Staged replace by truncate-and-copy** when the target has dependent views, to keep the views working.
- **`search_path` holds only the database's schema,** so extension functions installed in `public` are invisible. Probably want `schema, public`.
- **`max_result_bytes` is checked after the fetch** on PostgreSQL, not during it, so it does not protect memory there yet.

### Upstream suggestions

- None for sqlite-utils or Datasette. Note for any future PostgreSQL support in Datasette: `select :a` with an int fails without parameter casts.

## 2026-11-23: demo app, first version

### Built

- `dblayer_demo/`: a Starlette app using only dblayer's public API. Homepage, database page, paginated table page and a linkable SQL page. Thirteen commits, `66a5e41` to `965498e`.
- Run with `uv run --all-extras --group demo python -m dblayer_demo --port 8765`.
- I ran it and checked in-process SQLite, PostgreSQL and DuckDB myself: browsing, paging, a count query, a write being rejected by the read path (`SQLITE_READONLY`, PostgreSQL `25006`, DuckDB `READ_ONLY`) and a slow query cut off at the 1,000 ms limit. The worker database is listed as unavailable until milestone 4.
- Being extended now to a fleet of 1,000 databases through the `Registry`.

### Found: what a Datasette-like app runs into

- **`Database` had no `dialect`,** so code outside a transaction could not quote identifiers. Added.
- **String parameters from a URL give different answers per backend.** `where reviews >= :n` with `n="5"` against a computed column returns 0 rows on SQLite (text never compares less than an integer there) and 35 rows on PostgreSQL and DuckDB, which infer the parameter type.
- **A failed connection escapes as a raw OS error** (`FileNotFoundError` for a missing PostgreSQL socket), not a dblayer exception.
- **No portable row count or keyset paging.** The app uses `count(*)` under a 200 ms limit and LIMIT/OFFSET, using `truncated` to know whether a next page exists.
- **Result column types are inconsistent:** `None` for SQLite expressions, `int8` on PostgreSQL (while introspection says `BIGINT`), `BIGINT` on DuckDB.
- **The default PostgreSQL DSN relies on the process user name.** Launched from a GUI process it tried role `root` and failed.
- **`pyproject.toml`'s package pattern `dblayer*` would have shipped the demo** inside the library. Narrowed.

### Idea: library additions the demo suggests

- `ConnectionFailed` exception for unreachable servers and missing files.
- `db.count(table, time_limit_ms=...)` with an estimate option (PostgreSQL `reltuples`).
- A keyset paging helper that converts string cursor values using column types.
- A typed-parameter policy for string inputs, so URL values compare the same on every backend.
- A normalised portable type on result columns.
- Export `TableInfo`, `ColumnInfo`, `find_placeholders` and `leading_keyword` from the top-level package.
- `definition()` on PostgreSQL built from the catalog, at least `pg_get_viewdef` for views.
- A named operation for creating a view; the demo had to use raw `execute_write`.

### Upstream suggestions

- **Datasette:** a `:param` from the query string is always text, so comparing it with a computed (untyped) expression silently gives wrong results on SQLite, for example `where count >= :n`. Seen in SQLite through dblayer; not re-tested in Datasette itself. A "cast numeric-looking parameters" option or a documentation note would be credible.

### Unverified or fragile: flaky tests seen in CI

- `test_postgres_specific.py::test_schema_change_by_another_client_seen_after_refresh_and_ttl` failed on Linux 3.12.
- `test_duckdb_backend.py::test_long_analytical_query_interrupted_within_100ms` failed on the macOS runner.
- `test_lifecycle_fds.py::test_idle_timeout_releases_every_descriptor[postgres]` failed once locally and passes alone.
- All three are timing-dependent. Tight wall-clock bounds from the spec (50 ms, 100 ms) do not hold on shared CI machines; they need either looser bounds in CI or a measurement that does not depend on the scheduler.

## 2026-11-23: milestone 3 done (lifecycle and registry)

### Built

- Sixteen commits, `86f734f` to `d857cc9`. `Registry` with lazy `get()` through a resolver, a shared fixed-size thread pool, a global connection cap with least-recently-used eviction, a wake lock, `on_wake` / `on_sleep` / `on_evict` hooks, `evict()` to the Cold state and `stats()`.
- `Database` uses `__slots__`; its pool, schema cache and staging set are created lazily and dropped on sleep. New `state` property and `idle_timeout` setting.
- Idle timeout with one timer for the whole hub, not one per database.
- The 10,000-database scale test, a 1,000-database DuckDB variant and the thread-count test.
- I ran the whole suite with every backend after it landed: 1,805 passed, 58 skipped, 2 failed. Both failures are timing-dependent idle-timeout tests (`test_write_and_read_connections_time_out_separately[postgres]`, `test_many_databases_share_one_timer[duckdb]`), run while another subagent was loading the machine. The subagent's own last run and its CI run were green.

### Found: scale test figures (subagent-reported, on this Mac)

| Measure | Result | Spec target |
| --- | --- | --- |
| Memory per asleep, never-used database | 513 B (tracemalloc), 569 B (RSS) | under 2 KB |
| Registering | 0.86 µs per database at both 1,000 and 10,000 | linear |
| Load | 20,000 requests over 3,363 databases in 2.9 s (about 7,000 requests/s) | none |
| Open file descriptors | baseline 9, peak 73 with a cap of 64 | cap plus allowance |
| Threads | 9 before, 9 during | constant |
| DuckDB, 1,000 databases | 3,000 requests in 5.1 s (591 requests/s) | reported only |

- A `Database` plus backend cost 1,569 B before `__slots__` and 368 B after.
- **A per-connection cap does not bound DuckDB.** Descriptors and memory scale with awake instances, and queued callers kept evicted databases awake: 30 instances open under a cap of 6. Fixed by counting places (below).
- **asyncpg connections stranded on a closed event loop cannot be closed from another loop,** so their sockets leak. One test is marked xfail for PostgreSQL. This is a second argument for trying psycopg 3.
- **At saturation almost every returned connection is closed to make room for a waiter:** about 11,000 wakes for 20,000 requests with 96 workers and a cap of 64. Correct, but wasteful.
- The DuckDB backend's statement-parse cache grows 55 to 543 B per wake and sleep cycle per database.

### Decision: departures from the spec

- **The cap counts "places", not connections.** A place is a connection, and an awake database always holds at least one place until its backend is released. This is what makes the cap bound DuckDB instances and file descriptors.
- **Idle timeout and cap share one structure** (a hub with an LRU of idle connections and a single timer).
- **`registry.get()` raises `UnknownDatabase`** where the spec's resolver returns `None`.
- **A `Database` can move between event loops** when nothing is checked out: it sleeps and starts again on the new loop.
- **`evict()` works only on an asleep database and never waits,** so it is safe to call from hooks. The "copied out before eviction" rule lives in the hook, not the registry.
- **`on_sleep` errors are logged, not raised.**
- **Sleep drops the schema cache**, as the spec says, so the first introspection after a wake reads the schema again.

### Idea: to discuss

- **Cap policy at saturation:** prefer evicting a database with no queued callers, to cut the wake/sleep churn.
- **Weighted places:** let a backend declare what an awake instance costs (DuckDB: 2 descriptors and about 5 MB), so one cap can mix cheap and expensive backends honestly.
- **A waiting timeout on the cap.** A task that holds one connection and asks for another can deadlock silently when the cap is full.
- **Quota-driven eviction in the registry** (`evict_lru(bytes)`); today the test helper owns that policy.
- **`Registry.close()` shuts the thread pool down on the event loop thread,** which would block behind a query that ignores interrupts.

## 2026-11-23: demo app at 1,000 databases

### Built

- The demo now serves a fleet of 1,000 databases through one `Registry`: 800 in-process SQLite, 150 DuckDB, 30 PostgreSQL schemas (`dblayer_demo_fleet_NNN` in `dblayer_test`) and 20 SQLite-worker, which show as unavailable until milestone 4. Cap of 40 open connections, 8 threads, 12 s idle timeout.
- Every database starts Cold and is built in `on_wake` by copying a seeded template (or creating the schema on PostgreSQL), then personalised.
- Pages: `/-/fleet` (grid of all 1,000 coloured by state, with filter links), a live stats panel fed by `/-/stats.json`, and `/-/fanout?n=N` (up to 500 concurrent skewed reads, or evictions).
- New library method: `Registry.add(name, db, cold=True)`.
- Eleven commits, `499e60c` to `708f985`.

### Found

- **My own run, fresh server, one 500-way fan-out from the browser:** 500 of 500 reads succeeded in 0.87 s across 175 distinct databases, all of them cold beforehand. Open connections peaked at exactly the cap of 40. Threads went from 3 to 12 and stayed there; file descriptors 12 to 52; resident memory 86 MB to 185 MB. Latency p50 138 ms, p95 723 ms (most of it building cold databases).
- **Subagent-reported:** registering the fleet takes about 20 ms; after 12 s idle everything is asleep again with descriptors back to about 10, but resident memory stays near 180 to 200 MB (not returned to the OS, DuckDB being the likely main cost).
- **The cap limits connections, not wakes.** `on_wake` runs before a database has a place under the cap, so wake work (fetching from object storage) is unbounded: before the app added its own limit of 8, one fan-out built about 150 cold databases at once and peaked at 388 MB.
- **A hook cannot write to its own database:** `db.execute_write` inside `on_wake` deadlocks on the wake lock, so setup goes through a separate temporary `Database`, which the cap does not count.
- **`asyncio.to_thread` in a hook adds threads** beyond the registry's fixed pool (17 extra); hooks should use `registry.executor`.
- **A registry had no way to start a database Cold**; only `evict()` set that state. Fixed with `add(cold=True)`.
- **Possible bug, read in code and not tested:** the hub's cold count is not decremented when a cold database is closed or removed.
- **Startup wall time is 6 to 14 s, almost all Python imports from this Dropbox folder** (0.37 s of CPU). Not a library cost.

### Idea: to discuss

- **Bound concurrent wakes in the registry** (`max_concurrent_wakes`), or take the cap place before running `on_wake`. Object-storage fetches are exactly the work that needs bounding.
- **A hook context that allows setup writes during the first wake.**
- **Pass the reason to hooks:** `on_sleep(db, reason="idle" | "cap" | "explicit")`.
- **Counts by state and per-database open counts in `RegistryStats`,** so a status page need not read 1,000 properties.
- **`Registry.add_many()`.**

### Decision: add a DuckDB worker-process backend (beyond the spec)

The spec rules a DuckDB worker out of scope. Simon asked for one once the SQLite worker exists, to test whether it fixes DuckDB not returning memory after sleep: in-process, an awake instance costs about 5 MB and sleeping gives little of it back, and the demo's resident memory stayed near 180 to 200 MB after all 1,000 databases slept. A worker that exits on sleep returns everything to the OS by definition. The milestone 4 subagent has been told to make the worker engine-pluggable so this is a thin addition. To measure: resident memory of parent plus children before, during and after a fan-out, against the in-process DuckDB backend, and the per-query cost of the process boundary for analytical result sets.

### Decision: add a psycopg 3 PostgreSQL backend to compare with asyncpg

Simon asked for this on 2026-11-23 (his words: "a psycopg3 worker so we can compare it with asyncpg"). I read that as a second PostgreSQL backend on psycopg 3's native asyncio API, registered in the conformance suite next to the asyncpg one, with a problem-by-problem comparison and a benchmark. It answers the spec's open question 1 with evidence. Whether to also run it inside the worker process is a separate question the subagent will assess; the generic worker makes that cheap to try afterwards.

### Found: demo fleet worker databases were built from the wrong template (fixed)

Once the worker backend landed, opening any fleet worker database returned a 500, "file is not a database": the fleet code picked the DuckDB template for every kind except in-process SQLite. Fixed in `1b9c71e`, with a test that opens one fleet member of every backend kind (it failed before the fix). The demo tests had passed because none opened a worker member of the fleet. After the fix, on a fresh server: all 1,000 databases registered and none unavailable; a 500-way fan-out gave 500 of 500 reads in 1.44 s over 192 databases with open connections at the cap of 40.

A second gap this shows: an unexpected `QueryError` from `db.schema()` reaches the app as a 500. The demo should render it as an error page, and the library's error for "this file is not a database" could be more specific than `QueryError`.

## 2026-11-23: psycopg 3 backend and driver comparison

Subagent-reported. All timings were taken with this machine at a load average above 200 (several subagents running test suites at once), so absolute numbers are noisy; the subagent says the ratios held across three runs. The benchmark must be re-run on a quiet machine before anyone quotes it.

### Built

- `dblayer/backends/postgres_psycopg.py` (`PostgresPsycopg`), registered in the conformance suite as `postgres-psycopg`: 361 passed, 17 skipped, every skip from a capability flag. Eleven commits, `202ce89` to `a33dad1`.
- Shared PostgreSQL code factored into `backends/_postgres_common.py`.
- `tests/test_postgres_drivers.py`: 18 tests that pin down each difference below. `tests/latency_proxy.py`: a 20 ms latency proxy for counting round trips.
- `benchmarks/bench_postgres_drivers.py`.
- A fifth featured database in the demo, "PostgreSQL (psycopg 3)".

### Found: asyncpg problems, checked against psycopg 3

| asyncpg problem | psycopg 3 |
| --- | --- |
| Needs private driver APIs | No. Public API only. |
| Stale cached statements after DDL | Same failure with prepared statements on; the backend leaves them off by default and nothing goes stale. |
| `executemany` gives no row counts | Gives the total. |
| Untyped parameters need casts | Work without casts. Default int2 typing overflows in `$1*$2`, so the backend sends int4 or larger. |
| A timer per cached statement | None. |
| Connection stranded on a closed loop cannot be closed | `close()` closes the descriptor; the xfail test passes on psycopg. |
| `max_result_bytes` checked only after the fetch | Bounded while streaming: peak 1.1 MB against asyncpg's 56 MB for a 1 MB limit on a 50 MB result. |
| A savepoint per statement costs an extra round trip | SAVEPOINT, statement and RELEASE go in one pipeline: 1.30 round trips against asyncpg's 2.54. |
| Binary COPY refuses an int for a text column | Text COPY accepts it. |
| Catalog "char" columns arrive as bytes | Arrive as str. |

psycopg is worse in one place: a Python bool for a native boolean column needs `:b::boolean`.

Benchmark under load, asyncpg against psycopg: one-row read 515 µs against 126 µs; 100,000-row fetch 189 ms against 163 ms; 100,000-row insert by COPY 544 ms against 430 ms; 100,000-row insert by the default path 1.37 s against 3.07 s (psycopg re-parses each row when unprepared); statement in a transaction with savepoints 965 µs against 183 µs.

Two psycopg problems turned up, both worked around:

- **A Flush message after Sync disarms PostgreSQL's `idle_in_transaction_session_timeout`,** and psycopg sends exactly that at the end of every pipeline, so the server-side deadline would never fire. The backend sets `transaction_timeout` on PostgreSQL 17+ and re-arms the timer with an empty query on older servers. Script: `scripts/psycopg_flush_disarms_idle_timeout.py`.
- **With prepared statements on, a pipeline holding BEGIN, a statement and ROLLBACK leaves psycopg's statement cache out of step with the server.** Script: `scripts/psycopg_prepared_rollback_pipeline.py`.

### Decision

- **Recommendation for open question 1: psycopg 3.** It removes the private-API dependency, the stale-cache trouble, the timers, the stranded-socket leak and the missing counts; it makes `max_result_bytes` protect memory; and pipelines make statement savepoints nearly free, which matters most against a remote server. Both backends stay in the tree until Simon decides.
- Placeholders reach psycopg as `$n` on its raw cursors, reusing the shared rewriter, since `%s` style would need every `%` in the SQL doubled.
- The psycopg backend refuses more than one statement per call, as the DuckDB backend does.

### Idea: to discuss

- **PostgreSQL in the worker process is not worthwhile in general.** The query runs in the server, cancel already works server-side, and psycopg bounds result memory. Killing a worker would not stop the server statement. It would only serve as a test that the worker is engine-neutral.
- **`transaction_timeout` for the asyncpg backend too:** it bounds a running statement, not only an idle transaction.
- **Prepare only the `executemany` statement** on psycopg to fix the slow default-path insert.
- **A "remote server" column in the benchmark** using the latency proxy. Local-socket numbers flatter drivers that make extra round trips.
- **Booleans:** portable booleans are sent as 1/0, which is why native boolean columns are awkward. Worth revisiting once there is a decision on portable types.

### Upstream suggestions

- None for sqlite-utils or Datasette.
- **PostgreSQL:** a Flush message received while idle in a transaction disables the idle timer without re-arming it. Verified by the subagent with a raw-libpq script.
- **psycopg:** sends Flush after Sync on pipeline exit (verified, same script); prepared-statement cache out of step after ROLLBACK in a pipeline (minimal script, cause not traced); tries to prepare multi-statement strings with `prepare_threshold=0` (seen, no script).

### Found: process

- Running five subagents' test suites at once pushed this machine's load average above 200. A full-suite run that takes about two minutes took 22. Benchmarks and timing tests are meaningless in that state; the final benchmark milestone must run alone.

## 2026-11-23: milestone 4 done (SQLite in a worker process)

Subagent-reported unless marked. I have used the backend through the demo (featured database and 20 fleet members browse, a 500-way fan-out succeeded), but have not yet re-run the full suite on a quiet machine.

### Built

- Twelve commits, `6ca6fd9` to `b49d56c`: `dblayer/worker/protocol.py`, `dblayer/worker/__main__.py`, `dblayer/worker/engines.py`, `dblayer/backends/sqlite_worker.py`, `docs/worker-protocol.md` (with a recorded session, regenerated from a test), `benchmarks/worker_overhead.py`, `tests/worker/`, `tests/faults/`.
- The conformance suite runs on `sqlite-worker` (JSON) and `sqlite-worker-msgpack`: 768 tests, skipping only what in-process SQLite skips.
- Both placement policies: dedicated (a process per connection) and shared (a process serving many databases).
- CI run 45339370936: Linux 3.12 green with 3,084 passed; macOS and stdlib-only green. The Linux-only `RLIMIT_AS` tests ran and passed there. Linux 3.13 and 3.14 failed on two timing tests in other backends.

### Found: answers to the spec's questions

- **Can interactive transactions work across a process boundary without stalling other writers? Yes.** The deadline is enforced on both sides: with the parent's event loop blocked, the worker still rolled back at its 200 ms deadline and another process could write at once. A statement that ignores interrupts at the deadline is killed, and the caller gets `TransactionExpired` about 800 ms after a 300 ms deadline.
- **Does a hard kill ever corrupt a database or leave a lock? No,** in rollback-journal or WAL mode: `integrity_check` is ok, an independent `BEGIN IMMEDIATE` succeeds at once, no partial write is visible. One real problem found, next item.
- **A writer killed mid-transaction in rollback-journal mode leaves a hot journal that `mode=ro` readers cannot roll back.** Every read then fails with `SQLITE_READONLY_ROLLBACK` until a read-write connection opens the file. Fixed in `SqliteInProcess`: a read connection that finds a `-journal` file first opens briefly read-write.
- **Is the worker fast enough to be the default for untrusted SQL? For running queries, probably; for starting, no.** Provisional numbers (1-minute load about 17 on 18 cores; to be re-run alone):

| Measure | In-process | Worker, JSON | Worker, MessagePack |
| --- | --- | --- | --- |
| One-row read, median | 95 µs | 322 µs | 316 µs |
| One-row update | 538 µs | 790 µs | 906 µs |
| Per row, 100,000-row read | 0.58 µs | 1.90 µs | 1.12 µs |
| Insert with alter: interactive / one `op` | 1,520 / 1,492 µs | 2,916 / 2,217 µs | 2,832 / 2,128 µs |
| Open to first result | 0.44 ms | 86.6 ms | 86.2 ms |

  The spec's hypothesis "well under a millisecond for a small query" holds once a worker is running (about 0.2 to 0.3 ms added). Starting one costs about 85 ms: roughly 20 ms of Python startup and 50 ms of imports. On shared placement, later databases on a running process open in 0.76 ms.

- **Queries that neither the progress handler nor `interrupt()` can stop** (each is one long C function call; SQLite 3.53.1). All were killed by the parent at about 450 ms with a 200 ms limit:

| Query | Runs for |
| --- | --- |
| `instr(printf('%.*c', 2000000,'a'), printf('%.*c',100000,'a')\|\|'b')` | 2.6 to 4.6 s |
| A 200,000-character string `like` a 40,000-character pattern | about 4.8 s |
| The same with `glob` | about 2.2 s at 120,000 |
| `length(randomblob(1000000000))` | 1.0 to 1.5 s, 1 GB |
| `length(printf('%*d', 1000000000, 1))` | 0.9 to 2 s, 1 GB |

  A different class evades only the progress handler: `sum(length(randomblob(1e8)))` over 30 rows runs fewer than 1,000 VDBE opcodes, so a handler at n=1000 never fires, though `interrupt()` stops it in 95 ms.

- **Scale test at 1,000 worker databases:** 884 B per asleep database; 86 requests/s at saturation, because nearly every request starts a process; descriptors peak at 201 (3 per place).
- **DuckDB locks its file per process,** so a database's read and write workers cannot be separate processes. All of one database's connections must share a worker.

### Decision: departures from the spec

- **`subprocess.Popen` with `loop.add_reader` on non-blocking pipes,** not `asyncio.create_subprocess_exec`. asyncio uses a thread per child to wait for it on macOS, which breaks the constant-thread-count guarantee, and its transports cannot be closed from another event loop.
- **The worker is engine-pluggable.** `open` names an engine (`module:callable`) and its config; the worker drives it only through the `Backend` protocols. The worker runs an event loop on its main thread with the engine's blocking calls on one executor thread, where the spec had statements on the main thread and a reader thread.
- **Extra messages:** `close`, `batch`, an unsolicited `expired` notice, and `in_tx` on replies so the pool's reset-on-return costs no round trip. `schema` always returns the whole schema.
- **A value-type registry in the codec** (`register_value_type`) for dates, decimals and UUIDs; nothing registered by default.
- **`dblayer/__init__.py` imports lazily** so a worker start does not import the whole library.
- **The parent closes a worker by closing its stdin;** the `sleep` message exists but is unused.

### Idea: to discuss

- **A small pool of pre-started spare workers** would take the 85 ms out of each wake. This looks necessary before the worker can be the default for a fleet of mostly asleep databases.
- **Trim worker start-up:** 50 ms of it is imports. A worker that does not import asyncio, or a zygote that forks per connection, could get near the 20 ms interpreter floor or below.
- **Pool health check in the core** for connections whose worker has died while idle.
- **Shared placement keyed by database** as the default for DuckDB and for cold fleets.

### Upstream suggestions

- **Datasette: hot journal with read-only connections.** Datasette opens mutable databases for reading with `?mode=ro` (`datasette/database.py`). If a write is killed mid-transaction in rollback-journal mode, every read fails with `SQLITE_READONLY_ROLLBACK` until something opens the file read-write. Verified by the subagent with a minimal script on SQLite 3.53.1; not reproduced through Datasette itself.
- **Datasette issue 1270 (use `interrupt()` instead of the progress handler):** `interrupt()` fixes the low-opcode-count case above, but the single-call queries in the table evade both. Only a separate process bounds them. Worth adding those queries to that issue as evidence. Verified on SQLite 3.53.1 outside Datasette.
- **Datasette `sql_time_limit_ms` documentation** could say plainly that it does not bound single long function calls.

### Decision: add an experimental SQLite worker written in C (beyond the spec)

Simon asked for this on 2026-11-23 in response to the 85 ms Python worker start-up. A C program that speaks the same wire protocol and links SQLite directly, registered as a `sqlite-cworker` backend. It cannot run named operations as one message (they are Python functions; it falls back to round trips) or Python init hooks. Besides start-up time, it is the first second implementation of the protocol, so it tests whether `docs/worker-protocol.md` is really a specification. It must be run under AddressSanitizer: a memory-unsafe worker handling results of untrusted SQL would defeat its purpose.

## 2026-11-23: DuckDB in a worker process

Subagent-reported. Timings were taken at load 9.5 to 14 (Spotlight was indexing) and are provisional; memory figures are less sensitive to load. macOS only: no Linux memory figures yet.

### Found: the premise was partly wrong

- **Correction to earlier notes.** The "DuckDB does not return memory after sleep" finding (resident memory staying near 180 to 200 MB in the demo, "sleeping returned only 114 MB of 498 MB") was measured with RSS. On macOS, RSS keeps counting freed pages until the OS reclaims them under pressure. Measured by physical footprint (`ri_phys_footprint`, Activity Monitor's "Memory" column), in-process DuckDB gives most of it back: 150 databases peak at about 406 to 471 MB and settle at about 100 to 175 MB after sleep, against a 48 MB start.
- **What remains in-process:** footprint after sleep creeps slowly over cycles (154 to 174 MB over cycles 3 to 8).
- **No in-process trim helped:** `gc.collect()`, `malloc_zone_pressure_relief` (released 0 bytes) and DuckDB's allocator flush settings made no material difference. On macOS the DuckDB wheel uses the system malloc. On Linux it bundles jemalloc, so this must be re-measured there.

| Mode, 150 databases | Peak footprint | After sleep | Wake, median | Processes at peak |
| --- | --- | --- | --- | --- |
| In-process | 406 to 471 MB | 86 to 175 MB | 12 to 25 ms | 1 |
| Worker, one process per database | 2,594 MB | parent 39 to 45 MB, children 0 | 185 to 190 ms | 40 |
| Worker pool, 8 databases per process | 842 MB | parent 31 to 32 MB, children 0 | 30 to 43 ms | 8 |

- **A one-process-per-database worker returns everything but costs about 5 times the peak memory and 15 times the wake time.** Its floor is 45 MB of footprint before opening anything: 9 MB bare Python, 14 MB dblayer's own worker imports, 23 MB the duckdb import.
- **Overhead per query:** one-row read 418 to 473 µs in-process against 603 to 808 µs in a worker. 100,000 plain rows: 43 ms in-process, 95 ms MessagePack, 139 ms JSON. 100,000 rows of dates, timestamps and decimals: 40 ms in-process against 300 to 350 ms, about 3 µs per row, and MessagePack is slower than JSON there.
- **Asleep to first result:** 8.5 ms in-process, 160 ms worker, 15 ms with a pre-started spare, 9 ms on a pool process that is already up.
- **Bugs found in the worker layer along the way, all fixed:** result column types were dropped on the wire; MessagePack extension payloads did not handle nested values; JSON turned DuckDB MAP keys into strings; the worker's time-limit backstop raced DuckDB's own interrupt and lost the driver cause; `SharedWorkers` could start two processes for concurrent first opens, which is fatal with DuckDB's per-process file lock.

### Built

- `dblayer/backends/duckdb_worker.py`: `DuckDBWorker`, one process owning all of a database's connections and exiting on sleep. Conformance: 366 passed, 12 skipped (in-process DuckDB's 8 plus 4 executor tests that do not apply).
- Value types across the wire in both codecs: datetime with zone, date, time, Decimal, UUID, timedelta, and markers for MAP, STRUCT and arrays.
- Two experimental designs beyond the brief: `DuckDBWorkerPool` (several awake databases per process; a process exits when its last database sleeps) and `SpareWorkers` (pre-started workers, with a `--preload` option).
- `benchmarks/duckdb_memory.py`.
- Demo: a sixth featured database, the fleet's DuckDB members split 100 in-process and 50 worker, and a stats panel that shows this process and the worker processes separately, by footprint and RSS.
- Twelve commits, `8310b3a` to `5b2e4b4`.

### Decision

- **Recommendation: do not run DuckDB one-process-per-database by default.** Use a worker for untrusted SQL, where the hard kill and `RLIMIT_AS` are the point. For fleets, pool plus spares is the version worth making the default, after a Linux measurement and a decision on its cost: killing a pool process takes down every database on it.
- The parent process no longer imports `duckdb` when using the worker.

### Idea: to discuss

- **Report footprint, not RSS, everywhere memory is quoted** (the scale test and the earlier demo numbers used RSS). Earlier memory figures in these notes should be read with that in mind.
- **Trim the worker's own 14 MB of imports,** which also bears on the 85 ms start-up.
- **A faster wire form for typed values** (epoch integers for dates and timestamps); the per-value `packb` call is what makes MessagePack slower than JSON for typed rows.
- **One worker serves requests one at a time,** so a long analytical query delays every other query on that database, or on that pool process. In-process DuckDB runs cursors concurrently. This is a real functional cost of the worker for DuckDB.
- **Spares shared across engines** with lazy preload.

### Upstream suggestions

- None for sqlite-utils.
- **Datasette, documentation note:** on macOS, memory that looks "not returned" is often RSS counting freed pages; footprint is the honest figure. Verified here for DuckDB only.

### Unverified or fragile

- The DuckDB worker's `RLIMIT_AS` test had not finished on Linux CI when the subagent stopped; DuckDB under an address-space limit may fail at import.
- Pool bookkeeping when a pooled process is killed mid-placement is only exercised indirectly.

## 2026-11-24: experimental C worker for SQLite

Subagent-reported. Benchmarks were taken at load about 5.5 (Spotlight indexing) and are provisional. I built the binary myself against the system SQLite and used it through the demo: the featured "SQLite, in a C worker process" database browses and answers queries.

### Built

- `cworker/`: about 4,000 lines of C11 (`worker.c`, `codec.c`, `schema.c`, `sqlutil.c`), speaking `docs/worker-protocol.md` with both JSON and MessagePack. Build with `make -C cworker` (compiles in a pinned SQLite 3.53.1 amalgamation, downloaded once and checked against a SHA-256) or `make -C cworker SQLITE=system`.
- `dblayer/backends/sqlite_cworker.py`: `SqliteCWorker(path)`, registered as `sqlite-cworker`. Conformance and fault tests: 819 passed, 22 skipped, the same skips as the Python worker.
- A differential test against in-process SQLite, malformed-frame tests and a seeded fuzz of the decoders.
- CI builds it on Linux and macOS and runs its tests under AddressSanitizer and UBSan. Fifteen commits, `e047fd8` to `848628e`.

### Found

| Measure | In-process | Python worker | C worker |
| --- | --- | --- | --- |
| Open to first result, new database | 0.23 ms | 60.9 ms | 3.4 ms |
| One-row read, median | 64 µs | about 200 µs | 53 µs |
| Per row, 100,000-row read (MessagePack) | 0.43 µs | 0.84 µs | 0.47 µs |
| One-row update | 250 µs | about 360 µs | about 230 µs |
| Idle worker resident memory | n/a | 38 MB | 3 MB |
| 1,000-database scale test | n/a | 232 requests/s | 633 requests/s |

- **A C worker starts in about the time it takes to spawn any process.** Spawning `/usr/bin/true` takes 1.9 ms; the C worker reaches `hello` in 2.7 ms and its first result in 2.9 ms. The Python worker's 56 ms to `hello` is 9 to 14 ms of interpreter and the rest imports.
- **The C worker's round trip beats in-process SQLite** (53 µs against 64 µs), because the in-process backend pays for a hop to an executor thread on every call. That says the in-process backend's thread hop is now the larger cost for small queries.
- **The sanitizers earned their place.** Linux CI found two real bugs macOS missed: `qsort(NULL, 0)` on tables with no indexes (undefined behaviour under glibc), and a heap use-after-free in the watchdog at shutdown, hit by the fuzz test. Both fixed.
- **Apple's system SQLite ignores `PRAGMA hard_heap_limit`** (it reads back 0) and its header does not declare `sqlite3_hard_heap_limit64`. A heap limit is not enforceable against that library. This is why the default build compiles the amalgamation in.
- **The protocol document was not yet a specification.** Writing a second implementation found: JSON escaping of dicts that look like codec wrappers is incomplete inside lists, `params_seq` and batch statements; `hello` could not say what a worker supports (now `implementation` and `ops`); capabilities are derived from the parent's SQLite version, not the worker's; and several behaviours are defined only by what CPython's `sqlite3` module does (error class names, `rowcount` and `lastrowid` rules, the 256-row batching of the result-size check).

### Decision

- **Named operations run in the parent** on this backend, with round trips, since they are Python functions. The worker says so in `hello`, so no wasted message is sent. Python init hooks raise `NotSupported` at construction; loading SQLite extensions by path is the C equivalent, off by default.
- **Recommendation from the subagent: keep it, as experimental.** It makes a worker per database affordable. The cost is about 4,000 lines of C to keep in step with CPython's `sqlite3` behaviour, and a permanent need for sanitizer CI on Linux.

### Idea: to discuss

- **Pre-started C spares cost 3 MB each,** against 38 MB for Python spares.
- **Make the C worker the default for the demo fleet** and for cold fleets in general.
- **Remove the executor hop for in-process SQLite reads that are known to be fast,** or accept that a C worker is the faster path.
- **Put the CPython-derived semantics into the protocol document,** and pass engine capabilities in `hello`.
- **A nightly, longer fuzz run.**

### Unverified or fragile

- One test fails only under ASan on Linux: `test_cap_bounds_descriptors_under_load[sqlite-cworker]` ends with fewer descriptors than its baseline. Not understood.
- The sanitizer job leaves out the `RLIMIT_AS` tests after its first run hung for 40 minutes; the cause (a guess: `RLIMIT_AS` against ASan's shadow memory) is unconfirmed.
- The amalgamation checksum is trust-on-first-download.

### Upstream suggestions

- **Datasette, unverified:** on macOS, a Python linked against Apple's system SQLite cannot enforce a heap limit, because that library ignores the pragma. Worth a line in any future documentation of memory limits.

## 2026-11-24: timing-dependent tests stabilised; two real races found

Subagent-reported.

### Found

- **A cancel or time limit could be lost, on DuckDB and on in-process SQLite.** Both engines drop an interrupt that arrives before the statement has really started. A task cancelled just after its statement reached the thread left the statement running, forever for an unbounded query. Reproduced: SQLite hung at the 17th of 1,000 random-point cancels; DuckDB within about 7. On DuckDB the stray interrupt could also land on the read wrapper's ROLLBACK and leave an aborted transaction on a pooled cursor. Fixed in both backends: a statement refuses to start once cancelled, and the cancel re-interrupts every 5 ms until the call returns. After the fix, 1,000 of 1,000 (SQLite) and 500 of 500 (DuckDB) return. DuckDB's time-limit overshoot fell from about 29 ms to about 5 ms.
- **The DuckDB stream path still has the single-interrupt window.** Not yet fixed.
- **The `sqlite-worker` backend can hang after a cancellation under heavy load** ("WorkerCrashed exception in shielded future", then later tests in the same process hang). Not yet investigated.
- **`test_max_rows_stops_an_unbounded_query[duckdb-worker]` failed once on Linux 3.14 in CI:** the worker did not stop an unbounded recursive query within 5 s and was killed. Cause not found. It may be the same lost-interrupt race inside the worker.
- **Measured latencies for the spec's promises, on a quiet machine:** cancel to write slot released 1.1 ms median (spec 50 ms); DuckDB time-limit overshoot 4.4 ms (spec 100 ms); deadline to other writer proceeding 2.3 ms. With 18 CPU burners and two suite copies running they stayed under 50 ms at worst.
- **CPU burners ran from about 04:52 to 06:34 UTC on 2026-11-24** (20:52 to 22:34 local on the 23rd). Any timing in these notes taken in that window is unreliable, which includes the psycopg benchmark and the first worker overhead numbers.

### Built

- Tests now wait for the real event, move a hand-driven clock (`IdleClock`), or count (round trips, loop iterations) instead of asserting on elapsed time.
- `helpers.check_latency`: always asserts a 10 s functional bound, prints the measured figure, and asserts the spec's tight bound only when `DBLAYER_STRICT_TIMING=1`. Documented in the README.
- Five concurrent full-suite runs all passed: 3,082 passed, 88 skipped each.

### Idea: to discuss

- **Make `Backend.interrupt()`'s contract explicit:** it is a no-op when no statement is running, so callers must repeat it until the call returns.
- **A distinct "sleeping" state,** or a public way to await the end of a sleep; tests currently reach into a private attribute.

### Upstream suggestions

- **Datasette and sqlite-utils:** any code that stops a query by calling `conn.interrupt()` from another thread has the lost-interrupt window if the call arrives just before the statement steps. The fix is to repeat the interrupt until the query returns, or to set a flag the progress handler checks. Verified here on Python's `sqlite3` with a stress script (hung at the 17th of 1,000 cancels); not checked against Datasette's own code, whose time limit uses the progress handler and so may not be affected. Relevant to Datasette issue 1270, which proposes moving to `interrupt()`.
- **DuckDB itself:** `interrupt()` on an idle connection is silently dropped. Verified on 2.0.1 with a minimal script.

## 2026-11-24: proposed further backends (ideas only, none built)

Asked by Simon: more backend types worth exploring for maximum performance and minimum memory and resource use. Ranked by expected value for this project's goals. Expected gains are estimates from the measurements above, not results.

1. **SQLite in a WebAssembly sandbox, in-process** (SQLite compiled to WASM, run under wasmtime). Hard, exact memory cap per instance and CPU "fuel" metering that stops the queries no progress handler or interrupt can stop, without a process per database and without a kill. Instances start in well under a millisecond. Cost: probably 1.5 to 3 times slower per query than native, and a new dependency.
2. **Multi-tenant C worker** (one C process, a thread per open database). Keeps the 53 µs round trip and removes the 3 ms start and 3 MB per database, at the price of a shared blast radius: a kill takes every database on the process. Pair with a small pool of processes, as the DuckDB pool did.
3. **Fork-server Python worker.** A pre-imported parent forks a child per connection: start near the cost of fork (about 1 to 2 ms) with imports shared copy-on-write, while keeping what the C worker cannot do, namely named operations as one message and Python init hooks. Linux-first; fork on macOS is fragile.
4. **In-process SQLite without the executor hop.** The C worker's round trip (53 µs) beats in-process (64 µs) because every in-process call hops to a thread. Options: run statements inline on the event loop under a small opcode budget and fall back to a thread when the budget is exceeded; or a native extension that releases the GIL and encodes rows in C.
5. **Shared PostgreSQL connections across schemas.** Today each awake `Database` (a schema) holds its own server connections. One pool per server, with `SET LOCAL search_path` per transaction, would let thousands of schemas share a few dozen connections, and would work behind PgBouncer in transaction mode.
6. **Immutable SQLite backend** (`immutable=1` plus memory mapping). For read-only databases, the common Datasette case: no locks, no journal or WAL descriptors, and the page cache shared between processes by the OS.
7. **Object-storage-native backends.** SQLite through an HTTP range-request VFS (in the C worker), and DuckDB over Parquet files in place. A Cold database then needs no download at all; this replaces the `on_wake` copy with reads on demand.
8. **DuckDB worker in C or C++ against libduckdb.** The Python DuckDB worker's 45 MB floor is 9 MB of interpreter, 14 MB of dblayer imports and 23 MB of the duckdb module. A native worker should cut most of the first two.
9. **Columnar wire format for large results** (Arrow IPC over the pipe, or a shared-memory segment). Per-row cost is now mostly encoding and decoding (0.3 to 0.5 µs per row for MessagePack; about 3 µs for dates and decimals). Matters most for the DuckDB worker.
10. **apsw-based in-process SQLite.** Finer control than the standard module: real statement-level interrupt, VFS access, no implicit transaction handling, and the ability to pin the SQLite version, which removes the "which SQLite is this" differences found with Apple's library.

### Decision: build six of the proposed backends (2026-11-24)

Simon chose these from the proposals above, each built by its own subagent in its own worktree, in parallel:

1. SQLite in WebAssembly, in-process, under wasmtime (`sqlite-wasm`).
2. SQLite in WebAssembly inside a separate Node.js process speaking the worker protocol (`sqlite-nodeworker`).
3. Multi-tenant C worker: one C process, a thread per connection, many databases (`sqlite-cpool`).
4. DuckDB worker in C++ against libduckdb (`duckdb-cppworker`).
5. DuckDB worker in Rust (`duckdb-rsworker`), built separately from the C++ one for comparison.
6. In-process SQLite on apsw (`sqlite-apsw`), including an experiment in removing the executor thread hop.

Known risks set out in the briefs: WASI has no file locks or shared memory, so both WebAssembly backends must show how concurrent access to one file is made safe; a multi-tenant worker shares its blast radius on a kill; native DuckDB workers must reproduce the Python backend's read-only enforcement and aborted-COMMIT guard. With seven subagents running at once, every benchmark from this round is provisional until re-run alone.

### Decision: experiment with backend-side serialisation of CSV, TSV and JSON (2026-11-24)

Simon's idea: today the Python server must turn rows into CSV, TSV or JSON itself, which is expensive for large results. Add an optional capability where the backend produces the bytes and Python only forwards them. One subagent is trying it on three backends, chosen to cover three different places the work can happen:

- **C SQLite worker** (required by Simon): serialise in C in the worker process, straight from SQLite's column values; also try a separate data pipe so bytes bypass the message codec.
- **DuckDB in-process:** the engine's own vectorised CSV and JSON writers, with no process boundary.
- **PostgreSQL on psycopg 3:** `COPY (query) TO STDOUT`, so the server serialises and the bytes pass through two hops.

The core gets a Python fallback so `export()` works on every backend and serves as the baseline. The hard part is parity: three engines format floats, timestamps and NULLs differently, so the experiment must say, per format and backend, whether native output is byte-identical to the fallback, equal after parsing, or has to fall back. The numbers that matter are CPU time in the Python process and event-loop lag during a large export, not wall time alone.

## 2026-11-24: ambitious ideas for the process-backend pattern (ideas only)

Asked by Simon: wildly ambitious directions for out-of-process backends, "Haskell or Erlang or something that talks over a network to a novel service". None built. Gains are guesses.

1. **Hand the worker the client's socket.** For large exports, pass the HTTP connection's file descriptor to the worker (`SCM_RIGHTS` over a Unix socket) and let it write the CSV or JSON response itself. Python frames the headers and never sees the body. The logical end of the native export experiment.
2. **A database daemon shared by every web worker** (Rust, over a Unix socket; QUIC for remote). Today each Python process has its own registry, cap and write queue, so several uvicorn workers cannot share a SQLite write slot. A long-lived daemon owns them once, survives web-server restarts, and keeps hot databases open.
3. **Group commit in that daemon.** Coalesce independent write transactions from many callers into one SQLite transaction, a savepoint each, one fsync for all. Write slot hold time is dominated by the commit, so this could multiply write throughput. Haskell's STM is a natural fit for composing the queue, deadlines and rollback of only the failed members.
4. **Erlang/OTP as the supervisor.** The registry (sleep, wake, cap, wake lock, restart after kill) is a hand-written subset of OTP. A BEAM node could supervise the C workers as ports, one lightweight process per database, with restart strategies, backpressure through mailboxes and distribution across machines built in.
5. **PostgreSQL in WebAssembly (PGlite) inside the Node worker.** A real PostgreSQL per database with no server, sleeping to a directory on disk. Thousands of tiny, isolated PostgreSQL databases under the same registry.
6. **SQLite with concurrent writers:** mvSQLite (SQLite on FoundationDB, MVCC) or libSQL/Turso over its Hrana protocol with embedded replicas. Would flip `concurrent_writers` to true for a SQLite dialect and make a database reachable from many machines.
7. **Arrow Flight SQL / ADBC backend.** One columnar network protocol reaches DuckDB servers, DataFusion, InfluxDB 3 and others; results arrive as Arrow batches with no per-row decoding.
8. **A DataFusion (Rust) worker over Parquet or Iceberg in object storage.** A "database" is a prefix in a bucket; there is nothing to copy on wake. The Cold state disappears.
9. **Incremental-view engines over the PostgreSQL wire protocol** (Materialize, RisingWave). A backend whose queries stay continuously up to date, with a subscription API: Datasette pages that update live.
10. **ClickHouse through chDB or `clickhouse-local` as a worker.** An engine with its own output formats built in, so native export costs nothing to implement there.
11. **A microVM per untrusted database** (Firecracker snapshot restore, a few milliseconds). The strongest containment available, and it would allow untrusted extensions and user-defined functions, which no other option here can.
12. **Deterministic simulation testing of the worker protocol,** in the FoundationDB and TigerBeetle style: a simulated clock, scheduler and failure injector driving kills, lost frames and deadline races reproducibly from a seed.

### Decision: build a PGlite backend and a ClickHouse backend (2026-11-24)

Simon chose two of the ambitious ideas above, each given to its own subagent:

- **PGlite in a Node.js worker** (`pglite`): a real PostgreSQL per database with no server, one data directory per `Database`. Open questions put to the subagent: PGlite is a single session, so how do dblayer's pooled connections and transactions map onto it; durability after a kill; whether anything short of a kill can stop a running query; start-up and memory per awake database.
- **ClickHouse** (`clickhouse`), chDB in-process as the first form to try. It is the engine furthest from the others: no general transactions, no enforced uniqueness or foreign keys, mutations in place of row updates. It is the hardest test so far of the capability-flag approach, and a natural fit for the native export experiment.

### Decision: try the socket handoff (2026-11-24)

Simon asked for idea 1 from the ambitious list. It is added to the export experiment as a fourth transport for the C SQLite worker: Python sends the HTTP headers, then passes the client connection's file descriptor to the worker, which writes the body itself. Known preconditions to be confirmed by the experiment: plain HTTP/1.1 only (no TLS terminated in Python, no HTTP/2), no compression or other response middleware, and ASGI gives an application no supported way to reach the socket.

## 2026-11-24: in-process SQLite on apsw

Subagent-reported. Timings taken at load 9 to 17 with nine other subagents running; provisional. CI result unknown (runs were cancelled by later pushes, then `gh` was rate-limited).

### Built

- `dblayer/backends/sqlite_apsw.py` (`SqliteApsw`), optional extra `apsw`, registered as `sqlite-apsw`: 372 passed, 6 skipped, the same six skips as the standard-library backend. Also passes with inline reads switched on. 49 apsw-specific tests.
- `dblayer/backends/_sqlite_common.py`: dialect, error mapping and schema reading shared by both in-process SQLite backends.
- Twelve commits, `ca42c7c` to `c1c8959`.

### Found

- **Removing the thread hop works.** With `inline_reads=True`, a cheap read runs on the event loop thread under a budget of 10,000 opcodes or 2 ms. A one-row read drops from about 61 µs to 9.6 µs, against 53 µs for the C worker. Over budget in mid-step, the read-only statement is stopped and re-run on a thread; over budget between rows, the same cursor carries on from another thread. The longest event-loop gap during a slow query was 0.31 ms.
- **Most of that is available on the standard `sqlite3` module too.** A raw measurement outside dblayer: 3.2 µs inline against 29 µs through an executor. What the standard module lacks is a way to know a statement is read-only before running it.
- **`SqliteInProcess` has a `last_pk` bug:** the before/after `last_insert_rowid()` comparison reports `None` for an insert that reuses the previous insert's rowid (insert 5, delete, insert 5). apsw's sentinel approach gets it right and needs no extra SQL.
- **apsw gives result columns their declared types,** through views too; the standard module cannot.
- **A real but process-wide heap limit.** `hard_heap_limit` stops a 200 MB `randomblob` with the database still usable, but it is one limit for every apsw database in the process, last value wins, and it counts only SQLite's own allocations.
- **An "untrusted" mode is cheap:** an authorizer plus DEFENSIVE, TRUSTED_SCHEMA off and an attach limit of zero blocks ATTACH, pragma writes, `load_extension` and temp objects, while leaving ordinary reads and introspection working.
- **`sqlite3_stmt_readonly` is true for `ATTACH`, `BEGIN` and `PRAGMA x = 1`,** so it cannot be the only check for untrusted SQL.
- **The memdb idea from earlier notes does not work for us.** A memdb write transaction shuts readers out, which breaks the core's rule that reads proceed alongside a writer. apsw's wheel also omits shared cache. So apsw has no usable multi-connection in-memory database.
- **The lost-interrupt window can be closed by design:** a cancel flag read by the progress handler, plus one interrupt. 200 random-point cancels all returned.
- Per-row cost is about 15 to 20% lower than the standard module. One-row updates and bulk inserts are the same.
- apsw bundles SQLite 3.54.1; uv's Python has 3.53.1.

### Decision

- **Recommendation from the subagent: offer both, keep the standard module as the default,** and port inline reads to `SqliteInProcess` as the next step. Revisit if heap limits or untrusted SQL become requirements.

### Idea: to discuss

- **Port inline reads to the standard-library backend.** This is probably the largest single speed-up available for the default path: about six times on small reads.
- **Fix `last_pk` in `SqliteInProcess`** (rowid reuse).
- **Expose SQLite memory status in `RegistryStats`.**
- **A busy handler that checks the cancel flag,** so a cancel also ends a wait for a lock.
- **A custom VFS reading by HTTP range requests** for cold databases in object storage, with `immutable=1`.

### Upstream suggestions

- **Datasette: an inline fast path for cheap queries.** Running a query on the event loop thread under an opcode budget, and falling back to a thread when it is exceeded, cut a one-row read about six times here, and a raw test suggests it works on the standard `sqlite3` module. Measured in dblayer only; Datasette's `execute_fn` path not tested.
- **Datasette: lock down arbitrary SQL with an authorizer** plus DEFENSIVE, TRUSTED_SCHEMA=0 and an attach limit of zero. The authorizer half works on the standard module (`set_authorizer`). Behaviour verified here; Datasette's current protections not compared.
- **Datasette, issue 1270:** a cancel flag read by the progress handler closes the lost-interrupt window without repeated interrupts. Verified here with 200 cancels.
- **Datasette: typed result columns** need `sqlite3_column_decltype`, which the standard module does not expose. A reason to consider apsw as an optional driver.
- **sqlite-utils:** any before/after `last_insert_rowid()` comparison has the rowid-reuse blind spot found in `SqliteInProcess`. Not checked against sqlite-utils' code.

### Unverified or fragile

- Two apsw backends with different `memory_limit_mb` values in one process fight over the single heap limit.
- The inline budget does not bound a slow user-defined function or a cold disk read inside one opcode.

## 2026-11-24: DuckDB worker in C++

Subagent-reported. All timings taken at load 14 to 44; provisional. No Linux memory figures.

### Built

- `duckdb-cpp-worker/`: about 4,460 lines of C++17 against libduckdb 2.0.1 (a pinned, checksummed prebuilt download), reusing the C worker's `codec.c`. Builds in 7 seconds with `make -C duckdb-cpp-worker`; the binary is 375 KB plus a 67 MB shared library.
- `DuckDBCppWorker`, registered as `duckdb-cppworker`: 426 passed, 12 skipped, 3 expected failures. The skips match the Python DuckDB worker's. A Linux CI job for it passed, including the `RLIMIT_AS` test and an AddressSanitizer run.
- Eighteen commits, `082fdc2` to `8b3cc34`.

### Found

| Measure | In-process | Python worker | C++ worker |
| --- | --- | --- | --- |
| Process floor before opening anything | n/a | 44.7 MB | 2.1 MB |
| One database open | about 10 MB | 54.4 MB | 14.4 MB |
| Asleep to first result | 8 ms | about 190 ms | 28 ms |
| One-row read | 456 µs | about 1,000 µs | about 730 µs |
| Extra per row, plain, 100,000 rows (MessagePack) | n/a | 0.61 µs | 0.11 µs |
| Extra per row, typed, 100,000 rows (MessagePack) | n/a | 2.90 µs | 0.69 µs |
| Peak footprint, 150 databases, cap 40 | 454 MB | 2,690 MB | 1,145 MB |
| Point read while a long query runs on the same database | 0.81 ms | 6,205 ms | 1.04 ms |

- **The Python worker's cost was Python.** The floor falls from 45 MB to 2 MB and wake from 190 ms to 28 ms. What remains per process is DuckDB itself: 12 to 27 MB.
- **Concurrent reads work,** with a thread per connection. On the Python worker a long analytical query blocks every other query on that database; here a point read takes 1 ms alongside it.
- **`RLIMIT_AS` works with DuckDB** on Linux.
- **A native appender `insert_rows` is 7.3 times faster** than the default path (269 ms against 1,956 ms for 10,000 rows).
- **One real limitation, cause not found:** through DuckDB's C API (both the classic one and the new 2.0 one), an unbounded recursive CTE yields its first rows only after about 3.6 seconds, where the Python package gets them in under a second. `max_rows` is worked around by wrapping the query in a LIMIT; streams have no fix, hence the three expected failures.
- **The C API cannot print a type name the way the Python package does,** reports statement types only after prepare, and gives errors as message text only. Every value conversion the Python package makes had to be redone by hand.

### Decision

- Classic C API, not the 2.0 one, which is marked unstable and has no exception types.
- `hello` now lists which named operations a worker implements (`ops` is a list), and a worker may decline one.
- The subagent marked three export tests as expected failures in `tests/conformance/test_export.py`, a file owned by the export experiment. Recorded so it is not mistaken for that experiment's own judgement.
- **Recommendation from the subagent:** keep as experimental and prefer it to the Python worker wherever one process per database is wanted. For large fleets the next step is a multi-database C++ process.

### Idea: to discuss

- **A multi-database C++ pool process,** combining this with the pool design.
- **An Arrow wire format.** The remaining typed-row cost (0.69 µs) is now the parent's Python decoding.
- **Cache prepared statements** to cut the one-row read.
- **ThreadSanitizer in CI** for this worker, since it is multi-threaded.

### Upstream suggestions

- None for sqlite-utils or Datasette.
- **DuckDB itself:** the C API does not stream a recursive CTE's first rows the way the Python package's internal path does. Moderately verified by the subagent with minimal C programs on both APIs.

### Decision: register backends through pluggy (queued; 2026-11-24)

Simon asked that, once every running subagent has finished, dblayer use pluggy and every backend register itself through a pluggy hook. It waits until then because it touches every backend module, `tests/conftest.py` and the demo's database list, which ten subagents are editing now. Consequences to settle when it is built: pluggy becomes the core's first runtime dependency (the core is standard-library only today, and CI has a job that proves it); the worker processes must not import pluggy at start-up, since their import time is what the C worker was built to avoid; and the hook should let a backend report itself unavailable with a reason, as the demo's import guards do now.

### Found: demo with all thirteen featured backends (2026-11-24, my own check)

After building every native piece in the main checkout (`make -C cworker`, `make -C cworker wasm`, `npm ci --prefix nodeworker`, `npm ci --prefix pglite-worker`, `make -C duckdb-cpp-worker`, `make -C duckdb-rs-worker`), the demo lists 13 featured databases, all available. Checked over plain HTTP for each one: a count query returns 200 rows' worth of books, the table page loads, and the `.csv` export returns 201 lines (header plus 200 rows). A 500-way fan-out across the fleet: 500 of 500 succeeded in 0.99 s over 175 databases with connections at the cap of 40. Getting there took six separate build commands; a single top-level build target is worth adding. The demo server had died about an hour earlier with a termination signal, cause unknown; possibly a subagent's process clean-up.

### Found: eight subagents were stopped before reporting (2026-11-24)

At a status check at 07:33 no subagent was running. Their transcripts stopped at 05:02 (the fleet and bug-fix subagent) and at about 06:52 (the other seven), and the harness refuses to resume them. Both times are close to moments when the session was interrupted from the user's side, so that is the likely cause; I do not know it for certain. Lesson for the process: an interrupt of the main session can end every background subagent, and nothing announces it, so check for silent subagents after any interruption.

State of each at the stop, from what is pushed (their own commit messages; no final reports, so no verified results or measurements):

| Work | Pushed | Missing |
| --- | --- | --- |
| Fleet C workers and three worker bug fixes | Fleet C-worker members (verified by me in the demo) | All three bug fixes: DuckDB stream lost interrupt, `duckdb-worker` unbounded query, `sqlite-worker` hang after cancel |
| SQLite WASM under wasmtime | Backend, host VFS for locking, locking and fault tests, CI build, demo entry (works in the demo) | Report: locking design and cost, whether evading queries are stopped, measurements |
| SQLite WASM in Node.js | Backend, tests, fault tests, demo entry (works in the demo) | Report: locking, cancel limits, measurements |
| Multi-tenant C worker | Backend, tests, sanitizer CI, benchmark columns, demo entry (works in the demo) | Report: scaling curve, blast radius, sanitizer results |
| DuckDB worker in Rust | Worker, parent backend, demo entry (works in the demo) | Unknown how much of conformance, benchmarks and CI; report and the Rust against C++ comparison |
| Native export and socket handoff | `db.export()` with fallback, format definitions, C worker native export, socket handoff, PostgreSQL COPY export, DuckDB export, demo downloads (CSV works on all 13 demo databases), benchmark script, parity table | Benchmark results, report, recommendation |
| PGlite in Node worker | Backend, conformance registration, kill and scale tests, CI job | Demo entry, benchmark, report |
| ClickHouse | Backend, "whole conformance suite passes", tests | Demo entry, benchmark, capability table, report |

Uncommitted files were left in three worktrees (bug fixes, PGlite, ClickHouse).

### Decision: relaunch the eight stopped pieces of work (2026-11-24, about 07:45)

With Simon's go-ahead, eight fresh subagents were started, one per stopped piece, each briefed with what is already pushed, the leftover uncommitted files to recover, and what is still owed. Two changes to how they are briefed, because of the stop: each must push after every commit, and each must send an interim summary as soon as it has test counts and first measurements, so a second stop loses less.

### Found: the Node.js SQLite-WASM worker can corrupt a database shared with another process (interim, 2026-11-24)

Interim report from the relaunched subagent; a fix is in progress.

- **The official `@sqlite.org/sqlite-wasm` build under Node has no cross-process locking.** Node has no `fcntl`, so the worker's locks live in a SharedArrayBuffer table and are invisible to other processes. A Node-side write next to a live native writer rolls back the other writer's "hot" journal and corrupts the file. Until fixed, a `sqlite-nodeworker` database must not be opened by anything else at the same time. The demo's Node database is opened only by that worker.
- Fix being built: the Node main thread holds a native `node:sqlite` `BEGIN EXCLUSIVE` on the file, so other processes get "database is locked". That makes the database single-process while awake, which is a real restriction compared with the native workers.
- No WAL mode. After SIGKILL mid-transaction, native SQLite recovers and `integrity_check` is ok.
- Design: one Node process per database, each connection on a `worker_thread` with its own WASM instance. Cancel and time limits use a progress handler reading an `Atomics` flag; a statement that never reaches a progress check is stopped by terminating its thread about 100 ms after the limit, which retires the process.
- Tests on macOS: 531 passed, 18 skipped for this backend, nothing failed.
- Provisional numbers at load 46 to 48: open to first result 75 ms (Python worker 77 ms); idle footprint 37 MB (Python worker 24 MB); one-row read 248 µs against 156 µs for the C worker. One-row update 19 to 20 ms against under 1 ms for the others, because libuv's fsync on macOS is `F_FULLFSYNC`, a full drive flush on every commit.
- So far it is no better than the Python worker on start-up or memory, and worse on writes and on locking.
- CI: one run's three Linux jobs appeared hung at about twice their normal time; cause unknown.

### Found: SQLite in WebAssembly under wasmtime, interim (2026-11-24)

Interim report from the relaunched subagent; measurements at load about 18, provisional; full table still to come.

- **The hypothesis holds on containment.** With a 200 ms limit and 50 ms grace, `glob`, `instr` and `like` on long strings and `randomblob(1e8)` over 30 rows, the queries nothing in-process could stop, were all stopped by a WebAssembly trap at 251 to 255 ms. No process was killed. The instance is replaced and the next request gets a fresh one in under 1 ms. `printf` with a width of 1e9 and `randomblob(1e9)` hit the memory cap as `SQLITE_NOMEM` in 1 to 2 ms. 300 random-point cancels all returned, worst case 1.2 ms.
- **Locking design:** the module has no WASI filesystem. Every VFS call (open, read, write, sync, lock, unlock) is a host import into Python, and Python applies SQLite's own unix locking logic as real `fcntl` locks on the same byte ranges native SQLite uses. So other processes and native SQLite see and respect the locks. An allow-list restricts each instance to its own database file, its journal and anonymous temporary files, so ATTACH and VACUUM INTO fail.
- **No WAL:** `journal_mode=WAL` stays "delete", and opening a WAL-mode file gives `SQLITE_CANTOPEN`. That rules this backend out for any database another tool keeps in WAL mode.
- **A real locking bug was found and fixed** (`643b7f6`): opening a new instance opened and closed an extra descriptor, and closing any descriptor drops every classic POSIX lock the process holds on that file. That silently dropped native SQLite's EXCLUSIVE lock held elsewhere in the same process, on Linux as well as macOS. Fixed with open-file-description locks (macOS has them at private fcntl numbers 90 and 92), finding already-open files by `stat`, and parking descriptors that cannot be closed safely.
- **Cost of the host VFS:** about 1.5 µs per page read. A full scan of 4,010 pages took 6.7 ms through the host against 2.4 ms for native SQLite with a tiny cache. A one-row read makes 6 host calls.
- Tests: 479 passed, 7 skipped for this backend.

### Upstream suggestions

- **sqlite-utils and Datasette, general hazard worth a documentation note:** in one process, opening and closing a second file descriptor on a SQLite database file releases the POSIX locks SQLite itself holds on it. Any code that opens the database file directly (to hash it, copy it, check its header or size) while a connection is open in the same process can silently drop that connection's locks. SQLite's documentation describes this; the subagent reproduced it here. Worth checking Datasette's own code for direct opens of database files (for example hashing for `inspect`, or file copies in publish paths) while connections are live. Not checked against Datasette.

### Found: PGlite in a Node worker, interim (2026-11-24)

Interim report from the relaunched subagent; numbers at load 14 to 80, provisional.

- PGlite 0.5.17 embeds PostgreSQL 18.7. Tests: 474 passed, 15 skipped for this backend.
- **A real PostgreSQL per database works, but each awake one costs about 115 MB.** The WebAssembly module declares 128 MB of initial memory and that cannot be lowered. Twenty awake databases are about 2.3 GB. An empty database is 40 MB on disk, 16 MB of it one WAL segment. This also explains the 940 MB of worker memory seen in the demo after PGlite was added.
- **Start-up:** a new database takes 700 to 960 ms from initdb, or about 200 ms by copying a template directory (38 MB in 971 files). Asleep to first result is about 143 ms: Node boot 31 ms, spawn to hello 47 ms, PGlite open 79 ms, first query 9 ms.
- **Running cost is fine:** one-row read 0.17 ms, small transaction 0.55 ms, sleep 17 ms.
- **Three bugs found and fixed** (`cdb12ba`), the first two in the core:
  - With `concurrent_sessions=false`, a task holding a transaction that then read through `db` waited for itself forever. The core now raises `NestedWriteError` at once for that task; other tasks still wait.
  - A time-limit kill failed every other connection's queued request with `WorkerCrashed`, though those requests had never reached the worker. They are now retried once on the new process.
  - Python 3.15 logged "exception in shielded future" on each cancel-kill.
- Nothing short of killing the process can stop a running query, and a kill takes every session on that database with it.

### Idea

- **PGlite suits a handful of databases, not a fleet.** At about 115 MB each awake, the registry's cap would have to count a PGlite database as dozens of places. This is the strongest case yet for weighted places in the cap.

### Found: DuckDB worker in Rust, interim; the recursive-CTE cause (2026-11-24)

Interim report from the relaunched subagent; numbers at load 13 to 61, provisional.

- **Correction to "DuckDB worker in C++": the slow first rows are explained.** The cause is DuckDB's per-connection `streaming_buffer_size`. With the 10 MB default, a streaming execute through the C API waits until the buffer is about half full before yielding rows. Measured on one Rust build: first rows in 13 ms with `streaming_buffer_size=16KB`, 4,823 ms with the default. It can only be set per connection, before `lock_configuration`. The same setting should remove the C++ worker's three expected failures; the Rust subagent has been asked to apply it there and to measure what a small buffer costs on large reads. The bug-fix subagent has been told, since the same buffering may explain the one CI failure of the Python DuckDB worker.
- **Conformance: 429 passed, 12 skipped, no expected failures** (C++: 426, 12, 3).
- **Rust and C++ are level on every resource measure:** process floor 2.2 against 2.1 MB; one database open 14.0 against 14.1 MB; spawn to hello 12.6 against 12.1 ms; asleep to first result 27.8 against 28.3 ms; extra per typed row 0.74 against 0.63 µs. The cost is DuckDB, not the language.
- **The fuzz test found a real problem:** map decoding was quadratic in the number of keys (a 100,000-key map took seconds). Fixed.
- `unsafe` is denied outside the FFI module, which has 105 unsafe blocks.
- Native `insert_rows` through the appender: 10,000 rows in 269 ms against 1,643 ms on the default path.
- Clippy is not installed and was not run, since installing it would change the Rust toolchain.

### Found: ClickHouse, interim; two security holes closed (2026-11-24)

Interim report from the relaunched subagent; measurements still to come.

- **Form: chDB 4.5.1, which embeds ClickHouse 26.11, in-process.** No server and no binary. chDB allows one embedded server per process: one data directory, with one ClickHouse database per dblayer `Database`; it stops when the last connection closes.
- **chDB has no wheels for Python 3.15,** so the extra is `chdb>=4.5; python_version < '3.15'` and the backend is unavailable on 3.15. The demo now runs under Python 3.14 for that reason.
- **Two real security holes on the read path, found and fixed** (`557f09f`):
  - `select file('/etc/hosts')` read host files from a read connection. ClickHouse's `readonly=1` does not block the scalar `file()` function.
  - A comment or a backslash-escaped quoted name could hide `INTO OUTFILE` from dblayer's statement scanner, so an "untrusted read" wrote a file on the host.
  - Host table functions, engines, dictionaries, ATTACH, BACKUP and RESTORE are now refused unless `allow_host_access` is set.
- The stopped subagent's "whole conformance suite passes" was not true of the code it left: its Arrow `execute_many` sent the wrong column names, every value landed as the column default, and 48 conformance tests failed. Fixed. Now 485 passed, 38 skipped.

### Found: demo with fifteen featured backends (my own check)

Restarted under Python 3.14 with ClickHouse added. All 15 featured databases answer a count query, serve a table page and return a 201-line CSV export. A 500-way fan-out across the fleet succeeded in 1.1 s with connections at the cap of 40.

### Idea

- **Denying host access by scanning SQL text is fragile.** The ClickHouse hole came from a scanner being fooled by comment and quoting syntax. Where an engine offers a real setting or sandbox, use it; where it does not (chDB's `file()`), a worker process with no filesystem access beyond its data directory is the dependable control. The same reasoning applies to DuckDB's external access setting and SQLite's authorizer.

### Found: native export experiment, interim (2026-11-24)

Interim report from the relaunched subagent. Load 19 to 26 during the run; provisional. The 1,000,000-row run is still in progress.

CSV export of a wide 100,000-row table; CPU is the Python server process's own CPU time:

| Mode | Python CPU | Wall | Max event-loop lag |
| --- | --- | --- | --- |
| C worker, Python fallback | 0.223 s | 0.39 s | 4.7 ms |
| C worker, Datasette-style | 0.216 s | not reported | 5.7 ms |
| C worker, native, raw frames | 0.027 s | 0.18 s | 3.0 ms |
| C worker, native, socket handoff | 0.015 s | 0.25 s | 8 ms |
| DuckDB in-process, fallback | 0.349 s | not reported | not reported |
| DuckDB in-process, native | 0.091 s | 0.08 s | not reported |
| PostgreSQL (psycopg), fallback | 0.341 s | not reported | not reported |
| PostgreSQL (psycopg), native | 0.097 s | not reported | not reported |

- **Backend-side serialisation cuts the Python process's CPU by about 8 times on the C worker** (0.223 s to 0.027 s), and socket handoff by about 15 times (to 0.015 s). On in-process DuckDB and on PostgreSQL the gain is 3.5 to 4 times. DuckDB's figure includes the engine's own threads, since it runs in the same process.
- **Socket handoff saves little over raw frames** at this size (0.015 s against 0.027 s of Python CPU) and its wall time was slower. Given its preconditions, raw frames may be the practical answer.
- **Datasette-style JSON is the expensive case.** Building the whole response with one `json.dumps` cost 120 to 160 MB of memory and 80 to 235 ms of event-loop lag at 100,000 rows. Every chunked mode stayed under about 10 ms of lag.
- **PostgreSQL native export does not win on narrow tables** (0.092 s against 0.090 s): psycopg's `Copy.read()` hands over one row per await, so Python still does per-row work.
- **Parity, verified by 65 new tests:** native output is byte-identical to the Python fallback on the C worker, DuckDB and psycopg for CSV, TSV, every JSON shape and NDJSON, across NaN and infinities, a 5 MiB value, the empty result, NULL against the empty string, 64-bit extremes, and control characters. Dates and timestamps on DuckDB and PostgreSQL are equal only after parsing (`.5` and `-08` where Python writes `.500000` and `-08:00`). Intervals are unspecified.
- **A real bug fixed in the core** (`0eeec98`): the core's time limit and the backend's both fired for the same budget, and on the DuckDB worker the second cancel left a read transaction open ("cannot start a transaction within a transaction"). The core's limit now fires 100 ms after the backend's, and a connection it had to cancel is discarded.
- **The psycopg time-limit failures seen earlier are real, not load:** with zero grace, "another command is already in progress" in 2 of 10 runs.
- The C worker's export and handoff paths are clean under ASan and UBSan.

### Upstream suggestions

- **Datasette: stream JSON responses in chunks.** Serialising a whole JSON response with a single `json.dumps` cost 120 to 160 MB and blocked the event loop for 80 to 235 ms at 100,000 rows here; chunked serialisation of the same data stayed under 10 ms of lag. Measured in dblayer's benchmark with a "Datasette-style" mode written by the subagent, not by running Datasette itself, and Datasette normally caps rows at 1,000 per page, so this bears mainly on `?_shape=`/`_size=max`, on plugins, and on any future streaming JSON export.
- **Datasette: the CSV streaming path could delegate to the database** where one exists that can serialise (PostgreSQL `COPY`, DuckDB `COPY`, a worker process), for a 4 to 8 times cut in server CPU. A feature idea for the backend interface, not a bug.

### Found: `streaming_buffer_size` fix applied to the C++ DuckDB worker (2026-11-24)

Subagent-reported, load about 24.

- With `streaming_buffer_size='16KB'` set per connection, the C++ worker's first export chunk for an unbounded recursive CTE went from 6,770 ms to 5.3 ms. Its three expected failures are removed (`71a6b56`); that backend now shows 486 passed, 13 skipped, none expected to fail.
- The small buffer costs little: 100,000-row reads were within noise of the default, at most about 10% slower on plain rows in some runs.
- **It does not explain the Python DuckDB worker's CI failure.** Through the `duckdb` Python package the first rows arrive in 0.3 to 0.4 ms with either buffer size; only the C API's streaming execute waits for the buffer. That failure is still unexplained. The double time-limit bug fixed by the export subagent is the next candidate.
- The `LIMIT max_rows+1` wrap is kept as an optimisation.

### Found: multi-tenant C worker (`sqlite-cpool`), interim (2026-11-24)

Interim report from the relaunched subagent. Load 17 to 30; provisional, and single-client figures swung 3 to 10 times between runs.

- **Throughput scales with concurrency where the others do not.** Requests per second at 1, 4, 16 and 64 concurrent clients on different databases:

| Backend | 1 | 4 | 16 | 64 | Processes at 64 |
| --- | --- | --- | --- | --- | --- |
| Multi-tenant C worker | about 8,000 | 25,000 | 48,000 | 54,000 | 2 |
| Single-tenant C worker | about 8,000 | 28,000 | 48,000 | 23,000 | 64 |
| In-process SQLite | about 7,000 | 10,000 | 8,000 | 10,000 | 1 |

  At 64 clients the parent process is using 0.95 of a CPU, so the Python event loop is now the limit, not the worker. In-process SQLite is flat at about 10,000 because of the thread hop and the GIL.
- **Memory per open database is 188 KB,** against about 3 MB for a single-tenant C worker process. Warm open to first result is 0.49 ms.
- **One-row read:** 101 µs, level with the single-tenant C worker (105 µs); apsw inline is 10.6 µs.
- **Blast radius, measured.** One tenant's uninterruptible query was killed 451 ms after a 200 ms limit plus 250 ms grace. Four in-flight reads on other tenants were retried and finished 13 ms after the kill (normal median 7.9 ms). Of 1,948 writes, one failed loudly with `WorkerCrashed`; none was lost or duplicated. The open transaction got `WorkerCrashed` and left nothing behind. Idle connections reconnected in 0.9 to 6 ms. The culprit is then quarantined onto its own process, and a second offence killed only that process.
- **Sanitizers:** ASan and UBSan clean over 1,484 tests. TSan clean over 486 tests, with a positive control (5 reports without the suppressions file, all a known benign SQLite race in `unixOpen`), so detection works.
- Tests: 431 passed, 10 skipped on conformance.
- Added: a `sqlite-cpool` fleet kind in the demo (100 of the 1,000), and a fix for a worker process that outlived its event loop at demo shutdown.

### Found: CI is wedged and the GitHub API limit was exhausted (process)

- Run 45413464925 has been "in progress" since 14:25 UTC: the Linux runners got a shutdown signal during `tests/scale/test_threads.py`, and every later push has queued behind it and been cancelled. So no CI result exists for anything pushed since. The TSan step has never run in CI.
- Subagents polling `gh` used up the account's 5,000 requests per hour, which is shared with all of Simon's other work. All eight have been told to stop calling `gh` except once at the end. Future briefs must say this from the start.

## 2026-11-24: SQLite in WebAssembly under wasmtime, final report

Subagent-reported. Timings at load 17 to 47; provisional. CI not verified for these commits. The interim section above covers the locking design and the bug it fixed; this adds the measurements and the recommendation.

### Found

- **Every query a progress handler cannot stop was stopped about 50 ms after its limit, with no process and no kill.** `glob`, `instr` and `like` on long strings and `randomblob(1e8)` over 30 rows trapped at 251 to 255 ms against a 200 ms limit plus 50 ms grace; a fresh instance is ready in under 1 ms. `randomblob(1e9)` and a `printf` of width 1e9 hit the memory cap in 1 to 2 ms and the same instance carried on.
- **The memory cap is exact per instance,** but the memory is this process's memory: the cap bounds it, it does not hide it. Instances that grew are recycled.
- **Cost against native SQLite:**

| Statement | Native | Wasm, epoch interruption |
| --- | --- | --- |
| 1M-row recursive CTE | 70 ms | 150 ms |
| 200,000-row sort | 36 ms | 84 ms |
| `instr` on long strings | 146 ms | 115 ms |

  Roughly 1.5 to 2 times native on CPU-bound SQL (faster on `instr`, thanks to SIMD `memcmp`), about 1.5 µs per page read through the host VFS, and about 900 KB per open instance.
- **With an inline read mode added by the subagent, a one-row read is about 25 µs,** against 10 µs for apsw inline, 90 µs for in-process through the executor, 130 µs for the multi-tenant C worker and 184 µs for the C worker in the same run. New database to first result: about 0.6 ms.
- **Epoch interruption, not fuel:** fuel cannot be revoked from another thread, so it cannot implement cancellation.
- Compiling the module takes 131 to 187 ms once; the compiled form is cached on disk and loads in about 10 ms. Importing wasmtime takes about 120 ms.
- Scale test at 1,000 databases (load 42): 1,435 requests/s, 1,003 B per asleep database.
- **Verified safe against other writers on the same file:** another process, the C worker, the `sqlite3` shell, and native SQLite on a thread of the same process. A negative control with locks turned off tore 599 of 1,031 reads, so the tests do detect the problem.
- **A stale `.wasm` or C worker binary fails conformance after `worker.c` changes.** The build outputs need a staleness check or a single build target.
- Tests: 493 passed, 10 skipped for this backend. The full suite run showed 2 failures and 1 error that passed on rerun; their names were not captured.

### Decision

- **Recommendation from the subagent: make this the default for untrusted read-only SQL in-process,** and keep native in-process SQLite for trusted, WAL-mode or hot write paths. It is the only in-process option that stops single-C-call queries and caps memory exactly, on every platform.
- Caveats that go with that: no WAL; ATTACH and VACUUM INTO refused; a build step; and, on macOS, safety alongside native SQLite in the same process depends on open-file-description locks that are a private kernel interface there.

### Idea: to discuss

- **A three-tier picture is emerging for SQLite:** apsw or standard-library inline reads for trusted speed (about 10 µs); WebAssembly in-process for untrusted reads (about 25 µs inline, containment without processes); the multi-tenant C worker for parallel throughput and for writes that must be killable.
- Make `inline_reads` the default for WebAssembly read connections.
- One instance hosting several small databases, to cut the 900 KB per connection.
- Drive the epoch from a C-level ticker in place of a Python callback every 2 ms.

### Upstream suggestions

- **Datasette issue 1270:** a WebAssembly sandbox with epoch interruption stops the single-C-call queries that neither the progress handler nor `interrupt()` can. Verified here on SQLite 3.53.1, outside Datasette. Worth mentioning on that issue as a third option beside a worker process.

## 2026-11-24: native export experiment, final report

Subagent-reported. Every benchmark ran at load 18 to 36; provisional. No CI result for these commits. Results are committed in `benchmarks/export-results.md`.

### Built

- `db.export(sql, params, format="csv"|"tsv"|"json"|"ndjson", json_shape=..., max_rows, max_bytes, time_limit_ms, native=True)` returning an `ExportStream`: an async iterator of bytes that also carries the columns, row count, truncation flag and whether the backend serialised natively.
- Optional backend capability `SupportsExport`. A backend yields row bytes only; the core writes the header row, the JSON envelope and the truncation tail. A Python fallback covers every backend.
- Native implementations: the C SQLite worker (`cworker/src/export.c`), in-process DuckDB (each line built in SQL) and PostgreSQL on psycopg (`COPY (projection) TO STDOUT`).
- Socket handoff for the C worker (`dblayer/backends/cworker_handoff.py`) with its own small HTTP/1.1 server in `benchmarks/handoff_server.py`.
- `docs/export-formats.md` defines the formats and the parity contract.
- Full suite: 6,270 passed, 3,272 skipped (backends not built in that worktree), 4 expected failures.

### Found

Python server CPU for exporting 1,000,000 wide rows:

| Mode | CSV | JSON |
| --- | --- | --- |
| C worker, serialised in Python | 2.03 s | 1.61 s |
| C worker, native, raw frames | 0.15 s | 0.17 s |
| C worker, native, socket handoff | 0.06 s | 0.05 s |
| DuckDB in-process, serialised in Python | 2.77 s | 2.67 s |
| DuckDB in-process, native | 0.69 s | 0.66 s |
| PostgreSQL, serialised in Python | 3.30 s | 2.64 s |
| PostgreSQL, native | 1.18 s | 1.36 s |

- **Backend-side serialisation cuts Python CPU about 13 times on the C worker,** 4 times on DuckDB (whose figure includes the engine's own threads) and 2 to 3 times on PostgreSQL for wide rows. Over HTTP through the demo app the C worker's server CPU fell from 2.19 s to 0.24 s for a 174 MB CSV.
- **PostgreSQL native export does not help on narrow rows** (0.80 s against 0.84 s) because psycopg delivers one COPY row per await. A prototype that drains libpq directly through the public `psycopg.pq` module cut that to 0.17 s (`scripts/psycopg_copy_batch.py`); not yet used by the backend.
- **Socket handoff works end to end with curl** and takes the Python server's CPU for the body to about zero, flat at roughly 50 ms per export. Keep-alive survives it, and a worker killed mid-body gives the client a broken transfer (curl exit 18), not a short one that looks complete.
- **Socket handoff's preconditions are heavy:** plain HTTP/1.1 on a socket the server owns; no TLS in Python; no HTTP/2 or 3; no compression or body-rewriting middleware. ASGI offers no way to reach the socket, so it needs a non-ASGI server or a new ASGI extension. Two weak points: after a kill the body may end on a whole CSV row, with only the missing final chunk marking it broken; and while exporting, the worker holds the socket and could read the client's next pipelined request, including its auth headers, which undermines the worker as a containment boundary.
- **Whole-response JSON is the costly pattern.** One `json.dumps` of 1,000,000 rows blocked the event loop for 0.7 to 1.3 s and grew the process by 1.2 to 1.4 GB. Every chunked mode, native or not, kept loop lag under 10 ms and memory growth under 7 MB.
- **Parity:** byte-identical to the Python fallback on all three native backends for integers (including 64-bit extremes), floats (including NaN and infinities), text with every awkward character, blobs, NULL against the empty string and booleans. Dates and timestamps on DuckDB and PostgreSQL are equal only after parsing. Intervals and nested types are unspecified.
- **The side-channel pipe transport was worse than raw frames** (3 to 4 times the Python CPU) and should be dropped.
- **Truncation cannot be signalled in-band for CSV, TSV or NDJSON.** Only two JSON shapes carry a `"truncated": true` marker. An HTTP trailer is the natural place.
- In-process DuckDB cannot use the handoff as tried: `COPY ... TO '/dev/fd/N'` fails.

### Decision

- **Recommendation from the subagent: add backend-side serialisation to the real interface** as an optional capability that yields row bytes only. Contract on parity: byte-identical to the format definition for integers, floats, text, blobs, NULL and booleans, proven by running the parity suite against the backend; parse-equal for dates, timestamps, decimals and UUIDs; nested types unspecified.
- **Do not pursue socket handoff for Datasette now.** Raw frames already give most of the gain; handoff saves roughly another 0.1 s per million rows at the cost of the preconditions above.
- Engines with their own output formats (ClickHouse) should be asked for row-only output and must pass the parity suite before claiming a format; as shipped their writers differ in line endings, empty-string quoting, blobs, `nan`/`inf` and TSV escaping.

### Idea: to discuss

- Batch the psycopg COPY read inside the backend.
- HTTP trailers to signal truncation for CSV, TSV and NDJSON.
- An ASGI extension, `http.response.handoff`, where the server flushes and lends the app the socket; worth raising with the ASGI maintainers only if handoff is revisited.
- Drop the pipe transport.

### Upstream suggestions

- **Datasette CSV: NULL and the empty string are written identically.** `csv.writer` output cannot tell them apart. Quoting empty strings, as PostgreSQL's CSV does, leaves an unquoted empty field to mean NULL. Verified by the subagent reading Datasette's CSV rendering code; behaviour not run.
- **Datasette JSON: NaN would be emitted as bare `NaN`, which is invalid JSON.** `remove_infinites` handles infinities only. SQLite stores NaN as NULL, so this bites only with other backends or plugins. Verified by reading the code.
- **Datasette JSON: stream rows instead of one `json.dumps`** if the row limit is ever raised. Measured in this project's harness, not in Datasette.
- **psycopg (not Datasette or sqlite-utils):** a batched `Copy.read`. The prototype measured a 4 times gain.

## 2026-11-24: ClickHouse backend, final report

Subagent-reported. Measurements at load 19 to 25; provisional. CI not confirmed. The full suite was not rerun after the last fix. The interim section above covers the two security holes.

### Built

- `dblayer/backends/clickhouse.py` on chDB 4.5.1 (ClickHouse 26.11 embedded in-process), optional extra `clickhouse`, Python 3.12 to 3.14 only. Registered as `clickhouse`: 485 passed, 38 skipped, every skip by a capability flag.
- New capability flags, all defaulting to true so no other backend changed: `transactions`, `unique_constraints`, `foreign_keys`, `distinct_blob_type`, `duplicate_column_names`.
- `core/emulated_keys.py`: primary-key checks done by dblayer, since the engine enforces none.
- A featured demo database, a benchmark (`benchmarks/clickhouse.py`) and a README section. Four commits, `557f09f` to `b5f035d`.

### Found

| Measure | ClickHouse | DuckDB | SQLite |
| --- | --- | --- | --- |
| Group by, 10M rows | 48 ms | 24 ms | 3,165 ms |
| Join and group by, 10M rows | 82 ms | 64 ms | 2,962 ms |
| Point read, median | 4.0 ms | 0.9 to 1.5 ms | 0.6 to 0.8 ms |
| Insert 1M rows | 6.4 to 8.1 s | 1.07 s | 1.35 s |
| Disk, 10M rows | 156 MB | 107 MB | 238 MB |

- **The interface survived an engine with no transactions,** but only by redefining what the calls mean. `db.transaction()` just holds the write slot; every statement applies as it runs; rollback keeps the writes and logs a warning; `batch()` is not atomic. Staged replace is still atomic (`EXCHANGE TABLES`). Upsert is a delete followed by an insert, so a reader can see the gap and a failure in between loses rows.
- **Key checks are what make ClickHouse inserts slow** (6 to 8 s per million rows), and they are exact only against other dblayer writers; a plain SQL INSERT can add duplicates.
- **ClickHouse is 4 times slower than SQLite on a point read and about 60 times faster on a 10M-row aggregate.** DuckDB was faster than ClickHouse on every analytical query here, on 2 threads.
- **Databases in one process are not isolated from each other.** chDB allows one embedded server and one data directory per process, so every dblayer database is a ClickHouse database inside it. A read-only connection can read every other database, and `system.processes` shows other connections' SQL text.
- **Sleep does not return memory:** 329 MB resident after a 1M-row insert and aggregate, and still 329 MB after sleep. `import chdb` alone takes 1.36 s and adds about 140 MB.
- **`max_memory_usage` is per query,** not per database or process.
- **Cancellation is by `KILL QUERY`, repeated until the statement returns.** A single long step only notices when it finishes: `select sleep(2)` with a 100 ms limit returned after 1.35 s.
- **Engine-native export is parse-equal, not byte-identical:** ClickHouse quotes every CSV string and writes `2` for `2.0`. At 1M rows it is 0.12 s against 1.02 s through Python.
- Dialect traps: ClickHouse's `Date` covers only 1970 to 2149; an unmatched LEFT JOIN side is 0, not NULL, unless `join_use_nulls=1`; joined columns are named `b.id`.
- A task cancelled while its connection was opening leaked the connection, so the shared server never stopped. Fixed.
- A remaining, unverified gap: an escaped function name inside `INSERT ... VALUES` data on a write connection is not seen by the second parser check.

### Decision

- Host access is denied by an allowlist of safe table functions and engines, kept in step with the installed ClickHouse by a test that compares it with `system.table_functions` and `system.table_engines`, so a new version's additions fail a test.
- Engine-native export stays opt-in, because it does not meet byte parity.

### Idea: what the interface would need to make ClickHouse first-class

- **A declared atomicity and isolation level per operation,** replacing the single `transactions` flag: staged replace is atomic here, upsert is not.
- **Statement checks by the engine's own parser,** never a shared SQL scanner.
- **A "shared server" capability** saying databases in one process are not isolated from each other.
- **An "analytical engine" hint** so callers avoid pages built from many point reads.
- **A native batch-insert hook.**

### Upstream suggestions

- None credible for sqlite-utils or Datasette. The general lesson (filtering SQL needs the engine's own lexer) applies to any regex-based read-only check; not tested against Datasette.
- **chDB (not Datasette or sqlite-utils):** `file()` is not stopped by `readonly=1` and is not confined by `user_files_path`. Reproduced on 4.5.1.

### Built: a proposed revision of the API (2026-11-24)

`dblayer-api-revision-proposal.md`, written by me at Simon's request from the findings in these notes. It proposes four contract changes (errors end a transaction, with explicit savepoints; atomicity declared per operation; containment as a declared level with `untrusted=True`; no security by scanning SQL text), a set of smaller API additions, changes to the backend interface (interrupt contract, operations as data, capabilities in `hello`, execution strategy), registry changes (weighted places, bounded wakes) and nine decisions for Simon. Nothing in it is implemented, and it rests on the subagents' reports, not yet on a line-by-line code review.
