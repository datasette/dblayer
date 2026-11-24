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
