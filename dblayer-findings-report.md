# dblayer prototype: findings report

2026-11-24. The closing report for the prototype specified in `dblayer-prototype-spec.md`. The working log is `notes.md`; proposed changes to the API are in `dblayer-api-revision-proposal.md`.

## Summary

The spec's central idea works. A single asyncio `Database` object that never hands out a raw connection now fronts 16 backends, and one conformance suite of about 10,000 tests runs against all of them. A Datasette-like demo app was built on the public API alone and serves 1,000 databases through 40 connections.

Four conclusions matter most for Datasette:

1. **The caller-facing API should be kept almost as specified.** What needs revising is the contract underneath it: what happens after an error in a transaction, what a time limit can actually stop, and how read-only is enforced.
2. **Running SQLite outside the Python interpreter pays twice.** It contains queries that nothing in-process can stop, and on Linux it is faster: a C worker in another process reads one row in 119 µs against 220 µs in-process.
3. **The thread hop is the largest avoidable cost on the default path.** Running short reads inline on the event loop took a one-row read from 220 µs to 44 µs.
4. **Idle databases are nearly free, as hoped.** An asleep database costs about 500 bytes; 10,000 can be registered with constant threads and bounded file descriptors.

### How far to trust this report

- **Performance figures** come from one full benchmark run on a dedicated Linux machine (Fly.io `performance-8x`: 8 dedicated AMD EPYC CPUs, 15.6 GB, ext4, load under 1), at commit `5d8e474`. It is a single run with no error bars. Earlier figures taken on a loaded Mac are superseded and should not be quoted.
- **Test results** are as reported by the subagents that built each part, mostly on macOS. The last continuous-integration run that completed passed 10 of 15 jobs; fixes for the other five were pushed, and the CI repair was stopped before a fully green run was seen. **There is no green CI run for the final code.**
- **The code has not had a line-by-line review.** Opus subagents wrote it; the coordinator checked it through tests, reports and the demo.
- A partial run of the full suite on the Linux benchmark machine reached 56% with 5,704 passed and none failed before it was stopped.

## What was built

| Area | Contents |
| --- | --- |
| Core | `Database`, `Registry`, connection pool with idle timeout and a global cap, transactions with deadlines, `batch`, named operations, staged import, schema cache, placeholder rewriter, `db.export()` |
| SQLite backends | In-process on the standard library; in-process on apsw (with inline reads); worker process in Python; worker in C; multi-tenant worker in C; multi-tenant worker in Rust; WebAssembly in-process under wasmtime; WebAssembly in a Node.js process |
| DuckDB backends | In-process; worker in Python; worker in C++; worker in Rust |
| PostgreSQL backends | asyncpg; psycopg 3; PGlite (PostgreSQL compiled to WebAssembly, one per database, no server) |
| Other | ClickHouse, embedded through chDB |
| Tests | About 10,270 collected: conformance suite parametrised by backend, fault injection, scale tests, per-backend tests |
| Tools | Wire protocol reference (`docs/worker-protocol.md`), export format definition (`docs/export-formats.md`), demo app (`dblayer_demo/`), benchmark runner (`benchmarks/run_all.py`), repeatable Fly.io rig (`fly/`) |

Size: about 23,500 lines of Python in the library, 20,700 in tests, 6,300 of C, 4,500 of C++, 12,200 of Rust and 6,000 of JavaScript, in 447 commits.

Four of the sixteen backends were in the spec. The rest were added during the work to answer questions the first results raised.

## Answers to the spec's questions

### Can interactive transactions work across a process boundary without stalling other writers?

**Yes.** The worker enforces each transaction's deadline itself. With the parent's event loop deliberately blocked, the worker still rolled back at its deadline and another process could write at once. When a statement ignores interrupts at the deadline, the worker is killed and the caller gets `TransactionExpired`. The cost of crossing the boundary is round trips: an interactive insert-with-alter took about 2.9 ms against 2.2 ms as one named operation (macOS figure).

### Is the worker process fast enough to be the default for untrusted SQL?

**It depends entirely on which worker.**

| Worker | Wake (asleep to first result) | Warm one-row read | Memory |
| --- | --- | --- | --- |
| Python worker | 130 ms | 0.48 ms | about 24 to 38 MB per process |
| C worker | 1.75 ms | 0.14 ms | about 1.6 to 3 MB per process |
| Multi-tenant C worker | 2.15 ms | 0.18 ms | 171 KB per open database |
| Multi-tenant Rust worker | 2.65 ms | 0.19 ms | about 186 KB per open database |
| In-process SQLite, for comparison | 0.42 ms | 0.21 ms | about 165 KB per connection |

The spec's hypothesis, "well under a millisecond added to a small query", holds for a running worker and fails for starting a Python one. A native worker removes the start-up cost and is faster than in-process on warm queries, because in-process pays a hop to a thread pool on every call.

### Does a hard kill of a worker ever corrupt a database or leave a lock behind?

**No, with one real problem found and fixed.** Workers were killed mid-query and mid-transaction, in rollback-journal and WAL mode; every database passed `PRAGMA integrity_check` and an independent writer could proceed at once. The problem: a writer killed mid-transaction in rollback-journal mode leaves a hot journal that read-only (`mode=ro`) connections cannot roll back, so every read fails until something opens the file read-write. The backend now recovers. Datasette opens mutable databases for reading with `mode=ro` and is likely exposed to the same failure.

### How many idle databases can one process register, and what does each cost?

**At least 10,000, at about 500 bytes each**, against a 2 KB target. Registering is constant time per database. Under a skewed load across them, thread count stayed constant and descriptors stayed within the cap. In the final run asleep databases measured 463 to 1,492 bytes depending on backend.

### Which operations cannot be expressed portably?

More than the spec expected. Each is now a capability flag; the measured matrix is below.

- **Errors inside a transaction.** SQLite carries on, PostgreSQL aborts unless each statement has a savepoint, DuckDB aborts and has no savepoints, ClickHouse has no transactions.
- **"SQL passes through unchanged" is not enough.** An integer parameter fails on asyncpg without casts; a string parameter from a URL compares differently on SQLite than on PostgreSQL and DuckDB; booleans travel as 1 and 0.
- **`ALTER TABLE`.** DuckDB refuses almost every form when the table has an index. SQLite needs a table rebuild for most changes, and the rebuild used by sqlite-utils loses triggers and CHECK constraints.
- **Rowids, `RETURNING`, uniqueness, foreign keys.** Absent on one or more engines. On ClickHouse, keys are emulated by dblayer and are exact only against other dblayer writers.
- **Stopping a query.** Varies from a cooperative interrupt to killing a process shared with other databases.

### Can schema change detection be made cheap on every backend?

**On every embedded backend, yes; on a PostgreSQL server, no.** File size and modification time serve as a hint that never opens a connection. PostgreSQL has no cheap equivalent: `schema_version()` costs about 0.6 ms, so changes by other clients are seen after a time limit or an explicit refresh. PGlite, being files on disk, gets the cheap hint.

## The spec's goals

| Goal | Outcome |
| --- | --- |
| The three API levels cover what Datasette does with `execute_fn` and `execute_write_fn` | Met for core use. Gaps: custom SQL functions on native workers, `ATTACH`, table counts, label columns, FTS detection. See the migration map. |
| One conformance suite passes on all backends, differences declared as flags | Met locally for 16 backends. Not confirmed by a green CI run. |
| Thousands of registered databases at near-zero idle cost | Met. |
| SQLite in a worker with a memory limit and a kill | Met. The memory limit is enforced by the OS on Linux only. |
| Measure what the process boundary costs | Met, on a quiet Linux machine. |

## Capability matrix, as measured

Read from each backend's declared capabilities in the code, which the conformance suite holds it to. Measured on macOS; on Linux the SQLite worker processes also get an OS-enforced memory limit.

| Backend | Concurrent writers | Atomic transactions | An error aborts the transaction | Snapshot reads | Unconditional stop | Memory limit | Cheap change hint | CREATE statement available | Implicit rowid | Keys enforced by the engine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLite in-process (standard library, apsw) | No | Yes | No | Yes | No | No (apsw: process-wide, opt-in) | Yes | Yes | Yes | Yes |
| SQLite workers (Python, C, multi-tenant C, multi-tenant Rust) | No | Yes | No | Yes | Yes, by kill | Linux only | Yes | Yes | Yes | Yes |
| SQLite in WebAssembly (wasmtime, Node.js) | No | Yes | No | Yes | Yes, by trap or thread stop | Yes, exact | Yes | Yes | Yes | Yes |
| DuckDB in-process | No | Yes | Yes | Yes | No | Yes, engine setting | Yes | Yes | No | Yes |
| DuckDB workers (Python, C++, Rust) | No | Yes | Yes | Yes | Yes, by kill | Yes | Yes | Yes | No | Yes |
| PostgreSQL server (asyncpg, psycopg 3) | Yes | Yes | No, with a savepoint per statement | Yes | No (server cancel) | No | No | No | No | Yes |
| PGlite | No | Yes | No | Yes | Yes, by kill only | Yes | Yes | No | No | Yes |
| ClickHouse (chDB) | No | **No** | n/a | No | No | Per query | Yes | Yes | No | **No** |

Other measured differences: only SQLite has full-text search through this interface; PGlite has one session, so other connections wait behind an open transaction; ClickHouse cannot tell text from bytes, has no `RETURNING`, and its DDL is not transactional.

## Performance

All from the full run on the Linux machine. Each table is one session; compare within a table, not across tables.

### One-row read and update, medians

| Backend | Read | Update |
| --- | --- | --- |
| In-process SQLite | 220 µs | 1,583 µs |
| apsw | 246 µs | 1,478 µs |
| apsw with inline reads | 44 µs | 1,382 µs |
| C worker | 119 µs | 1,016 µs |
| Multi-tenant C worker | 131 µs | 1,094 µs |
| Python worker | 466 µs | 1,690 µs |
| Node.js worker | 444 µs | 1,812 µs |
| PostgreSQL, asyncpg / psycopg 3 | 139 / 151 µs | 140 / 175 µs (in a transaction) |
| PGlite | 420 µs | not in this table |
| DuckDB / ClickHouse point read | 2.4 / 7.0 ms | not measured |

### Wake cost and warm query cost, 1,000 databases

| Backend | Wake | Warm query |
| --- | --- | --- |
| In-process SQLite | 0.42 ms | 0.21 ms |
| SQLite in WebAssembly | 1.52 ms | 0.34 ms |
| C worker | 1.75 ms | 0.14 ms |
| Multi-tenant C worker | 2.15 ms | 0.18 ms |
| Multi-tenant Rust worker | 2.65 ms | 0.19 ms |
| DuckDB in-process | 22.9 ms | 1.27 ms |
| DuckDB Rust worker | 51.7 ms | 1.37 ms |
| DuckDB C++ worker | 55.8 ms | 1.39 ms |
| Python SQLite worker | 130 ms | 0.48 ms |
| Node.js SQLite worker | 174 ms | 1.03 ms |
| Python DuckDB worker | 230 ms | 1.71 ms |
| PGlite | 422 ms | not measured (runner bug) |

### Concurrency without Python in the way

Requests per second from a native load generator, with worker CPU per request in brackets.

| Clients | Multi-tenant C worker | Multi-tenant Rust worker |
| --- | --- | --- |
| 1 | 24,700 (29 µs) | 24,800 (30 µs) |
| 4 | 58,000 (44 µs) | 84,000 (33 µs) |
| 16 | 65,000 (70 µs) | 104,000 (50 µs) |
| 64 | 35,000 (93 µs) | 113,000 (55 µs) |

Both workers share a design with one global lock taken several times per request. The C worker collapses at 64 clients while using only 3.2 cores. Driven through Python, the event loop is the limit: about 3,500 requests per second in-process and 5,000 to 11,000 on the native workers.

### Memory

| Backend | Per awake database |
| --- | --- |
| Multi-tenant C worker | 171 KB |
| Node.js SQLite worker | 332 MB |
| PGlite | 504 MB (1.93 GB for 20) |
| DuckDB in-process, 150 databases | 476 MB peak in total; 143 MB after sleep |
| DuckDB workers, 40 processes | 2.85 GB (C++), 2.9 GB (Rust), 3.7 GB (Python) |
| ClickHouse | 264 MB after import, not returned on sleep |

In-process DuckDB gives back about 70% of its memory on sleep on Linux.

### Backend-side serialisation

Server CPU to export one million wide rows as CSV:

| Backend | Serialised in Python | Serialised by the backend |
| --- | --- | --- |
| C SQLite worker | 7.15 s | 0.47 s (0.17 s with socket handoff) |
| DuckDB in-process | 11.0 s | 2.6 s |
| PostgreSQL (psycopg 3) | 9.1 s | 2.7 s |

Native output was byte-identical to the Python output for numbers, text, blobs, NULLs and booleans on all three, and equal after parsing for dates and timestamps.

## Verdict on each backend

| Backend | Verdict |
| --- | --- |
| In-process SQLite, standard library | Keep as the default. Add inline reads; the measured gain on apsw was five times. |
| In-process SQLite on apsw | Optional. Adds a pinned SQLite, typed result columns, a heap limit and an "untrusted" lockdown. |
| Python SQLite worker | Keep only for Python features (named operations in one message, custom SQL functions). Too slow to start and too heavy for a fleet. |
| C SQLite worker | The practical containment option today. |
| Multi-tenant C or Rust worker | The right shape for many lightly used databases. Rust is ahead on Linux under concurrency and prevents three of the six bugs the native workers have had; it aborted under a memory limit on Linux, with a fix pushed and unconfirmed. Fix the shared global lock in both before choosing. |
| SQLite in WebAssembly under wasmtime | The best in-process option for untrusted reads: every unstoppable query was stopped about 50 ms after its limit, with an exact memory cap and no process. No WAL; 1.5 to 2 times native on CPU-bound SQL (macOS figure). |
| SQLite in WebAssembly under Node.js | Drop. It pays for both a process and a sandbox and needed an exclusive file lock to be safe. Its value was as a third protocol implementation. |
| DuckDB in-process | Keep as the default DuckDB form. |
| DuckDB native worker (Rust or C++) | Use where a kill or a memory limit is required. The two are equal on every measure; Rust is preferred for maintenance. |
| Python DuckDB worker | Drop in favour of a native one. |
| PostgreSQL on psycopg 3 | Prefer over asyncpg: public API only, no stale statement cache, bounded result memory, cheap savepoints. Speed is level. |
| PostgreSQL on asyncpg | Works, but leans on three private driver APIs. |
| PGlite | A real PostgreSQL per database with no server, for a handful of databases. About 500 MB each awake and no way to cancel a query short of a kill. |
| ClickHouse | Proof that the interface stretches. Not a first-class backend without declared atomicity per operation; databases in one process are not isolated from each other. |

## Bugs and hazards the work uncovered

These are the findings most likely to matter beyond this prototype.

- **Queries no in-process mechanism can stop.** Single long C function calls (`instr`, `LIKE` and `GLOB` on very long strings, `randomblob(1e9)`, a `printf` of width 1e9) evade both the progress handler and `interrupt()`, for one to five seconds each. Only a process kill or a WebAssembly trap stops them.
- **Lost interrupts.** Both SQLite and DuckDB drop an interrupt that arrives before the statement has started. This was found in four separate places; in-process streams could not be cancelled at all until it was fixed.
- **Closing any file descriptor on a database file drops the process's POSIX locks on it.** Two backends hit this independently. Any code that opens a database file directly while a connection is live in the same process is at risk.
- **Scanning SQL text is not a security boundary.** On ClickHouse a comment and an escaped identifier hid `INTO OUTFILE` from the statement scanner, and an "untrusted read" wrote a file on the host. `select file('/etc/hosts')` also worked, because the engine's read-only setting does not cover that function. Both are closed; the lesson is general.
- **An engine's host-reaching functions differ by platform.** The Linux build of chDB had table functions the Mac build lacks; CI on Linux caught them.
- **Silent rollback on COMMIT.** DuckDB and PostgreSQL both report success when committing an aborted transaction while rolling it back. The backends now raise.
- **Both DuckDB and the C workers had a "cancel lost just after a stream's last frame" race.**
- **Rust's standard library aborts when it cannot allocate a thread's signal stack,** which under a memory limit took down a multi-tenant worker.

## Recommended API for Datasette

Keep the surface. `execute`, `stream`, `execute_write`, `batch`, `transaction`, the named operations, the introspection methods, `sleep` and `close` all held up across sixteen backends, and so did `db.export()` once it was added.

Change the contract in four places, set out in `dblayer-api-revision-proposal.md`:

1. **Any error ends a transaction**, with an explicit `tx.savepoint()` for recovery.
2. **Atomicity is declared per operation**, and a non-atomic transaction is opt-in.
3. **Containment is a declared level**: how a statement is stopped, what else a stop takes down, how memory is capped, whether file access is confined. `execute(..., untrusted=True)` refuses where the backend cannot guarantee those.
4. **Read-only and host-access claims come only from mechanisms the engine or OS enforces.**

Then, in rough order of value for Datasette:

- **Inline reads on the default SQLite backend.**
- **`db.export()` with backend-side serialisation** as an optional capability. Do not adopt socket handoff: it needs plain HTTP/1.1, no TLS in Python and a server outside ASGI, for a small further gain.
- **Remove `last_pk`**; return keys through `RETURNING`.
- **A parameter coercion rule** for values that arrive as strings.
- **Weighted places in the registry's cap, and a bound on concurrent wakes.**
- **Named operations expressed as data**, so native workers can run one in a single message.

A deployment would then pick a backend per trust level: inline in-process SQLite for trusted speed, WebAssembly in-process for untrusted reads, a native multi-tenant worker for throughput and for writes that must be killable.

The larger open question is whether the registry should live in each Python process at all. A shared daemon would give several server processes one write queue and one cap. Nothing in the caller's API prevents it.

## Migration map for Datasette plugins

Datasette's current `Database` methods, and where each lands.

| Datasette today | dblayer | Notes |
| --- | --- | --- |
| `execute(sql, params)` | `db.execute(sql, params, time_limit_ms=, max_rows=)` | `Results` has `columns`, `rows`, `truncated`. Datasette's `Results.first()`, `single_value()` and `dicts()` have no counterpart yet. |
| `execute_fn(fn)` for introspection | `table_names`, `view_names`, `table_columns`, `primary_keys`, `foreign_keys`, `indexes`, `definition`, `schema` | One round trip for `schema()`. |
| `execute_fn(fn)` for several consistent reads | `async with db.transaction(mode="read") as tx:` | One snapshot. |
| `execute_fn(fn)` that needs the raw connection | **No equivalent.** | The prototype's rule is that no raw connection leaves a backend. See the list below. |
| `execute_write(sql, params)` | `db.execute_write(sql, params)` | Returns `WriteResult`. |
| `execute_write_script(sql)` | `db.batch([...])` | A list of statements; no multi-statement strings. |
| `execute_write_many(sql, params_seq)` | `tx.execute_many(...)` or `db.insert_rows(...)` | |
| `execute_write_fn(fn)` | `async with db.transaction() as tx:` or a named operation | The function body becomes awaited calls on `tx`. A deadline (5 s by default) applies. |
| `execute_write_fn(fn)` wrapping sqlite-utils | `create_table`, `insert_rows(alter=True)`, `upsert_rows`, `alter_table`, `create_index`, `staged_import` | Covers the common sqlite-utils calls. |
| `execute_isolated_fn(fn)`, `analyze_sql` | No equivalent | Proposed as a named operation behind a capability flag. |
| `table_exists`, `view_exists` | `name in await db.table_names()` | Served from the schema cache. |
| `table_column_details` | `table_columns` | |
| `foreign_keys_for_table`, `get_all_foreign_keys` | `foreign_keys(table)`, `schema()` | |
| `get_table_definition`, `get_view_definition` | `definition(name)` | `NotSupported` on PostgreSQL. |
| `hidden_table_names` | `TableInfo.hidden` in `schema()` | |
| `table_counts(limit)` | Not provided | `db.count(table, time_limit_ms=)` is proposed. |
| `fts_table`, `label_column_for_table` | Not provided | Datasette-level conventions; would sit above this layer. |
| `hash`, `size`, `mtime_ns` | `change_hint()` for change detection | No content hash. |
| `attached_databases`, cross-database queries | Not modelled | `ATTACH` is refused on the WebAssembly backend and in untrusted modes. |
| `close()` | `close()`; new `sleep()` | Sleep is reversible and releases everything. |
| `prepare_connection` hook | Connection init hook | In-process: a callable. Python worker: importable `module:function` names. Native and WebAssembly workers: **not possible**; the C worker can load an extension by path. |
| `write_wrapper`, `track_event` hooks | Out of scope | The transaction object is the natural attachment point. |
| `sqlite_timelimit` | `time_limit_ms` on every call | |

### What plugins would lose, or must change

- **Custom SQL functions written in Python** work in-process and in the Python worker only. This is the main obstacle to making a native worker the default.
- **SpatiaLite and other loadable extensions** work in-process through the init hook and in the C worker by path. Not in WebAssembly.
- **Anything that holds a connection across awaits**, or passes a connection to another library such as sqlite-utils, must be rewritten against `tx` or the named operations.
- **Code that catches an error inside a write function and carries on** will need an explicit savepoint if the revised contract is adopted.
- **Long write functions** meet the transaction deadline. Large imports should use batched `insert_rows` or `staged_import`.

## Suggestions for other projects

Collected in `notes.md` under each milestone, with how well each was verified. The ones with the strongest evidence:

**Datasette**

- Reads through `mode=ro` fail with `SQLITE_READONLY_ROLLBACK` after a writer is killed mid-transaction in rollback-journal mode. Reproduced on SQLite 3.53.1, not through Datasette.
- Issue 1270: `interrupt()` fixes one class of query the progress handler misses, but single long C calls evade both; a retry loop must also expect `sqlite3_prepare` to return `SQLITE_INTERRUPT`; a cancel flag read by the progress handler closes the lost-interrupt window.
- CSV output writes NULL and the empty string identically. Found by reading Datasette's code.
- JSON output would emit bare `NaN` on a non-SQLite backend, which is invalid JSON. Found by reading the code.
- A single `json.dumps` of a large response blocks the event loop (0.7 to 1.3 s at a million rows in this harness) and should be streamed if row limits are raised.
- An inline fast path for cheap queries, and an authorizer-based lockdown for arbitrary SQL.

**sqlite-utils**

- `transform()` does not recreate triggers and silently loses CHECK constraints, `STRICT` and `WITHOUT ROWID`. Subagent-reported; no minimal reproduction against sqlite-utils yet.
- Its foreign-key deferral misbehaves inside a transaction with `foreign_keys=ON`.
- Comparing `last_insert_rowid()` before and after an insert gets a reused rowid wrong.

**Others:** DuckDB (silent rollback on COMMIT; dropped interrupts; C API streaming waits for half the streaming buffer), psycopg (pipelines disarm the idle-transaction timeout; no batched COPY read), chDB (`file()` not stopped by read-only), PGlite, Node.js.

## What was not done

- **A green CI run for the final code.** The last completed run passed 10 of 15 jobs. Fixes for the rest were pushed and the repair was stopped before confirmation.
- **A line-by-line code review.**
- **Error bars.** One benchmark run; no repeat.
- **PGlite in the wake table and scale test**, lost to two bugs in the benchmark runner, since fixed and not re-run.
- **The global lock in the multi-tenant workers** is identified, not fixed.
- **The Rust worker's abort under a memory limit** has a fix pushed, not confirmed on Linux.
- **Savepoints as a public API, weighted places, bounded wakes, operations as data, columnar results:** proposed, not built.
- **Backend registration through one hook:** dropped by decision.
- **Object storage:** a local directory stood in for it, as the spec allowed.

## Decisions waiting

The nine in `dblayer-api-revision-proposal.md`, plus three this report adds:

1. Which SQLite forms to carry forward. The recommendation is three: inline in-process, WebAssembly in-process, and one native multi-tenant worker.
2. C or Rust for that native worker, after the lock fix.
3. Whether to drop the Node.js SQLite worker and the Python DuckDB worker.
