# Proposed revision of the dblayer API

2026-11-24. A proposal for discussion, written after building the prototype in `dblayer-prototype-spec.md` and thirteen backends beyond it. Nothing here is implemented.

## Summary

The core idea of the spec held: one `Database` object, no raw connections, three API levels. Seventeen backends pass one conformance suite through it, from in-process SQLite to ClickHouse and a PostgreSQL compiled to WebAssembly. The surface callers see needs little change.

What needs revising is the contract underneath that surface. Each new backend forced a new capability flag or a quiet redefinition of an existing promise. This document proposes four contract changes that alter what callers can rely on, then smaller additions, changes to the backend interface, and changes to the registry.

### Evidence and its limits

- The proposals rest on the subagents' reports and test results recorded in `notes.md`. The code has not yet had a line-by-line review.
- Every timing quoted was taken on a loaded machine and is provisional. The final benchmark has not run.
- The names in the sketches are suggestions.

## What to keep

| Design choice | Why it held |
| --- | --- |
| `Database` as the only handle; no raw connections | No backend needed an escape hatch. The one exception, the connection init hook, is what stopped Python functions crossing into C and WebAssembly workers. |
| Three levels: SQL, transactions, named operations | Enough to build a Datasette-like app on every backend. |
| Capability flags over silent differences | The suite skips by flag, so every difference is written down. |
| Core owns pooling, write ordering, deadlines, caching | Written once; fifteen backends reused it. |
| An asleep database is one small object | About 500 bytes each; 10,000 registered with bounded threads and descriptors. |
| Optional backend protocols (`SupportsOperations`, `SupportsBatch`, `SupportsExport`, `SupportsServerDeadline`) | Let fast paths exist without widening the required interface. |
| A wire protocol for worker processes | Implemented in Python, C, C++, Rust and JavaScript. |

## Contract changes that need a decision

These four change what a caller may assume. They should be settled before anything else.

### 1. One rule for errors inside a transaction

**Problem.** After a statement fails inside `async with db.transaction()`, backends behave four ways:

| Backend | After a failed statement |
| --- | --- |
| SQLite | The transaction carries on. |
| PostgreSQL | Aborted, unless each statement is wrapped in a savepoint. asyncpg pays an extra round trip for that; psycopg sends it in one pipeline. |
| DuckDB | Aborted. No savepoints exist. A later COMMIT silently rolls back. |
| ClickHouse | There is no transaction. Earlier statements stay applied. |

Today this is the flag `statement_errors_abort_transaction` plus a per-backend savepoint option.

**Proposal.** The portable contract is: any error ends the transaction. The next call on `tx` raises `TransactionClosed`, and the block rolls back. SQLite is made stricter to match. Callers who want to recover ask for it:

```python
async with db.transaction() as tx:
    try:
        async with tx.savepoint():
            await tx.execute("insert into t values (:id)", {"id": 1})
    except IntegrityError:
        ...  # the transaction is still usable
```

`tx.savepoint()` raises `NotSupported` where the engine has none.

**Cost.** Code that catches an error mid-transaction and carries on must add a savepoint. The implicit per-statement savepoint on PostgreSQL goes away, which removes its overhead.

### 2. Atomicity declared per operation, not one `transactions` flag

**Problem.** ClickHouse passes the suite with `transactions=False`, but the flag hides detail: `db.transaction()` there only holds the write slot, rollback keeps the writes, upsert is delete-then-insert, yet staged replace is still atomic. PGlite has real transactions but one session (`concurrent_sessions=False`). Read snapshots vary (`snapshot_reads`).

**Proposal.** Replace the transaction-related booleans with one structured description:

```python
db.capabilities.transactions
# Transactions(atomic=True, savepoints=True, snapshot_reads=True,
#              concurrent_writers=False, sessions="many")

db.capabilities.atomic("upsert_rows")   # True / False per named operation
```

`db.transaction()` raises `NotSupported` when `atomic` is false, unless the caller passes `db.transaction(atomic=False)` to say that holding the write slot is all they need. A rollback that cannot roll back becomes an error, not a logged warning.

**Cost.** ClickHouse callers must opt in to non-atomic blocks. That is the point.

### 3. Containment as a declared level

**Problem.** A time limit means different things:

| Backend | How a statement is stopped | What else it costs |
| --- | --- | --- |
| In-process SQLite, apsw | Progress handler or interrupt | Cannot stop some queries at all (one long C function call). |
| SQLite in WebAssembly | Trap about 50 ms after the limit | That connection's instance is replaced. |
| Worker process, one database | Process killed after a grace period | That database's open transaction. |
| Multi-tenant C worker | Process killed | Every database on the process; reads are retried, open transactions fail. |
| DuckDB, PostgreSQL | Interrupt or server cancel | The open transaction is aborted. |
| PGlite | Process killed; nothing else works | Every session on that database. |
| ClickHouse | `KILL QUERY`, noticed between steps | A long single step runs on. |

Today this is two booleans, `hard_kill` and `memory_limit`, which say none of it.

**Proposal.** Each backend declares a containment description, and callers can require a level:

```python
db.capabilities.containment
# Containment(stops="always" | "cooperative",
#             mechanism="interrupt" | "trap" | "kill",
#             blast_radius="statement" | "connection" | "database" | "process",
#             memory_cap="exact" | "process" | "per_query" | "none",
#             filesystem="confined" | "open")

result = await db.execute(sql, params, untrusted=True)
```

`untrusted=True` raises `NotSupported` unless the backend stops statements unconditionally, caps memory, and confines file access. A deployment then chooses a backend per trust level without reading its source.

**Cost.** One more keyword on the read path. In return, "is this safe for arbitrary SQL" becomes a question the API answers.

### 4. No read-only enforcement by scanning SQL text

**Problem.** On ClickHouse a comment and an escaped identifier hid `INTO OUTFILE` from dblayer's statement scanner, so an "untrusted read" wrote a file on the host. `sqlite3_stmt_readonly` reports `ATTACH` and `PRAGMA x = 1` as read-only. DuckDB needed statement classification plus a read-only transaction.

**Proposal.** A backend may claim `filesystem="confined"` or read-only enforcement only through a mechanism the engine or the operating system enforces: an engine setting, an authorizer, a sandbox, or a process with no access beyond its data directory. The shared scanner stays for placeholder rewriting and for counting statements. It is never the security boundary.

**Cost.** Some backends lose the claim until they earn it (chDB has no setting that stops `file()`).

## Smaller additions to the caller's API

| Change | Reason |
| --- | --- |
| Remove `last_pk`; writes return keys through `RETURNING`, and named operations report the keys they wrote | It is SQLite's rowid, wrong for other keys, `None` after an upsert, and had a rowid-reuse bug. |
| `Column.portable_type` beside the native type | Result column types are `None`, `int8` or `BIGINT` for the same data. |
| A documented coercion rule for parameters, with `params_as_text=True` for values from URLs | `where n >= :n` with `"5"` returns different rows on SQLite and PostgreSQL. Booleans travel as 1/0. |
| New exceptions: `ConnectionFailed`, `NotADatabase`, `ReadOnlyViolation`, `HostAccessDenied` | Callers currently match on message text or get a raw OS error. |
| `db.count(table, *, time_limit_ms, estimate=False)` | Every listing page needs it; engines have cheap estimates. |
| `db.page(table, *, after, size)`: keyset paging on the primary key | The demo fell back to OFFSET. |
| `create_view` as a named operation | The demo had to use raw `execute_write`. |
| Truncation reported out of band for exports (`ExportStream.truncated`, and an HTTP trailer in servers) | CSV, TSV and NDJSON cannot say they were cut off. |
| `Results.as_arrow()` where the backend can supply it | For DuckDB and ClickHouse, per-row decoding in Python is the main cost. |

`db.export()` and the `SupportsExport` capability are already built and should stay: native export cut the Python server's CPU about 13 times on the C worker at a million rows. Socket handoff should not be adopted; its preconditions are too heavy for the gain.

## Changes to the backend interface

### State the interrupt contract

`interrupt()` is a no-op when no statement is running, on SQLite and on DuckDB. A cancel that arrives just before a statement starts was lost in four separate places. The protocol should say: after `interrupt()`, the connection must not start a new statement for the cancelled call, and the caller repeats the interrupt until the call returns. A cancel flag read by the progress handler satisfies this by design.

### Named operations as data

**Problem.** Operations are Python functions. The Python worker runs one as a single message; the C, C++, Rust, WebAssembly and Node workers cannot, and fall back to several round trips.

**Proposal.** An operation compiles in the core to a small plan: an ordered list of statements with parameters, plus a few conditional steps ("if column missing, run this ALTER"). Any worker can run a plan in one message. The `op` message then carries a plan, not a function name.

**Cost.** The rebuild path of `alter_table` reads the schema and decides in between; it needs either a richer plan or two messages.

### Worker `hello` carries capabilities

Capabilities are derived today from the parent's SQLite version, not the worker's. `hello` should carry the engine version and the capability description, so a worker with a different SQLite cannot be misdescribed.

### Execution strategy belongs to the backend

Inline reads (run on the event loop thread under an opcode budget, continue on a thread if it runs over) took a one-row read from about 61 µs to about 10 µs on apsw, and a raw test suggests the standard `sqlite3` module can do the same. Nothing in the protocol prevents this. The default SQLite backend should do it, and the `UsesExecutor` protocol should be described as one strategy among several.

### Backend registration through pluggy

Already requested. Each backend registers through a hook and may report itself unavailable with a reason. Worker processes must not import pluggy at start-up.

## Registry and lifecycle

| Change | Reason |
| --- | --- |
| Weighted places: a backend declares what an awake database costs | One cap counts a 188 KB multi-tenant SQLite database and a 115 MB PGlite database the same. |
| `max_concurrent_wakes`, or take the place before `on_wake` runs | The cap bounds connections, not wake work. One fan-out built 150 cold databases at once. |
| A hook context that permits setup writes during first wake | `db.execute_write` inside `on_wake` deadlocks on the wake lock. |
| `on_sleep(db, reason="idle" \| "cap" \| "explicit")` | Hooks cannot tell why a database slept. |
| Counts by state in `RegistryStats`; memory by footprint, not RSS | A status page reads 1,000 properties; RSS overstated retained memory on macOS. |
| A distinct `sleeping` state, or an awaitable for the end of a sleep | Tests reach into a private attribute to wait for it. |
| A pool health check | A connection whose worker died while idle is found only on next use. |

### The larger question: where the registry lives

Each Python process has its own registry, cap and write queue. Two server processes cannot share one SQLite write slot or one connection cap. The multi-tenant C worker already reaches about 54,000 requests per second with the Python parent as the bottleneck. A long-lived database daemon, shared by every server process, would own the registry once, and could batch commits from many callers into one disk sync.

This is a different architecture, not a revision. It is listed here so that the smaller changes above do not close it off: nothing in the caller's API assumes the registry is in-process, and it should stay that way.

## What this proposal does not change

- The method names and shapes of `execute`, `stream`, `execute_write`, `batch`, `transaction`, the named operations and the introspection methods.
- Placeholder rewriting from `:name` and `?`.
- The sleep and wake model and the Cold state.
- The wire protocol's framing and codecs.

## Decisions needed

1. Adopt "any error ends the transaction" with explicit `tx.savepoint()`?
2. Replace the transaction flags with a structured description, and make non-atomic transactions opt-in?
3. Add containment levels and `untrusted=True`?
4. Rule out SQL scanning as a security boundary?
5. Remove `last_pk`?
6. Named operations as data: worth the redesign, or accept round trips on native workers?
7. Which SQLite backend is the default: the standard module with inline reads, or apsw?
8. PostgreSQL driver: psycopg 3, as the comparison recommends?
9. Is a shared database daemon in scope for the next round?

## Suggested order

1. Decisions 1 to 4, since they change what callers can rely on.
2. The small additions, starting with exceptions, `last_pk` and parameter coercion.
3. The interrupt contract and `hello` capabilities, which are cheap and remove known bugs.
4. Registry weighting and wake bounding.
5. Operations as data, then columnar results.
6. The daemon, as its own project.
