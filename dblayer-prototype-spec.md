# Spec: async database layer research prototype

Nov 23, 2026 · @Simon Willison

## Summary

This prototype is a standalone asyncio Python library that tests one idea: Datasette can talk to every database through an interface that never hands out a raw connection. It has no dependency on Datasette. The working package name in this spec is `dblayer`.

The library offers three levels of API over four backends:

- **SQL execution:** run a read or a single write and get rows back.
- **Interactive transactions:** an async object for read-then-decide-then-write logic, kept short by a deadline.
- **Named operations:** create table, insert rows, upsert, alter and staged bulk import, similar in spirit to sqlite-utils.

The four backends are in-process SQLite, SQLite in a separate worker process, PostgreSQL and DuckDB.

### Goals

1. Show that the three API levels cover what Datasette does today with `execute_fn` and `execute_write_fn`.
2. Show that one conformance test suite can pass on all four backends, with differences declared as capability flags.
3. Hold thousands of registered databases at near-zero cost while they are idle: no threads, no file handles, no processes.
4. Run SQLite in a worker process that can be given a memory limit and killed when a query overruns.
5. Measure what the process boundary costs, per query and per row.

### Non-goals

- Integration with Datasette, its plugin hooks, permissions or HTTP layer.
- Translating SQL between dialects. SQL strings pass through unchanged; only parameter placeholders are rewritten.
- An ORM, a query builder or a migrations system.
- Object storage. The lifecycle section defines hooks for it and the tests use a local directory as a stand-in.
- Operating-system sandboxing of the worker beyond resource limits.

### Questions the prototype must answer

| Question | Answered by |
| --- | --- |
| Can interactive transactions work across a process boundary without stalling other writers? | Transaction deadline and fault-injection tests |
| Is the worker process fast enough to be the default for untrusted SQL? | Overhead benchmarks |
| Does a hard kill of a worker ever corrupt a database or leave a lock behind? | Fault-injection tests |
| How many idle databases can one process register, and what does each cost? | 10,000-database test |
| Which operations cannot be expressed portably? | Capability flags and skipped conformance tests |
| Can schema change detection be made cheap on every backend? | Schema fingerprint tests |

## Architecture

The library has three layers, and only the top one is visible to callers.

&#91;embedded content: architecture · public API, shared core, four backends\]

Callers use the `Database` object. The core supplies everything that is the same for every engine. Each backend supplies ten connection methods and a dialect. The worker backend is a proxy: its worker process runs the in-process backend against the file.

### Package layout

```
dblayer/
    __init__.py          Database, Registry, Results, exceptions
    core/
        pool.py          connection pool, idle timeout, global cap
        transactions.py  Transaction object, write queue, deadlines
        operations.py    default implementation of each named operation
        params.py        placeholder tokenizer and rewriter
        schema.py        Schema types and the schema cache
    backends/
        base.py          Backend and BackendConnection protocols, Dialect
        sqlite.py        in-process SQLite
        sqlite_worker.py parent side of the worker backend
        postgres.py
        duckdb.py
    worker/
        __main__.py      the worker process entry point
        protocol.py      framing, codecs, message types
tests/
    conformance/         one suite, parametrised by backend
    faults/              kill, cancel and limit tests
    scale/               the 10,000-database test
benchmarks/
```

### Design rules

- **No raw connections.** Nothing outside a backend module touches a driver connection.
- **Async at the surface.** Every public method is a coroutine. Blocking drivers run on the shared thread pool.
- **Core owns policy.** Pooling, write ordering, deadlines and caching are written once, in the core.
- **Backends declare differences.** A behaviour that varies is a capability flag, not a silent difference.

## Core API

A `Database` object is the only thing callers hold. Creating one opens nothing: no connection, thread or process exists until the first call.

```python
from dblayer import Database
from dblayer.backends import SqliteInProcess, SqliteWorker, Postgres, DuckDB

db = Database(SqliteInProcess("data.db"), name="data")

result = await db.execute(
    "select id, title from posts where author = :author",
    {"author": "simon"},
    time_limit_ms=1000,
    max_rows=1000,
)
result.columns      # ("id", "title")
result.rows         # list of Row objects: tuple access and name access
result.truncated    # True if more than max_rows were available

await db.execute_write(
    "update posts set title = :t where id = :id", {"t": "New", "id": 1}
)
```

### Methods on `Database`

| Method | Purpose |
| --- | --- |
| `execute(sql, params, *, time_limit_ms, max_rows)` | One read. Returns `Results`. |
| `stream(sql, params, *, time_limit_ms, chunk_rows)` | Async iterator of row chunks, with backpressure. |
| `execute_write(sql, params, *, returning_limit)` | One write in its own transaction. Returns `WriteResult`. |
| `transaction(mode, deadline_ms)` | Interactive transaction. See Transactions. |
| `batch(statements, *, expect_schema_version)` | A whole write transaction sent in one message. |
| Named operations | `create_table`, `insert_rows` and others. See High-level operations. |
| Introspection | `table_names`, `table_columns`, `schema` and others. See Schema. |
| `sleep()` / `close()` | Release resources but stay usable / shut down for good. |
| `capabilities` | Flags describing what this backend supports. |

### Parameters

Named parameters in `:name` style are the canonical form, passed as a dict. Positional `?` with a sequence is also accepted. Each backend rewrites placeholders to its native style.

The rewriter is a small tokenizer, not a regular expression. It must skip string literals, quoted identifiers, comments and Postgres `::type` casts. It gets its own test file, because a mistake here corrupts queries silently.

### Values

Every backend accepts and returns these portable types: `None`, `int`, `float`, `str`, `bytes` and `bool`. Backends may also return native types such as `datetime`, `Decimal` or `UUID`. Whether to coerce those to portable types by default is an open question.

### Results

`Results` holds `columns`, `rows` and `truncated`. Each column carries a name and, where the backend knows it, a declared type. `WriteResult` holds `rowcount`, `last_pk` and any rows from a `RETURNING` clause, capped by `returning_limit`.

### Limits

| Limit | Applies to | Enforced by |
| --- | --- | --- |
| `time_limit_ms` | Every read and write statement | Backend-specific; see each backend |
| `max_rows` | Reads | Fetch one extra row and set `truncated` |
| `max_result_bytes` | Reads | Stop fetching and raise `ResourceLimitExceeded` |
| `memory_limit_mb` | Worker and DuckDB backends | Process limit or engine setting |
| `deadline_ms` | Transactions | Rollback when the transaction has been open too long |

### Errors

All backends raise the same exception classes. The original driver exception is kept as `__cause__`.

| Exception | Meaning |
| --- | --- |
| `QueryError` | The SQL failed. Carries the backend's message and error code. |
| `IntegrityError` | A constraint was violated. Subclass of `QueryError`. |
| `QueryInterrupted` | A statement passed its time limit. |
| `ResourceLimitExceeded` | A memory or result-size limit was hit. |
| `TransactionExpired` | The transaction passed its deadline and was rolled back. |
| `WriteConflict` | The backend aborted the transaction because of a concurrent write. |
| `NestedWriteError` | A write was attempted on `db` from inside an open transaction on the same database. |
| `WorkerCrashed` | The worker process died or was killed. |
| `NotSupported` | The backend lacks the capability this call needs. |
| `DatabaseClosed` | The database was closed for good. |
| SchemaChanged | A batch was sent with expect\_schema\_version and the schema had moved on. Nothing ran. |

### Mapping from Datasette today

| Datasette today | Prototype |
| --- | --- |
| `db.execute()` | `db.execute()` |
| `db.execute_fn()` for introspection | Named introspection methods |
| `db.execute_fn()` for anything else | `db.transaction(mode="read")` |
| `db.execute_write()` | `db.execute_write()` |
| `db.execute_write_many()` | `tx.execute_many()` or `db.insert_rows()` |
| `db.execute_write_script()` | `db.batch()` |
| `db.execute_write_fn()` | `db.transaction()` or a named operation |
| `db.execute_isolated_fn()` | No direct equivalent. `analyze_sql` becomes a named operation with a capability flag. |
| `prepare_connection` plugin hook | A connection init hook. See the worker backend and open questions. |

## Transactions

The interactive transaction is the main write API, and `batch()` is a shorthand that sends a whole transaction in one message. Short transactions are the documented contract, backed by a deadline.

```python
async with db.transaction() as tx:
    columns = await tx.table_columns("posts")
    if "slug" not in columns:
        await tx.add_column("posts", "slug", "text")
    await tx.execute_many(
        "insert into posts (id, title, slug) values (:id, :title, :slug)",
        rows,
    )
```

### Semantics

- **Commit and rollback.** A clean exit commits. Any exception rolls back, including `asyncio.CancelledError`.
- **What `tx` offers.** `execute`, `execute_many`, every named operation and every introspection method. This is what makes read-then-decide-then-write possible.
- **Modes.** `mode="write"` is the default. `mode="read"` gives several reads one consistent snapshot and never takes the write lock.
- **One caller at a time.** Calls on one `tx` are serialised. Using `tx` after the block exits raises an error.
- **No nesting.** Opening a transaction inside a transaction on the same database raises `NestedWriteError`. Savepoints are a stretch goal.
- **No deadlock by accident.** Calling `db.execute_write()` inside an open write transaction on the same `db` would wait forever on the write queue. A context variable detects this and raises `NestedWriteError` instead.

### Deadlines

A write transaction on SQLite or DuckDB holds the database's only write slot until it ends. Two safeguards keep a slow or broken caller from blocking other writers.

| Safeguard | Default | Behaviour |
| --- | --- | --- |
| `deadline_ms` | 5,000 | Total time the transaction may stay open. On expiry the backend rolls back. The next call on `tx` raises `TransactionExpired`. |
| Rollback on disconnect | Always on | If the calling task is cancelled, or the client side of a worker connection goes away, the backend rolls back at once. |

The deadline is a backstop for bugs. Well-behaved code should finish far inside it. The default is a starting point to tune from benchmark results.

### Write ordering

| Backend | Writers at once per database | Mechanism |
| --- | --- | --- |
| SQLite, in-process | 1 | Per-database FIFO queue, `BEGIN IMMEDIATE` |
| SQLite, worker | 1 | Same queue, held in the parent process |
| DuckDB | 1 by default | Same queue. Concurrent writers are an experiment. |
| PostgreSQL | Up to the pool size | No queue. The server arbitrates. |

The queue lives in the event loop as an asyncio primitive. No backend starts a dedicated writer thread per database.

### Batch shorthand

```python
await db.batch(
    [
        ("insert into log (msg) values (:m)", {"m": "start"}),
        ("update counters set n = n + 1 where name = :n", {"n": "runs"}),
    ],
    expect_schema_version=version,
)
```

The statements run in one transaction with no round trips between them. If `expect_schema_version` is given and the schema has changed, nothing runs and `SchemaChanged` is raised. That lets a caller read the schema, build its SQL, and send it without holding a transaction open while it thinks.

### Large writes

Callers should send large imports as many short transactions, not one long one. `insert_rows` does this by default with `batch_size=100`. A caller that needs all-or-nothing behaviour uses the staged import described under High-level operations.

## High-level operations

Named operations cover the common writes so that callers rarely need hand-written SQL or an interactive transaction. Each one exists on both `db` and `tx`. Called on `db`, it runs in its own transaction.

```python
await db.create_table(
    "posts",
    {"id": "integer", "title": "text", "body": "text"},
    pk="id",
)
await db.insert_rows("posts", rows, alter=True)
await db.upsert_rows("posts", rows, pk="id")
```

### Operations in the prototype

| Operation | Notes |
| --- | --- |
| `create_table(name, columns, *, pk, not_null, defaults, foreign_keys, if_not_exists)` | Column types from the portable set, or a native type string passed through. |
| `drop_table(name)`, `rename_table(old, new)` |  |
| `add_column(table, name, type)` |  |
| `alter_table(table, *, rename, drop, types, pk)` | SQLite rebuilds the table as sqlite-utils `transform` does. Other backends use native `ALTER`. |
| `create_index(table, columns, *, unique, if_not_exists)` |  |
| `insert_rows(table, rows, *, pk, alter, ignore, replace, batch_size)` | With `alter=True`, adds missing columns first. Creates the table if absent. |
| `upsert_rows(table, rows, *, pk, alter)` |  |
| `update_row(table, pk_values, updates)`, `delete_rows(table, pk_values_list)` |  |
| `staged_import(table, *, mode)` | Atomic large import. See below. |

Portable column types are `integer`, `float`, `text` and `blob`. Each backend maps them to its own type names.

### How an operation is implemented

Each operation has one default implementation in the core. It is written against the transaction API, the introspection methods and a small per-backend `Dialect` object. The dialect supplies identifier quoting, type names and syntax for upsert and conflict handling.

A backend may override any operation with a faster native path. Expected overrides:

- **SQLite worker:** sends the operation as one message, so the worker runs the default implementation locally with no round trips.
- **PostgreSQL:** `COPY` for `insert_rows`.
- **DuckDB:** a bulk-load path for `insert_rows`, to be chosen during that milestone.

The conformance suite runs every operation through both the default path and the override, and requires identical results.

### Staged import

A staged import gives all-or-nothing behaviour for a large load while keeping every transaction short.

```python
async with db.staged_import("posts", mode="replace") as imp:
    async for chunk in source:
        await imp.write(chunk)      # one short transaction per chunk
# leaving the block performs the swap in one short transaction
```

1. Create a hidden staging table named `_dblayer_import_<id>`. It is an ordinary table, not a `TEMP` table.
2. Write each chunk to it in its own transaction.
3. On clean exit, swap it in. On error, drop it.

| Mode | Final step |
| --- | --- |
| `create` | Rename staging to the target. Fails if the target exists. |
| `replace` | Drop the target and rename staging, in one transaction. Indexes are recreated on staging first. |
| `append` | `INSERT INTO target SELECT * FROM staging`, then drop staging, in one transaction. |

Things the prototype must handle and test:

- **Orphans.** A crash leaves a staging table behind. `db.sweep_staged(max_age)` drops any staging table older than the given age.
- **Dependents.** Replacing a table that views, triggers or foreign keys refer to may fail on some backends. The suite records which.
- **Visibility.** The staging table is hidden from listings by its prefix, but it is still readable by name.
- **Append cost.** The final copy in `append` mode holds the write slot for as long as the copy takes. The benchmark measures this at one million rows.

## Schema introspection and change detection

Introspection is a fixed set of named methods, which replaces most of what Datasette core does with `execute_fn` today. Every method is available on both `db` and `tx`.

| Method | Returns |
| --- | --- |
| `table_names()`, `view_names()` | Lists of names |
| `table_columns(table)` | Columns with name, type, not-null, default, primary key position and hidden flag |
| `primary_keys(table)` | Column names in key order |
| `foreign_keys(table)` | Outbound foreign keys |
| `indexes(table)` | Index name, columns and uniqueness |
| `definition(name)` | The `CREATE` statement, where the backend can supply one |
| `schema()` | Everything above for the whole database, in one call |
| `schema_version()` | An opaque token that changes when the schema changes |
| `change_hint()` | A cheap token that changes if the schema *might* have changed |

`schema()` exists so that a catalog, such as Datasette's internal database, can be filled in one round trip. On the worker backend it is a single message.

### Two-level change detection

A caller that tracks many databases checks `change_hint()` first and calls `schema_version()` only when the hint moves. The hint must not open a connection or wake a sleeping database.

| Backend | `change_hint()` | `schema_version()` |
| --- | --- | --- |
| SQLite, both kinds | Size and modification time of the database file and its `-wal` file | `PRAGMA schema_version` |
| DuckDB | Size and modification time of the database file and its WAL file | Hash of the catalog listing |
| PostgreSQL | None available; returns a time-based token that changes every `schema_ttl` seconds | Hash of a catalog query over the configured schema |

For PostgreSQL this means schema changes made by other clients are seen only after the TTL, 300 seconds by default, or on an explicit `db.refresh_schema()`. That matches the "click refresh if you can't see your table" behaviour discussed for remote backends.

### Schema cache

Each `Database` keeps a cache of introspection results keyed by schema version. A write made through the object invalidates the cache before the caller hears back. The cache is dropped when the database sleeps, since it can be rebuilt from one `schema()` call.

## Lifecycle: pool, sleep and wake, registry

An idle database costs one small Python object and nothing else. Connections, threads and worker processes are shared or released, never owned for the life of a database.

### States

| State | Holds | Enters when |
| --- | --- | --- |
| Cold | Nothing local. The file lives elsewhere, such as object storage. | Evicted from disk by an `on_evict` hook |
| Asleep | The file on disk. No connections, threads or processes. | Created, or idle past `idle_timeout`, or `db.sleep()` |
| Awake | One or more pooled connections | First call after sleep |
| Closed | Nothing. Every call raises `DatabaseClosed`. | `db.close()` |

Sleep is reversible and invisible to callers: the next call wakes the database. Close is permanent, which keeps the meaning Datasette documents for `close()` today.

### Connection pool

Each `Database` has a pool in place of today's thread-local connections.

- **Checkout.** A call checks a connection out, uses it and returns it. Connections are not tied to threads.
- **Size.** At most `max_read_connections` per database, default 3. At most one write connection.
- **Idle timeout.** A connection unused for `idle_timeout` seconds, default 30, is closed.
- **No close race.** Only checked-in connections are ever closed. A request in flight always finishes on the connection it holds.
- **Reset on return.** A returned connection has any open transaction rolled back and its time limit cleared.

Blocking drivers (SQLite and DuckDB) run on one shared thread pool owned by the registry. Thread count is fixed by configuration and does not grow with the number of databases.

### Registry

A `Registry` owns many databases, the shared thread pool and the global limits.

```python
registry = Registry(
    max_open_connections=200,
    max_threads=8,
    resolver=resolve,          # async: name -> Database or None
    on_wake=fetch_if_cold,     # async: called before the first connection opens
    on_sleep=None,             # async: called after the last connection closes
)
db = await registry.get("customer-123/sales")
```

- **Global cap.** When `max_open_connections` is reached, the registry closes the least recently used idle connection in any database. If none is idle, the caller waits.
- **Lazy lookup.** `registry.get()` calls `resolver` for a name it does not know. This stands in for a future Datasette plugin hook.
- **Wake lock.** Concurrent first requests for one database trigger a single `on_wake` call. The others wait for it.
- **Constant-time operations.** Adding, removing and looking up a database must not scan or copy the full set.
- **Introspection.** `registry.stats()` reports awake databases, open connections, wakes, sleeps and evictions.

### Object storage stand-in

The prototype does not talk to S3. The tests implement `on_wake` and `on_evict` against a local directory that plays the part of the remote store. That is enough to test the wake lock, eviction under a disk quota, and the rule that a writable database is only safe to evict after it has been copied out.

## Backend interface

A backend implements a small connection-level interface, and the core builds everything else on top of it. This is the interface a future Datasette backend plugin would implement.

```python
class Backend(Protocol):
    capabilities: Capabilities
    dialect: Dialect

    async def open(self, *, write: bool) -> "BackendConnection": ...
    def change_hint(self) -> object: ...          # must not open a connection
    async def close(self) -> None: ...

class BackendConnection(Protocol):
    async def execute(self, sql, params, *, time_limit_ms, max_rows) -> Results: ...
    async def execute_many(self, sql, params_seq) -> WriteResult: ...
    def stream(self, sql, params, *, chunk_rows) -> AsyncIterator[list[tuple]]: ...
    async def begin(self, mode: str) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
    async def schema(self) -> Schema: ...
    async def schema_version(self) -> object: ...
    async def interrupt(self) -> None: ...
    async def close(self) -> None: ...

    # Optional: native overrides for named operations
    async def run_operation(self, name: str, args: dict) -> object: ...
```

The core owns the pool, the write queue, deadlines, the schema cache, parameter rewriting, error mapping and the default implementation of every named operation. A minimal backend is therefore the ten required connection methods plus a dialect.

### Capability flags

The conformance suite skips or adjusts tests by flag. A caller can check a flag before relying on a feature.

| Flag | SQLite in-process | SQLite worker | PostgreSQL | DuckDB |
| --- | --- | --- | --- | --- |
| `concurrent_writers` | No | No | Yes | Experimental |
| `transactional_ddl` | Yes | Yes | Yes | To verify |
| `implicit_rowid` | Yes | Yes | No | No |
| `cheap_change_hint` | Yes | Yes | No | Yes |
| `hard_kill` | No | Yes | Server-side cancel | No |
| `memory_limit` | No | Yes | Server-side setting | Yes |
| `table_definition_sql` | Yes | Yes | No | To verify |
| `full_text_search` | Yes | Yes | Different syntax | Different syntax |
| `sleep_releases_everything` | Yes | Yes | Yes | Yes |

Cells marked "to verify" are assumptions from memory. Confirming them is part of each backend's milestone.

## Backend: in-process SQLite

This backend is the reference implementation and the baseline for every benchmark. It uses the standard library `sqlite3` module and behaves as Datasette does today, minus the thread-local connections and the per-database writer thread.

- **Connections.** Read connections open with `mode=ro`, or `immutable=1` when the database is declared immutable. The write connection opens read-write. All use `check_same_thread=False`, and the pool guarantees one thread at a time per connection.
- **Threads.** Each call runs on the registry's shared thread pool via `loop.run_in_executor`. Context variables are copied so tracing still works.
- **Transactions.** Python's implicit transaction handling is switched off. The backend issues `BEGIN IMMEDIATE`, `BEGIN`, `COMMIT` and `ROLLBACK` itself.
- **Interactive transactions.** The transaction pins one connection but not one thread. Each `await tx.execute()` is a separate hop to the thread pool.
- **Time limit.** A progress handler checks a deadline, as `sqlite_timelimit` does today. `interrupt()` calls `Connection.interrupt()` from another thread.
- **Memory limit.** Not enforceable in-process. The capability flag is off.
- **Schema version.** `PRAGMA schema_version`.
- **Connection init.** An optional `init(conn)` callable runs on every new connection, for registering SQL functions or loading extensions. This is the one place a raw connection is visible, and it never leaves the backend.
- **In-memory databases.** Named shared-cache memory databases are supported. They cannot sleep, because closing the last connection destroys them, so the pool keeps one connection open.

### What this backend must demonstrate

1. Every named operation and the staged import, using only the default implementations.
2. Sleep and wake with no leaked file descriptors, checked by counting entries in `/proc/self/fd`.
3. A writer queue with no dedicated thread: 1,000 databases each written once must not raise the process thread count.
4. Rollback when the task holding a transaction is cancelled part-way through.

## Backend: SQLite in a separate process

The worker backend is a thin remote proxy: the worker process runs the in-process backend, and the parent sends it messages. Its purpose is to make untrusted SQL safe to run, by giving the worker a memory limit and killing it when a query will not stop.

### Process model

- **Launch.** The parent starts `python -m dblayer.worker` with `asyncio.create_subprocess_exec` and talks over the child's stdin and stdout. Stderr is captured for logs.
- **One job at a time.** A worker executes one statement at a time on its main thread. A second thread reads incoming frames so it can act on `cancel` while SQL is running.
- **Pinning.** An interactive transaction pins its worker until commit or rollback.
- **Roles.** A database has at most one write worker and up to `max_read_connections` read workers. Each starts on demand and exits when idle. A sleeping database has no workers.
- **Placement policy.** Two policies are implemented and compared: `dedicated`, where a worker serves one database, and `shared`, where a worker serves many databases one request at a time.

### Limits and kill

| Limit | Soft enforcement, inside the worker | Hard enforcement, by the parent |
| --- | --- | --- |
| Time | Progress handler, then `Connection.interrupt()` from the reader thread | `SIGKILL` if no reply within the limit plus a 250 ms grace period |
| Memory | `PRAGMA hard_heap_limit` | `RLIMIT_AS` set in the child before it opens the database |
| Result size | Stop fetching at `max_result_bytes` | Frame size cap on the parent's reader |
| SQL complexity | `Connection.setlimit()` for expression depth, SQL length and similar | None |

After a kill, the in-flight call raises `WorkerCrashed`, or `QueryInterrupted` if the kill was for a time limit. The pool starts a replacement on the next request. A killed write worker loses its open transaction, and SQLite's journal or WAL recovery discards the partial work when the file is next opened.

`RLIMIT_AS` behaves differently on macOS and does not exist on Windows. The prototype targets Linux for hard limits and reports the capability as off elsewhere.

### Wire protocol

Each frame is a 4-byte big-endian length followed by a payload. The payload codec is agreed in the first exchange. JSON is implemented first because it needs no dependencies. MessagePack is the second codec, added for the benchmark comparison.

In JSON, `bytes` values travel as `{"$b": "<base64>"}`. Integers stay exact because both ends are Python. Non-finite floats use Python's `NaN` and `Infinity` extensions.

Every request carries an `id`. Every reply repeats it.

| Request | Fields | Reply |
| --- | --- | --- |
| `hello` | `protocol`, `codecs` | `hello` with `codec`, `sqlite_version`, `pid` |
| `open` | `path`, `mode`, `limits`, `init` | `ok` |
| `execute` | `sql`, `params`, `time_limit_ms`, `max_rows`, `tx` | `result` |
| `execute_many` | `sql`, `params_seq`, `tx` | `result` |
| `stream` | `sql`, `params`, `chunk_rows`, `credit` | `rows` frames, then `end` |
| `more` | `stream`, `credit` | none; grants more `rows` frames |
| `begin` | `mode`, `deadline_ms` | `ok` with `tx` |
| `commit`, `rollback` | `tx` | `ok` |
| `op` | `name`, `args`, `tx` | `result` |
| `schema`, `schema_version` | `tx` | `result` |
| `cancel` | `target` (a request id) | none; the target request replies with an error |
| `sleep` | none | `ok`; the worker closes its connection and exits |

An error reply is `{"id": 7, "error": {"type": "QueryError", "message": "...", "code": "SQLITE_CONSTRAINT"}}`. The parent maps `type` back to the shared exception classes.

### Example: insert with alter, as an interactive transaction

```
→ {"id": 1, "type": "begin", "mode": "write", "deadline_ms": 5000}
← {"id": 1, "ok": true, "tx": "t1"}
→ {"id": 2, "type": "schema", "tx": "t1", "table": "posts"}
← {"id": 2, "result": {"columns": [{"name": "id", "type": "INTEGER"}, {"name": "title", "type": "TEXT"}]}}
→ {"id": 3, "type": "execute", "tx": "t1", "sql": "alter table \"posts\" add column \"slug\" TEXT"}
← {"id": 3, "result": {"rowcount": -1}}
→ {"id": 4, "type": "execute_many", "tx": "t1", "sql": "insert into ...", "params_seq": [{...}, {...}]}
← {"id": 4, "result": {"rowcount": 2}}
→ {"id": 5, "type": "commit", "tx": "t1"}
← {"id": 5, "ok": true}
```

### The same work as one named operation

```
→ {"id": 1, "type": "op", "name": "insert_rows", "args": {"table": "posts", "rows": [{...}, {...}], "alter": true}}
← {"id": 1, "result": {"rowcount": 2, "columns_added": ["slug"]}}
```

Five round trips become one, and the write slot is held only while the worker computes. The benchmark reports both forms.

### Deadline and disconnect handling

- The worker tracks each open transaction's deadline itself and rolls back on expiry, even if the parent is unresponsive.
- If the worker reads end-of-file on stdin, it rolls back and exits.
- The parent also runs a timer. If the worker has not confirmed rollback shortly after the deadline, the parent kills it.

### Connection init

Plugins today register custom SQL functions through `prepare_connection`, which hands them a raw connection. That cannot cross the process boundary. The `open` message therefore takes `init`, a list of importable `module:function` names that the worker calls with each new connection. The functions run inside the worker, so they are subject to its limits.

### What this backend must demonstrate

1. The full conformance suite passes with results identical to the in-process backend.
2. A query that ignores the progress handler is killed within the grace period, and the next query succeeds.
3. A query that allocates without bound is stopped by the memory limit without affecting the parent.
4. Killing a write worker mid-transaction leaves the database passing `PRAGMA integrity_check`, with no lock held.
5. Measured overhead per query, per row and per worker start.

## Backend: PostgreSQL

PostgreSQL tests whether the API survives a database that is remote, has many concurrent writers and offers no cheap schema version. The driver is `asyncpg`, which is natively async, so this backend uses no threads. Choosing `psycopg` 3 instead is an open question.

- **What a "database" is.** One `Database` object maps to one schema inside a PostgreSQL database. The backend sets `search_path` on each connection. Table names in the API stay unqualified.
- **Pool.** An `asyncpg` pool with a minimum size of zero. Sleep closes the pool. The registry's global connection cap applies as it does for SQLite.
- **Parameters.** `:name` and `?` are rewritten to `$1`, `$2` and so on.
- **Read transactions.** `mode="read"` opens `REPEATABLE READ, READ ONLY` for a consistent snapshot.
- **Write transactions.** Default isolation. There is no write queue. A serialization or deadlock failure raises `WriteConflict`.
- **Time limit.** `SET LOCAL statement_timeout` inside transactions, plus the driver's per-call timeout, which cancels on the server.
- **Transaction deadline.** A client timer, backed by `idle_in_transaction_session_timeout` on the session so the server enforces it if the client hangs.
- **Introspection.** Queries against `pg_catalog`, restricted to the configured schema. `definition()` raises `NotSupported`.
- **Schema version.** A hash of the catalog query, refreshed on a TTL as described under Schema.
- **Native overrides.** `insert_rows` uses `COPY` when no conflict handling is requested. `upsert_rows` uses `INSERT ... ON CONFLICT`.
- **No implicit rowid.** Operations that address rows need a primary key. A table without one is readable but `update_row` and `delete_rows` raise `NotSupported`.
- **Values.** `asyncpg` returns native Python types for dates, decimals, UUIDs, arrays and JSON. The backend passes them through unless the portable-values option is on.
- **Staged import.** Drop and rename run in one transaction. PostgreSQL takes an exclusive lock on the table for that moment only.

### What this backend must demonstrate

1. The conformance suite passes, with skips only where a capability flag is off.
2. Two write transactions on one database make progress at the same time.
3. A transaction left open past its deadline is rolled back by the server, with the client task suspended.
4. Sleep closes every server connection, checked against `pg_stat_activity`.
5. A schema change made by another client is seen after `refresh_schema()` and after the TTL.

Tests run against a real server, started by the test fixture in a container. The suite is skipped with a clear message when no server is available.

## Backend: DuckDB

DuckDB tests the API against an embedded engine with a different concurrency model and no progress-handler hook. It uses the `duckdb` Python package on the shared thread pool.

The points below marked "to verify" are from memory. Confirming each against current DuckDB documentation is the first task of this milestone.

- **Instance and cursors.** One DuckDB instance per database file, opened on wake and closed on sleep. Each pooled "connection" is a cursor from that instance, used by one thread at a time.
- **Read-only and read-write.** One process cannot hold the same file open in both modes at once (to verify). The backend therefore opens read-write when the database is writable, and enforces read-only at the API level for read calls.
- **Writers.** DuckDB uses optimistic concurrency and aborts a transaction that conflicts with another (to verify). The default is the same single-writer queue as SQLite. A flag turns the queue off so the conformance suite can measure conflict rates.
- **Time limit.** There is no progress handler. A timer calls `interrupt()` on the cursor from another thread.
- **Memory and threads.** `SET memory_limit` and `SET threads` are applied on open, from the backend's configuration. Defaults are deliberately low, because the registry may have many instances awake at once.
- **Parameters.** `:name` is rewritten to `$name`. Positional `?` passes through.
- **Introspection.** The `duckdb_tables()`, `duckdb_columns()`, `duckdb_constraints()` and `duckdb_indexes()` functions.
- **Schema version.** A hash of the catalog listing. `change_hint()` uses file size and modification time.
- **Native overrides.** `insert_rows` needs a bulk path, because row-by-row inserts are slow in DuckDB. Candidates are a multi-row `VALUES` statement and insertion from an Arrow table. The milestone picks one by benchmark.
- **Alter limits.** Some `ALTER TABLE` forms are rejected when indexes or constraints depend on the column (to verify). `alter_table` falls back to the rebuild approach used for SQLite.
- **Cost when awake.** A DuckDB instance holds more memory than a SQLite connection. The 10,000-database test reports the per-instance figure.

### What this backend must demonstrate

1. The conformance suite passes, with skips only where a capability flag is off.
2. A long analytical query is interrupted within 100 ms of its time limit.
3. Sleep releases the file, so another process can open it read-write.
4. With the write queue off, the suite records how often concurrent writers conflict.

A worker-process variant of DuckDB is out of scope. The wire protocol carries no SQLite-specific fields, so that the option stays open.

## Conformance tests, fault injection and benchmarks

One pytest suite, parametrised by backend, is the main output of the prototype. A behaviour that cannot be written as a test that passes on all four backends becomes either a capability flag or a finding in the final report.

### Conformance suite

| Area | Examples |
| --- | --- |
| Reads | Parameter styles, `max_rows` truncation, time limit raises `QueryInterrupted`, streaming with a slow consumer |
| Parameter rewriting | Placeholders inside strings, comments, quoted identifiers and `::` casts are left alone |
| Transactions | Commit, rollback on exception, rollback on cancellation, read snapshot isolation, deadline expiry, `NestedWriteError` |
| Write ordering | Two writers are serialised on single-writer backends and concurrent on PostgreSQL |
| Named operations | Each operation through the default path and any native override, with identical results |
| Staged import | All three modes, failure part-way, orphan sweep, target with indexes |
| Introspection | Each method, `schema()` equal to the sum of the others, version changes after DDL |
| Lifecycle | Sleep and wake, idle timeout, global cap eviction, wake lock, close is permanent |
| Errors | Each exception class is raised by the same action on every backend |

### Fault injection

| Fault | Expected result |
| --- | --- |
| Kill a read worker mid-query | Caller gets `WorkerCrashed`; next query succeeds on a new worker |
| Kill a write worker mid-transaction | No partial write visible; integrity check passes; next writer proceeds |
| Cancel the task holding a transaction | Rollback; write slot released within 50 ms |
| Caller sleeps past the deadline | `TransactionExpired`; other writers proceed |
| Parent stops reading from the worker | Worker blocks on output, then is killed by the parent's timer |
| Unbounded recursive query | `QueryInterrupted` at the time limit |
| Query that builds a multi-gigabyte value | `ResourceLimitExceeded`; parent memory unchanged |
| Queries known to evade the progress handler | Hard kill within the grace period |
| Truncated or oversized frame | Connection dropped; worker replaced |

The list of queries that evade the progress handler should come from the cases that prompted this work in Datasette.

### Scale test

Create 10,000 small SQLite files and register them all. Drive requests with a skewed distribution, so that a few databases are hot and most are touched rarely or never. Assert throughout:

- Open file descriptors stay at or below `max_open_connections` plus a fixed allowance.
- Thread count stays constant.
- Resident memory per registered-but-asleep database is reported, with a target under 2 KB.
- Registering all 10,000 takes time linear in the count.

Repeat with 1,000 databases on the worker backend and on DuckDB, and report the same figures.

### Benchmarks

All benchmarks compare against the in-process SQLite backend on the same machine.

| Measurement | Variants |
| --- | --- |
| Round-trip time for a one-row read | In-process, worker with JSON, worker with MessagePack |
| Throughput for reads of 1,000 and 100,000 rows | Same three, plus streaming |
| Insert of one million rows | `insert_rows` in batches, interactive transaction per batch, staged import |
| Insert with alter | Interactive transaction against one named operation |
| Worker start to first result | Dedicated and shared placement |
| Wake from sleep to first result | All four backends |
| Write slot hold time under contention | 1, 10 and 100 concurrent writers |

The working hypothesis is that the worker adds well under a millisecond to a small query. The benchmark exists to confirm or refute that.

## Milestones and deliverables

The milestones are ordered so that each backend after the first is tested against a suite that already passes.

| # | Milestone | Done when |
| --- | --- | --- |
| 1 | Core API and in-process SQLite | Reads, single writes, parameter rewriting, errors and limits pass the suite |
| 2 | Transactions and named operations | Interactive transactions, `batch`, every operation and the staged import pass |
| 3 | Lifecycle and registry | Pool, sleep and wake, global cap and the 10,000-database test pass |
| 4 | SQLite worker | Protocol implemented; suite passes; fault-injection tests pass on Linux |
| 5 | PostgreSQL | Suite passes with documented skips |
| 6 | DuckDB | Assumptions verified; suite passes with documented skips |
| 7 | Benchmarks and report | Numbers collected; findings written up |

### Deliverables

- A Python package with the four backends and no dependency on Datasette.
- The conformance, fault-injection and scale test suites.
- A benchmark script that prints one table of results.
- A protocol reference for the worker, with a recorded example session.
- A findings report covering: answers to the questions in the summary, the capability matrix as measured, a recommended API for Datasette, and a migration map for plugins that use `execute_fn` and `execute_write_fn`.

### Engineering constraints

- Python 3.11 or later, for `asyncio.TaskGroup`, `asyncio.timeout` and `sqlite3.Connection.setlimit`.
- Core and both SQLite backends use the standard library only. `asyncpg`, `duckdb` and `msgpack` are optional extras.
- No raw driver connection appears in any public signature, apart from the connection init hook.
- Type hints throughout. The backend interface is a `typing.Protocol`.

## Open questions

These are decisions the spec makes provisionally. Each one names the default the prototype will use unless you choose otherwise.

| # | Question | Provisional default |
| --- | --- | --- |
| 1 | PostgreSQL driver: `asyncpg` or `psycopg` 3? | `asyncpg` |
| 2 | Does a `Database` map to a PostgreSQL schema or a whole database? | Schema |
| 3 | Return native Python values, or coerce to the portable set? | Native, with an opt-in portable mode |
| 4 | How should plugins register SQL functions when SQLite runs in a worker? | Importable `module:function` names called in the worker |
| 5 | Worker placement: dedicated per database, or shared? | Build both and decide from the benchmark |
| 6 | Wire codec: JSON or MessagePack? | JSON first; decide from the benchmark |
| 7 | Default transaction deadline | 5 seconds |
| 8 | Should a request's reads share one snapshot by default? | No; available through `mode="read"` |
| 9 | Multi-statement scripts: keep an `execute_script`? | No; `batch()` takes a list of statements |
| 10 | DuckDB: single-writer queue or concurrent writers? | Queue on, with a flag to measure the alternative |
| 11 | Hard limits on macOS and Windows | Linux only; capability off elsewhere |
| 12 | Where do Datasette's `write_wrapper` and `track_event` hooks attach? | Out of scope; the transaction object is the natural place |
| 13 | Savepoints inside a transaction | Stretch goal after milestone 2 |
