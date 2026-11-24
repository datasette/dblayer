# Worker protocol

The wire protocol between the parent (`dblayer.backends.sqlite_worker`) and a
worker process (`python -m dblayer.worker`). The code is the reference:
`dblayer/worker/protocol.py` (framing, codecs, `MESSAGE_TYPES`) and
`dblayer/worker/__main__.py` (one `do_<type>` handler per request).
`tests/worker/test_protocol_doc.py` checks this document against both: every
message type in the code appears in the table below, and the recorded session
at the end is regenerated from a real worker and compared.

A second implementation, the C worker in `cworker/`
(`dblayer.backends.sqlite_cworker.SqliteCWorker`), speaks this protocol for
the SQLite engine only; `tests/worker/test_cworker.py` and the conformance
suite (`sqlite-cworker`) check it against the Python worker and in-process
SQLite. Where it differs is listed under "Implementations" below. A third,
the Node worker in `nodeworker/` (SQLite compiled to WebAssembly, in Node.js;
`dblayer.backends.sqlite_nodeworker`), is described there too.

## Transport and framing

- The parent starts the worker with `subprocess.Popen` and three pipes: the
  worker reads requests on stdin and writes replies on stdout; stderr is
  captured into the parent's `dblayer.worker` logger (and the last 8 KB are
  quoted when a worker dies). Inside the worker, file descriptor 1 is
  pointed at stderr, so a stray `print()` (from an init hook, say) cannot
  corrupt the protocol stream.
- The worker's command line is `python -m dblayer.worker [--memory-limit-mb
  N] [--debug] [--preload MODULE ...]`. `--preload` imports a module before
  serving (a DuckDB worker preloads `duckdb` and `dblayer.backends.duckdb`),
  so a pre-started spare process (`SpareWorkers`) has already paid for its
  imports when a database takes it.
- Each frame is a 4-byte big-endian unsigned length followed by that many
  bytes of payload.
- Both readers refuse a frame whose declared length is over
  `max_frame_bytes` (default 256 MB, agreed in `hello`) as soon as the header
  arrives. The parent then kills the worker and fails the pending call with
  `ResourceLimitExceeded`; the pool replaces the worker on the next request.
  A worker refuses to *send* a reply over the cap and sends a
  `ResourceLimitExceeded` error instead; the parent refuses to send an
  oversized request (`ResourceLimitExceeded`, nothing sent).
- End of input on either side is a dropped connection. A frame cut short by
  end of input (a truncated frame) or a payload that does not decode is
  treated the same way: the worker is killed and the call raises
  `WorkerCrashed`.

## Codecs

The `hello` exchange is always JSON. The parent lists the codecs it wants in
order; the worker picks the first it has, and every later frame in both
directions uses it.

| Codec | `bytes` | Integers | Non-finite floats | Notes |
| --- | --- | --- | --- | --- |
| `json` (stdlib) | `{"$b": "<base64>"}` | exact, any size: a number with no fraction or exponent is an integer, one with either is a float (Python writes `1.0`, never `1`, for a float) | `NaN`, `Infinity`, `-Infinity` (Python extensions) | A user dict whose only key is `$b`, `$d` or `$x` is escaped as `{"$d": [key, value]}`. |
| `msgpack` (optional extra) | native `bin` | native up to 64 bits; larger as ext type 1 (decimal text). A float is always a float (float 64), even with an integral value | native | |

Both rules matter to a peer whose numbers are doubles (JavaScript): SQLite
INTEGER is 64-bit, so integers beyond 2^53 must be kept exactly (the Node
worker uses `BigInt`), and `1.0` must stay a float, since SQLite binds it
as REAL and returns it as `1.0`, not `1`.

Value types beyond the portable set (dates, decimals, UUIDs from a DuckDB or
PostgreSQL engine) go through one extension point,
`dblayer.worker.protocol.register_value_type(ValueType(tag, ext_code, cls,
to_wire, from_wire))`: JSON sends `{"$x": [tag, to_wire(v)]}`, MessagePack an
ext type `ext_code`. The standard library types DuckDB returns are
registered by default (`STANDARD_VALUE_TYPES`, ext codes 16 to 23):
`datetime` (a `ZoneInfo` zone is kept by name), `date`, `time`, `Decimal`,
`UUID`, `timedelta`, and two marker shapes, `map` (a dict with non-string
keys, or a single `$b`/`$d`/`$x` key) and `tuple` (a fixed-size array).
Markers are applied only to result columns whose declared type can hold a
map, struct or array (`MAP(`, `STRUCT(`, `UNION(`, `[`), so plain results
pay nothing. Results carry `types` (the declared column types) when the
engine reports any; the first `stream` frame does too.

## Messages

Every request carries an `id` (an integer, unique per worker); every reply
repeats it. Requests on one worker are executed one at a time in arrival
order; the parent sends the next request only after the previous reply
(so a time limit is measured from when the request is sent). The exception
is a multi-tenant C worker (below): there, requests for *different*
connections run concurrently and replies arrive in any order, matched by
`id`; requests for one connection still run in order, one at a time. `cancel` and
`more` are control messages: they have no reply of their own and are acted on
as soon as they arrive.

Connection-scoped requests may carry `conn` (the id `open` returned). It may
be left out when the worker has a single connection, which is always the case
with dedicated placement; shared placement always sends it. Requests inside a
transaction carry `tx` (the id `begin` returned); the worker refuses a `tx`
that is not open (`TransactionClosed`) or has expired (`TransactionExpired`),
so a statement can never silently run outside its transaction.

| Request | Fields | Reply |
| --- | --- | --- |
| `hello` | `protocol`, `codecs`, `max_frame_bytes` | `hello` with `codec`, `protocol`, `sqlite_version`, `python`, `pid`, `rlimit_as`; optionally `implementation` (`"c"`) and `ops` (`false`: `op` is not supported, the parent runs named operations itself) |
| `open` | `path`, `mode` (`read`/`write`), `engine` (`module:callable`, default `dblayer.worker.engines:sqlite`), `config` (engine keyword arguments: for SQLite `init`, `sql_limits`, `memory_limit_mb`, `immutable`, `create`, `busy_timeout`) | `ok` with `conn` |
| `close` | `conn` | `ok`; the connection is rolled back and closed (shared placement) |
| `execute` | `sql`, `params`, `time_limit_ms`, `max_rows`, `max_result_bytes`, `tx` | `result`: `columns`, `rows`, `truncated`, `rowcount`, `last_pk` (defaults left out) |
| `execute_many` | `sql`, `params_seq`, `tx` | `result`: `rowcount` |
| `batch` | `statements` (`[[sql, params], ...]`), `time_limit_ms`, `max_rows`, `tx` | `result`: one result per statement |
| `stream` | `sql`, `params`, `chunk_rows`, `credit`, `time_limit_ms`, `max_result_bytes`, `tx` | up to `credit` frames `{"id", "rows"}` (the first also has `columns`), then the worker pauses with its cursor open; `{"id", "end": true}` when done |
| `more` | `stream` (the stream's id), `credit` | none; grants `credit` more `rows` frames |
| `export` (C worker only) | `sql`, `params`, `encoding`, `transport`, `chunk_bytes`, `credit`, `max_rows`, `time_limit_ms`, `data_path`, `handoff_path`, `tx` | see "Export" below |
| `begin` | `mode` (`write`/`read`), `deadline_ms` | `ok` with `tx` |
| `commit`, `rollback` | `tx` | `ok` (`rollback` of an expired transaction: `ok` with `expired: true`) |
| `op` | `name`, `args` (as `normalise_args` produces them), `tx` | `result`: `rowcount`, `table_created`, `columns_added` |
| `schema` | `tx` | `result`: the whole schema (`version`, `tables`, `views`, `triggers`) |
| `schema_version` | `tx` | `result`: the version token |
| `cancel` | `target` (a request id) | none; the target replies with a `QueryInterrupted` error (a running statement is interrupted; a queued request is not run). Cancelling a paused stream closes its cursor and the stream replies `{"end": true, "cancelled": true}`. |
| `sleep` | none | `ok`; the worker closes every connection and exits |
| `expired` (notice, worker to parent) | `conn`, `tx`; no `id` | sent when the worker rolled a transaction back at its deadline |
| `_debug` (only with `--debug`) | `action` | test hooks: `sleep`, `oversized_frame`, `truncated_frame`, `garbage`, `alloc`, `exit`, `print` |

Every reply to a connection-scoped request carries `"in_tx": true` while a
transaction is open on that connection, so the parent can skip the
reset-on-return `rollback` when there is nothing to roll back.

### Errors

An error reply is

```json
{"id": 7, "error": {"type": "IntegrityError", "message": "...", "code": "SQLITE_CONSTRAINT_PRIMARYKEY",
                    "cause": {"type": "sqlite3.IntegrityError", "message": "..."}}}
```

`type` is the name of one of the shared exception classes (`QueryError`,
`IntegrityError`, `QueryInterrupted`, `ResourceLimitExceeded`,
`TransactionExpired`, `TransactionClosed`, `WriteConflict`, `NotSupported`,
...); the parent raises that class with `message` and `code`, and a
`WorkerError` describing `cause` as `__cause__` (the driver exception cannot
cross the boundary). The worker maps driver exceptions with the engine's
`Dialect.map_exception`, exactly as the core does in-process. Built-in errors
the engine raises unmapped (`OverflowError` for an integer SQLite cannot
store, `ValueError`, `TypeError`) are sent with `"builtin": true` and raised
as themselves. `"fatal": true` means the worker exits after this reply (a
`MemoryError` under `RLIMIT_AS`).

JSON escaping covers dict-valued `params` and `op` arguments only: a dict
nested in a list `params`, in `params_seq` or in `batch` statements is not
escaped, so a one-key `{"$b": ...}` value there arrives as bytes rather than
as an (unbindable) dict, in either worker. MessagePack has no such case.

## Implementations

| | Python worker (`python -m dblayer.worker`) | C worker (`cworker/build/dblayer-cworker`) |
| --- | --- | --- |
| Engines | any `module:callable` | SQLite only; another `engine` is `NotSupported` |
| `op` | runs the core's default implementation | `hello` says `"ops": false`; `op` gets `NotSupported` |
| `init` hooks | `module:function`, called per connection | refused (`NotSupported` at construction); `config.extensions` loads SQLite extensions by path |
| Codecs | JSON, MessagePack if installed | both, in C |
| SQLite | the interpreter's `sqlite3` module | compiled in (pinned amalgamation) or the system library |
| `SQLITE_NOMEM` (heap limit) | `MemoryError`: `ResourceLimitExceeded` with `fatal: true`, the worker exits | `ResourceLimitExceeded`, the worker carries on |
| Value-type params | decoded, then bound by `sqlite3` (default adapters for `datetime` and `date`) | the same results: `datetime`/`date` bound as ISO text, others a binding error |
| Start-up to `hello` | ~55 ms (interpreter and imports) | ~3 ms |
| Concurrency | one request at a time | one at a time; with `--multi`, one at a time *per connection* |

### Multi-tenant C worker (`dblayer-cworker --multi`)

Used by `dblayer.backends.sqlite_cpool` (`SqliteCPool`, `CPool`). Options:
`--max-connections N` (default 256), `--heap-limit-mb N` and
`--soft-heap-limit-mb N` (process-wide SQLite heap limits), `--cache-kb N`
(each connection's page cache), plus the usual `--memory-limit-mb` and
`--debug`.

- `hello` adds `"multi": true`, `max_connections` and `"threading":
  "multi-thread"`.
- Every `open` starts a thread that owns the new connection and runs its
  requests in arrival order. An `open` past `--max-connections` gets
  `ResourceLimitExceeded` at once. A connection's thread exits after its
  `close`.
- Connection-scoped requests are routed by `conn` (or to the only
  connection when there is exactly one, as in single-tenant mode); a
  request naming no open connection gets a `PROTOCOL` error. Requests with
  no connection (`sleep`, `_debug` without `conn`) run on the main thread.
- `cancel`, `more`, time limits, the backstop, transaction deadlines and
  the `expired` notice act on the one connection they concern. Replies are
  written whole, under one lock, so frames from different connections
  never interleave mid-frame.
- SQLite runs in multi-thread mode (`SQLITE_CONFIG_MULTITHREAD`): each
  connection is used by one thread only, so per-connection mutexes are
  off; `sqlite3_interrupt()` from the watchdog is safe in any mode.
- The per-database `memory_limit_mb` in `open`'s `config` is ignored (it
  would change every tenant's limit); heap limits come from the command
  line. A fatal error exits the whole process.

### Rust SQLite worker (`sqlite-rs-worker/`)

`sqlite-rs-worker/target/release/dblayer-sqlite-rsworker`, used by
`dblayer.backends.sqlite_rspool` (`SqliteRustPool`, `RustPool`): a port of
the C worker to Rust, request for request, with the same command line
(single-tenant, and `--multi` with every option above) and the same SQLite
(the pinned amalgamation, compiled by its `build.rs` with the `SQLITE_DEFS`
and `OPT` of `cworker/Makefile`). Differences:

- `hello` says `"implementation": "rust"`.
- `export` has the `frames`, `raw` and `fifo` transports, not `socket`
  (`hello` lists what it has; a `socket` request gets `NotSupported`).
- `--version` prints `dblayer-sqlite-rsworker protocol 1, SQLite ...` and
  the compile options.
- A connection (`Db`) is not `Send`: the compiler keeps it on its lane's
  thread, the rule `SQLITE_CONFIG_MULTITHREAD` relies on. Other threads
  reach a connection only through an interrupt handle that is emptied
  before the connection closes.

### Node worker (`nodeworker/`, SQLite-WASM)

`node nodeworker/src/main.mjs [--memory-limit-mb N] [--debug] [--version]`,
used by `dblayer.backends.sqlite_nodeworker` (`SqliteNodeWorker`): the
official SQLite WebAssembly build (`@sqlite.org/sqlite-wasm`, pinned in
`nodeworker/package.json`) with a file VFS over Node's synchronous `fs`
calls. Checked by `tests/worker/test_nodeworker.py` (including the C
worker's differential cases), `tests/worker/test_nodeworker_locking.py`,
the conformance suite (`sqlite-nodeworker`) and the fault tests.

- `hello` says `"implementation": "node"`, `"ops": false`,
  `"concurrent": true` (one request in flight per connection, as the DuckDB
  workers; the parent's `PipelinedWorkerProcess`), `"guard"` (whether the
  cross-process guard below is on), and adds `node` (the Node version) and
  `wasm` (the package). `op`, `init` hooks and
  `config.extensions` are refused as by the C worker (the build omits
  `load_extension`).
- **One process per database, one thread per connection.** Node cannot take
  the POSIX advisory locks native SQLite uses, so the VFS keeps SQLite's
  lock states in shared memory inside the process: correct between its
  connections, invisible to every other process. Every connection of a
  database is in one Node process (`DatabaseWorker` placement), each on an
  executor thread with its own WebAssembly instance, and the parent may have
  one request in flight *per connection* (as the multi-tenant C worker): a
  writer waiting on a reader's lock must not stop the reader's next
  request.
- **Cross-process guard** (`nodeworker/src/guard.mjs`). On the first `open`
  of a file the main thread opens it with Node's built-in native SQLite
  (`node:sqlite`, `journal_mode=MEMORY`) and holds `BEGIN EXCLUSIVE` until
  the last connection on it closes: an EXCLUSIVE fcntl lock owned by the
  process, which the worker's own WebAssembly connections never conflict
  with. Any other SQLite (the `sqlite3` shell, Python, another worker)
  gets `database is locked`; an `open` that meets another process's lock
  retries for `busy_timeout`, then fails with `QueryError` `SQLITE_BUSY`.
  POSIX drops a process's locks on a file when *any* descriptor of it is
  closed, and Node closes a worker thread's descriptors when the thread
  exits: while a guard is held the VFS keeps a closed connection's
  main-database descriptor (the thread's next open reuses it) and the main
  thread keeps a retired executor thread for the next connection. No guard for
  `immutable`, a WAL-mode file, or `DBLAYER_NODEWORKER_GUARD=0` (which
  reproduces the hazard: a Node worker writing beside a live native writer
  takes its journal for a hot journal, rolls it back and corrupts the
  database; `test_nodeworker_locking.py`). A read-only file gets a SHARED
  guard (other writers cannot commit). The guard's native SQLite reads the
  file header and rolls back a hot journal; it never reads table data.
- No shared memory in the VFS, so no WAL: `journal_mode=wal` leaves the
  database in `delete` mode, and a database already in WAL mode fails to
  open (`unable to open database file`).
- Cancellation reaches a running statement through shared memory read by
  SQLite's progress handler (the executor's JavaScript event loop is
  blocked while SQLite runs). A statement that does not reach a progress
  check is stopped by terminating its executor thread (V8 checks for
  termination inside WebAssembly loops), about 100 ms after its time limit
  or a cancel: the worker replies `QueryInterrupted` (or
  `TransactionExpired`) with `fatal: true` and exits, before the parent's
  `kill_grace_ms`.
- Memory: `--memory-limit-mb` is each connection's WebAssembly memory
  maximum (SQLite cannot allocate past it, on every platform; an
  allocation that fails is `SQLITE_NOMEM`, `ResourceLimitExceeded`, and the
  worker carries on) and its thread's V8 old-generation limit (exceeded:
  `ResourceLimitExceeded`, fatal). No `RLIMIT_AS` (`rlimit_as: false`): V8
  reserves far more address space than any useful limit.
- SQLite settings where the WebAssembly build's compile-time defaults differ
  from CPython's build are reset per connection to CPython's: double-quoted
  string literals allowed, `recursive_triggers` off, `cache_size` -2000,
  `page_size` 4096 for new files. The build has no FTS3/FTS4 or geopoly.
- `fsync` is libuv's: on macOS that is `F_FULLFSYNC`, so every commit
  flushes the drive's cache (about 10 ms here; native SQLite does the same
  only with `PRAGMA fullfsync`). On Linux it is `fsync(2)`.

Errors are reproduced as Python's `sqlite3` raises them: the same exception
class in `cause.type`, the same `sqlite_errorname` as `code`, and the
module's own `ProgrammingError` texts (binding counts, one statement at a
time, `Could not decode to UTF-8 ...`).

### Rust DuckDB worker (`duckdb-rs-worker/`)

`dblayer.backends.duckdb_rsworker.DuckDBRustWorker`: the DuckDB engine of
the Python worker (`DuckDBWorker`), as a Rust program linked against
libduckdb 2.0.1. One process owns one database file (`DatabaseWorker`
placement) and every connection of it.

- `hello` adds `"implementation": "rust"`, `"ops": false`,
  `"native_ops": ["append"]`, `"concurrent": true` and `duckdb_version`
  (no `sqlite_version` or `python`).
- `"concurrent": true`: each `open` starts a thread that owns the new
  connection and runs its requests in arrival order; requests for
  different connections run at the same time, so the parent sends them
  without waiting for earlier replies (`ConcurrentWorkerProcess`). `cancel`
  and `more` are acted on by the main thread at once. Replies are written
  whole, under one lock.
- `op` supports one operation, `append` (`args`: `table`, `columns`,
  `rows` as lists in column order), which inserts through DuckDB's
  appender. With `"check": true` the worker first looks the table's
  columns up and replies `{"result": {"declined": true}}` if the table or
  any of `columns` is missing; the parent's `insert_rows` sends that first
  (one round trip per batch) and, on a decline, creates or alters the table
  with the default code, then appends. Any other `op` gets `NotSupported`.
- Every connection is configured with `streaming_buffer_size = '16KB'`
  before the database's configuration is locked (the setting is
  per-connection and cannot be a database default), so streamed results
  arrive as soon as the first chunk exists; with DuckDB's default (10 MB)
  the C API's streaming execute waits until the buffer is about half full,
  which for an unbounded recursive CTE is seconds. Up to 8 such
  connections are made per database and reused; a connection beyond those
  has DuckDB's default.
- `open` accepts any engine name containing `duckdb` (the parent sends
  `dblayer.backends.duckdb_rsworker:engine`); its `config` is
  `DuckDB.__init__`'s keyword arguments.
- Results, column type names, value types and errors are those of the
  Python worker: values are converted as the `duckdb` Python client
  converts them, then encoded as `protocol.py` encodes those objects
  (`TIMESTAMP WITH TIME ZONE` is sent as the UTC instant with the session's
  `TimeZone`, which the parent applies). Errors carry the Python client's
  exception class in `cause.type` (`_duckdb.CatalogException`) and its
  messages for parameter binding.

### C++ DuckDB worker (`duckdb-cpp-worker/`)

`dblayer.backends.duckdb_cppworker.DuckDBCppWorker`: the same engine as a
C++17 program on libduckdb 2.0.1's C API (`duckdb-cpp-worker/README.md`),
`DatabaseWorker` placement.

- `hello` adds `"implementation": "cpp"`, `"concurrent": true` (as for the
  Rust worker: a thread per connection, the parent multiplexes) and
  `duckdb_version`, and `"ops": ["insert_rows"]`: a list names the named
  operations the worker runs itself. For any other operation the parent
  runs the default implementation; the worker may also answer an `op` with
  `{"result": {"declined": true}}` (ignore/replace, a table to create or
  alter), and the parent then runs the default.
- `open` accepts any engine name containing `duckdb` (the parent sends
  `dblayer.backends.duckdb_cppworker:LocalDuckDB`).
- Results, column type names, value types and errors match in-process
  DuckDB (`tests/worker/test_duckdb_cppworker.py` checks them
  differentially). With `max_rows`, a SELECT runs as `SELECT * FROM (sql)
  LIMIT max_rows + 1` when that keeps its column names.

## Export (C worker only)

`export` serialises a read's rows in the worker, as
`docs/export-formats.md` defines them (`encoding`: `csv`, `tsv`,
`json-objects`, `json-arrays`, `ndjson`; row bytes only, the parent
writes the header row or JSON envelope). Code: `cworker/src/export.c`;
parent side `dblayer/backends/_cworker_export.py` and
`cworker_handoff.py`. `hello` says what the worker can do:
`"export": {"formats": ["csv", "tsv", "json"], "transports": [...]}`.

| `transport` | Data | Control replies |
| --- | --- | --- |
| `frames` (default) | codec frames `{"id", "data": <bin>, "rows": 1}` (`{"$b": base64}` under JSON), up to `credit` of them, then a pause until `more`, as for `stream` | first `{"id", "columns"}`; last `{"id", "end": true, "row_count", "truncated"}` |
| `raw` | frames whose payload is `0x00`, the request id (8 bytes, big endian) and the bytes; never decoded by the codec; the same credit rule | as `frames` |
| `fifo` | written to the named pipe `data_path`, which the parent created and holds open (read end, plus a write end of its own so the pipe never reports end of file); starts with a 4-byte length and a JSON array of column names. Writes are at most 8 KB: on macOS a blocking write of 16 KB or more to a FIFO read through kqueue can stall for good (`scripts/macos_fifo_stall.py`) | one reply, at the end: `result` with `row_count`, `truncated`; the worker closes the pipe before sending it |
| `socket` | the worker connects to the Unix socket `handoff_path`, receives one descriptor (`SCM_RIGHTS`: the HTTP client's connection) and writes the rows to it as HTTP/1.1 chunks, never the terminating chunk; it leaves `O_NONBLOCK` alone (it belongs to the open file description it shares with the parent) and waits out `EAGAIN` with `poll`, checking for `cancel` every 100 ms; `EPIPE` is an error, not a signal | `{"id", "columns", "rows": 0}` before it connects; `{"id", "end": true, "row_count", "truncated", "bytes"}` after it has closed its copy of the descriptor. The parent waits for the end with a `more` the worker ignores, registered before it hands the descriptor over |

`cancel` of a paused `frames`/`raw` export ends it with `{"end": true,
"cancelled": true}` as for a stream; a running one (any transport) stops
between rows (or while waiting to write) with `QueryInterrupted`. An error
after data was sent is an error reply like any other; the bytes already
delivered stay delivered. The export runs on the connection's own
statement machinery (time limit through the progress handler, a read
connection opened read-only), and a statement that is not read-only is
refused before it steps.

## Limits, kills and deadlines

| Limit | In the worker (soft) | In the parent (hard) |
| --- | --- | --- |
| `time_limit_ms` | The engine's limit (SQLite: progress handler), backed by a timer that cancels the request at the limit (SQLite: `interrupt()` until the statement returns) | SIGKILL when no reply came within the limit plus `kill_grace_ms` (250 ms); the call raises `QueryInterrupted`, other calls on that worker `WorkerCrashed` |
| Memory | SQLite engine: `PRAGMA hard_heap_limit` | `RLIMIT_AS` set by the worker on itself (Linux) before any database is opened |
| Result size | `max_result_bytes` stops fetching; reply over `max_frame_bytes` refused | Frame size cap on the parent's reader |
| SQL complexity | SQLite engine: `Connection.setlimit()` from `sql_limits` | none |
| Transaction deadline | The worker's own timer: cancels a request running in the transaction (`TransactionExpired`), rolls back, sends `expired` | SIGKILL at the deadline plus `kill_grace_ms` plus `deadline_slack_ms` (250 ms) unless the end of the transaction was confirmed (a `commit`/`rollback` reply, or `expired`) |
| Cancelled caller | `cancel` interrupts the request | the parent waits up to `kill_grace_ms` for the reply, then kills |
| Parent gone | end of input on stdin: cancel everything, roll back, close, exit (after 5 s, exit regardless) | |

Some statements spend seconds inside one C function call (`instr`, `LIKE`
or `GLOB` with a long literal pattern on a long string, `randomblob(1e9)`,
`printf('%*d', 1e9, 1)`), where neither the progress handler nor `interrupt()`
can reach them; only the parent's SIGKILL stops them (the Node worker
stops them itself by terminating the statement's thread, then exits). See
`tests/faults/test_worker_time.py`.

## Example session

<!-- recorded-session:start -->
Recorded from the write worker of one database (JSON codec), then from a
read worker serving a stream. Generated by `uv run python -m
tests.worker.protocol_session --write`; pids and versions normalised.

```
→ {"type": "hello", "protocol": 1, "codecs": ["json"], "max_frame_bytes": 268435456, "id": 1}
← {"id": 1, "type": "hello", "protocol": 1, "codec": "json", "sqlite_version": "3.x.y", "python": "3.x.y", "pid": 12345, "rlimit_as": false}
→ {"type": "open", "path": "/data/blog.db", "mode": "write", "id": 2}
← {"ok": true, "conn": 1, "id": 2}
→ {"type": "begin", "mode": "write", "deadline_ms": 5000, "id": 3}
← {"ok": true, "tx": "t1", "id": 3}
→ {"type": "op", "name": "create_table", "args": {"name": "posts", "columns": {"id": "integer", "title": "text"}, "pk": ["id"], "not_null": [], "defaults": {}, "foreign_keys": [], "if_not_exists": false}, "tx": "t1", "id": 4}
← {"result": {"rowcount": 0, "table_created": true, "columns_added": []}, "in_tx": true, "id": 4}
→ {"type": "commit", "tx": "t1", "id": 5}
← {"ok": true, "id": 5}

# insert with alter, as an interactive transaction
→ {"type": "begin", "mode": "write", "deadline_ms": 5000, "id": 6}
← {"ok": true, "tx": "t2", "id": 6}
→ {"type": "schema", "tx": "t2", "id": 7}
← {"result": {"version": 1, "tables": [{"name": "posts", "columns": [["id", "INTEGER"], ["title", "TEXT"]], "...": "..."}], "...": "..."}, "in_tx": true, "id": 7}
→ {"type": "execute", "sql": "alter table \"posts\" add column \"slug\" TEXT", "tx": "t2", "id": 8}
← {"result": {"columns": []}, "in_tx": true, "id": 8}
→ {"type": "execute_many", "sql": "insert into posts (id, title, slug) values (:id, :title, :slug)", "params_seq": [{"id": 1, "title": "One", "slug": "one"}, {"id": 2, "title": "Two", "slug": "two"}], "tx": "t2", "id": 9}
← {"result": {"rowcount": 2}, "in_tx": true, "id": 9}
→ {"type": "commit", "tx": "t2", "id": 10}
← {"ok": true, "id": 10}

# the same work as one named operation
→ {"type": "begin", "mode": "write", "deadline_ms": 5000, "id": 11}
← {"ok": true, "tx": "t3", "id": 11}
→ {"type": "op", "name": "insert_rows", "args": {"table": "posts", "pk": null, "alter": true, "ignore": false, "replace": false, "rows": [{"id": 3, "title": "Three", "slug": "three", "tags": "a"}]}, "tx": "t3", "id": 12}
← {"result": {"rowcount": 1, "table_created": false, "columns_added": ["tags"]}, "in_tx": true, "id": 12}
→ {"type": "commit", "tx": "t3", "id": 13}
← {"ok": true, "id": 13}

# an error reply
→ {"type": "execute", "sql": "insert into posts (id, title) values (1, 'dup')", "max_rows": 1000, "id": 14}
← {"id": 14, "error": {"type": "IntegrityError", "message": "UNIQUE constraint failed: posts.id", "code": "SQLITE_CONSTRAINT_PRIMARYKEY", "cause": {"type": "sqlite3.IntegrityError", "message": "UNIQUE constraint failed: posts.id"}}}

# a transaction past its deadline: the worker rolls back by itself
→ {"type": "begin", "mode": "write", "deadline_ms": 50, "id": 15}
← {"ok": true, "tx": "t4", "id": 15}
→ {"type": "execute", "sql": "insert into posts (id, title) values (9, 'late')", "tx": "t4", "id": 16}
← {"result": {"columns": [], "rowcount": 1, "last_pk": 9}, "in_tx": true, "id": 16}
→ {"type": "execute", "sql": "select 1", "tx": "t4", "id": 17}
← {"type": "expired", "conn": 1, "tx": "t4"}
→ {"type": "cancel", "target": 17}
← {"id": 17, "error": {"type": "TransactionExpired", "message": "transaction t4 passed its deadline and was rolled back"}}
→ {"type": "rollback", "tx": "t4", "id": 18}
← {"ok": true, "expired": true, "id": 18}

# a stream with credit 2, abandoned after three chunks
→ {"type": "stream", "sql": "select id, title from posts", "chunk_rows": 2, "credit": 2, "id": 4}
← {"id": 4, "columns": ["id", "title"], "rows": [[1, "post 1"], [2, "post 2"]]}
← {"id": 4, "rows": [[3, "post 3"], [4, "post 4"]]}
→ {"type": "more", "stream": 4, "credit": 2}
← {"id": 4, "rows": [[5, "post 5"], [6, "post 6"]]}
← {"id": 4, "rows": [[7, "post 7"]]}
→ {"type": "cancel", "target": 4, "id": 4}
← {"end": true, "cancelled": true, "id": 4}
```
<!-- recorded-session:end -->
