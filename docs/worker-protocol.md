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
SQLite. Where it differs is listed under "Implementations" below.

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
| `json` (stdlib) | `{"$b": "<base64>"}` | exact (both ends are Python) | `NaN`, `Infinity` (Python extensions) | A user dict whose only key is `$b`, `$d` or `$x` is escaped as `{"$d": [key, value]}`. |
| `msgpack` (optional extra) | native `bin` | native up to 64 bits; larger as ext type 1 (decimal text) | native | |

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
(so a time limit is measured from when the request is sent). `cancel` and
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

Errors are reproduced as Python's `sqlite3` raises them: the same exception
class in `cause.type`, the same `sqlite_errorname` as `code`, and the
module's own `ProgrammingError` texts (binding counts, one statement at a
time, `Could not decode to UTF-8 ...`).

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
can reach them; only the parent's SIGKILL stops them. See
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
