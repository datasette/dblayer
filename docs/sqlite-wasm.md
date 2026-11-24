# SQLite in WebAssembly (`SqliteWasm`)

Experimental. `dblayer.backends.sqlite_wasm.SqliteWasm` runs SQLite compiled
to WebAssembly inside this Python process, under
[wasmtime](https://wasmtime.dev/), one instance per pooled connection. The
question it answers: can a sandbox give worker-grade containment (an exact
memory cap, a hard stop for statements that never return to SQLite's VDBE)
without a process per database?

```bash
make -C cworker wasm                 # cworker/build-wasm/dblayer-sqlite.wasm
uv run --extra wasm --extra msgpack python -c "..."
```

## What is in the module

`cworker/wasm.mk` compiles the pinned SQLite 3.53.1 amalgamation (the one the
C worker's Makefile fetches and checksums) and the C worker's own sources
(`worker.c`, `codec.c`, `schema.c`, `sqlutil.c`) for `wasm32-wasip1` with a
pinned wasi-sdk (34, downloaded into `cworker/vendor/` and checked against
the SHA-256 GitHub publishes), plus two files written for it:

* `src/wasm_vfs.c`: SQLite's only VFS (`SQLITE_OS_OTHER`). Every file
  operation and lock is a host function (`src/wasm_host.h`, module
  `dblayer`). Version-1 I/O methods: no `xShmMap`, no mmap.
* `src/wasm_libc.c`: a 64-byte-per-iteration SIMD `memcmp` in place of
  wasi-libc's byte loop (see "Costs").

`worker.c` built with `-DCWORKER_WASM` has no threads and no stdin/stdout:
the host writes one request frame's payload into linear memory and calls
`dbl_call(n)`; every frame the request produced (the reply, stream chunks,
an `expired` notice) is in an output buffer when it returns. So the
statement handling, result encoding (MessagePack or JSON), CPython-compatible
errors, schema reading and transaction bookkeeping are the C worker's,
unchanged, and the parent side is the worker backend's `WorkerConnection` and
`WorkerStream`, talking to a `WasmProcess` instead of a `WorkerProcess`.

The only WASI imports left are clocks, randomness, `fd_write` (stderr) and
`proc_exit`. There is no preopened directory: the instance cannot see the
filesystem except through the host functions, which allow exactly its
database file, that file's `-journal` and anonymous temporary files.
`ATTACH 'other.db'` and `VACUUM INTO` fail with `SQLITE_CANTOPEN`.

## Locking without WASI locks

WASI has no `fcntl()` byte-range locks and no shared memory, and the pool
opens up to four instances per database (three readers, one writer), each
with its own linear memory. Several instances opening one file without
locks is unsafe; so is anything that ignores other processes.

**Chosen: host-implemented locks with real POSIX record locks.** The VFS's
`xLock`, `xUnlock` and `xCheckReservedLock` call Python, which runs SQLite's
own `unixLock`/`posixUnlock` logic, line for line, against the same bytes
the native unix VFS locks (PENDING at 1 GB, RESERVED after it, a 510-byte
SHARED range). POSIX locks belong to a process, so, as SQLite's
`unixInodeInfo` does, the host keeps one table per inode: instances in this
process are counted against each other there, and the strongest lock any
of them holds is the one taken from the kernel. The host owns every
descriptor, and the database file has exactly one per process, closed only
when no instance has it open: closing *any* descriptor of a file drops all
of the process's locks on it, which is also why the files cannot go through
wasmtime's own WASI filesystem (it would close descriptors behind the lock
table's back when a store is dropped).

Proved by `tests/faults/test_wasm_locking.py`:

* Readers in instances see no torn state while another process (plain
  `sqlite3`) commits 400 transactions that rewrite every page of two tables;
  the negative control, the same test with the host lock functions turned
  into no-ops, catches torn or failed reads in about 60% of reads.
* Writers on both sides lose no update; locks are visible both ways (a
  transaction in an instance makes `BEGIN IMMEDIATE` elsewhere fail with
  "database is locked"; another process's `BEGIN EXCLUSIVE` makes an
  instance's read fail with `SQLITE_BUSY` after its busy timeout).
* A process using `SqliteWasm`, SIGKILLed with a write transaction half done
  (pages already in the file, a hot journal beside it): the next opener,
  WebAssembly or native, rolls it back; `integrity_check` is ok and no lock
  is left behind. A trapped instance (time limit) inside a write
  transaction behaves the same way within one process.
* Two instances in one process: a read transaction holds off a writer's
  commit until it ends, and keeps its snapshot.
* The other writer may also be another backend's worker (the C worker) or
  the `sqlite3` command-line shell: no torn reads.
* Another process switching an open database to WAL: the instance's next
  statement fails with `SQLITE_CANTOPEN` (it cannot follow without shared
  memory), the file stays intact and the other process carries on.

What it costs and gives up:

* **No WAL.** The wal-index lives in shared memory that every connection
  maps; separate linear memories cannot share it. The VFS has no `xShmMap`,
  so `PRAGMA journal_mode = WAL` stays `delete`, and a file another program
  put in WAL mode cannot be opened (`SQLITE_CANTOPEN`). Rollback-journal
  locking is what in-process SQLite uses by default here, so readers and
  the writer still run concurrently, with the usual rollback-mode rule that
  a commit waits for readers to finish.
* **No memory-mapped I/O**, and every page read or write is a call into
  Python (about 1 to 2 us each). A one-row read on a warm connection makes
  seven host calls: lock, two journal checks, the change counter, the file
  size, unlock.
* **Ordinary SQLite in the same process** (the in-process backend,
  `sqlite3`) using the same file at the same time: safe on Linux and macOS.
  The host takes open file description locks (`F_OFD_SETLK`; on macOS the
  kernel has them, with Linux's semantics, but its header defines the
  commands only under `PRIVATE`, so they are probed at import), which
  conflict with ordinary SQLite's classic POSIX locks even in this process
  and are not dropped when some other descriptor of the file closes. The
  other direction needs care too: closing *any* descriptor of a file drops
  every classic lock this process holds on it, so the host finds a file it
  already has open by `stat()` instead of opening another descriptor, and
  parks a descriptor it would close while anyone holds a lock on the file
  (`F_OFD_GETLK` over the whole file sees this process's classic locks
  too), closing it later. Tests: sqlite3 on a thread of this process
  committing while instances read, the in-process backend and instances
  writing the same file, and new instances opening and closing the file
  while sqlite3 here holds EXCLUSIVE. Elsewhere, or with
  `DBLAYER_WASM_POSIX_LOCKS=1` (classic locks), this case is unsafe: the
  rule SQLite documents for two copies of the library in one process.
  Other processes are fine either way.

Alternatives considered: one instance per database serving all its
connections (no locking problem inside the process, but every statement of
a database queues behind the slowest, and other processes still need
locks); `locking_mode=EXCLUSIVE` with a heap-memory wal-index (WAL back, but
the file is closed to every other connection and process); dot-file locking
(only SQLite processes using the same dot-file VFS would honour it).

## Limits

| Limit | Soft (instance carries on) | Hard |
| --- | --- | --- |
| `time_limit_ms` | The progress handler (every 1,000 opcodes, or every one for limits up to 20 ms) stops the statement at the limit: `QueryInterrupted` | wasmtime epoch interruption: `kill_grace_ms` (default 50) after the limit the instance traps; the call raises `QueryInterrupted`, the instance is discarded, the connection gets a new one on its next request |
| Cancelled caller | The host writes a flag into the instance's memory that the progress handler and every statement start read: never lost | trap after `kill_grace_ms` |
| Memory | `memory_limit_mb` caps linear memory (`Store.set_limits`); SQLite's heap limit is set about 12% below, so a large allocation is `SQLITE_NOMEM` (`ResourceLimitExceeded`) and the instance carries on | `memory.grow` fails at the cap; an allocation failure in the worker code itself ends the instance (`ResourceLimitExceeded`) |
| Transaction deadline | Nothing runs between calls, so the host pokes the instance (`dbl_call(0)`) at the deadline: it rolls back and sends `expired`. A statement running at the deadline is stopped by its progress handler | the worker backend's own kill timer |

The epoch ticker is one thread, running only while some call is in flight,
that advances the engine's epoch every 2 ms. Each instance has an epoch
callback that checks its deadlines and kill flag; returning an error from
it is the trap.

**`hard_kill=True`**: there is no process to kill, but a statement that
ignores interrupts is stopped by trapping its instance, which is what the
flag promises callers (the queries from `notes.md` that only SIGKILL stopped
in a worker are stopped about 50 ms after their limit). **`memory_limit=True`**
on every platform, unlike the worker (Linux only).

Linear memory never shrinks. An instance that once held a big value keeps
that memory, so an idle instance whose memory has grown past
`recycle_memory_mb` (default 64) is replaced before its next request,
outside a transaction (a new instance and the same `open`), and a discarded
store is freed at once. Memory used inside an instance is this process's
memory: the cap bounds it, it does not hide it.

## Choices

* **Epoch interruption, not fuel.** Both cost roughly the same on
  CPU-bound statements in the benchmark (epoch 10 to 30% over no
  interruption at all, fuel the same or a little more, at high machine
  load), but only epochs can be driven from another thread: fuel cannot be
  topped up or revoked while a call runs, so it cannot implement
  cancellation, and a fuel budget is a count of instructions, not time.
* **One instance per pooled connection** (dedicated placement only). The C
  code can serve several connections in one instance; that would serialise
  a database's statements.
* **Named operations run in the parent** (the default path), as for the C
  worker: `hello` says `"ops": false`.
* **The raw wasmtime C API on the hot path.** wasmtime-py's `Func` wrappers
  allocate type objects on every guest call and every host call, under the
  GIL: with eight busy threads a 300-database load ran 381 requests/s. Host
  imports are now unchecked callbacks on one shared linker with an
  `InstancePre` (the running instance found through a thread-local), and a
  request is one `wasmtime_func_call_unchecked`; the same load runs about
  1,700 requests/s. The GIL is released while the guest runs.
* **The compiled module is cached** next to the `.wasm` file
  (`<module>.<wasmtime version>.<flavour>.<size>.<mtime>.cwasm`): compiling
  takes about 150 ms, loading the cache about 10 ms, once per process.

## Inline reads

`SqliteWasm(path, inline_reads=True)` (off by default; the suite runs with it
under `DBLAYER_WASM_INLINE=1`) runs a read on a read connection on the
event loop's thread, with no executor hop, as `SqliteApsw(inline_reads=True)`
does. The epoch ticker bounds it: past `INLINE_S` (2 ms) the progress
handler is told to stop and the read runs again from the start on the
executor, where its own time limit applies; a statement that ignores the
interrupt (one long C call) is trapped `INLINE_GRACE_S` (4 ms) later, the
instance replaced and the read run on the executor. The longest event-loop
stall measured for an evading query was about 5 ms. A one-row read drops
from about 110-185 us to about 25 us (`wasm-inline` in the benchmark), close
to apsw inline's 10-15 us; the instance call itself is about 19 us, of
which the six host calls are about 9.

## Costs

`benchmarks/wasm_overhead.py`; see the notes for measured figures. Broadly:
a new database answers its first query in about 1 ms (instantiation is
about 50 us); a one-row read costs about 13 us inside the instance (the
rest is the same dblayer and executor-hop cost as in-process); CPU-bound
SQL runs 1.5 to 2 times slower than native. `memcmp`-bound SQL (`instr`,
`LIKE` on long strings) ran 16 times slower with wasi-libc's byte loop,
hence the SIMD `memcmp`.
