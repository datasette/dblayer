#!/usr/bin/env node
// dblayer Node worker: a drop-in replacement for `python -m dblayer.worker`
// that runs SQLite compiled to WebAssembly (the official
// @sqlite.org/sqlite-wasm build, with a file VFS of our own, vfs.mjs).
// Speaks docs/worker-protocol.md on stdin/stdout.
//
//   node nodeworker/src/main.mjs [--memory-limit-mb N] [--debug] [--version]
//
// Threads
//   main      (this file) reads and decodes frames; answers `hello`; acts on
//             `cancel` at once; routes every other request to the executor
//             of its connection, one job at a time per connection; owns the
//             timers: transaction deadlines (`expired`), and the hard stop
//             that terminates an executor whose statement does not reach a
//             progress check.
//   executor  (executor.mjs) one per connection, each with its own
//             WebAssembly instance of SQLite; writes its own replies to
//             fd 1. Connections run at the same time (as threads do for
//             in-process SQLite); the parent may have one request in flight
//             per connection. A spare executor is kept started, so an
//             `open` does not wait for a WebAssembly instance.
//
// Cancellation reaches a running statement through shared memory
// (flags[0] of its executor, read by SQLite's progress handler): 1
// cancelled, 2 time limit, 3 transaction deadline.

import fs from 'node:fs';
import { Worker } from 'node:worker_threads';
import { createRequire, enableCompileCache } from 'node:module';
import { decode, encode } from './codec.mjs';
import { writeAll, ParentGone, consoleToStderr, newWriteLock, setWriteLock } from './io.mjs';
import { newLockTable, releaseThread } from './locks.mjs';
import * as guard from './guard.mjs';

const PROTOCOL_VERSION = 1;
const DEFAULT_MAX_FRAME = 256 * 1024 * 1024;
const SHUTDOWN_GRACE_MS = 5000;
/** How long after its time limit (or a cancel, or a deadline) a statement
 * may keep running before its executor thread is terminated: well inside
 * the parent's kill_grace_ms (250 ms by default), so the worker stops it
 * first and says why. */
const HARD_STOP_MS = Number(process.env.DBLAYER_NODEWORKER_HARD_STOP_MS || 100);
// Executors started ahead of an `open`: "first" (default) starts one with the
// process, so the first connection's thread boots while Node does; "always"
// also keeps one ready for each later connection (about 20 MB idle);
// "0" starts every executor on demand.
const SPARES = process.env.DBLAYER_NODEWORKER_SPARES ?? 'first';
const TIMING = process.env.DBLAYER_NODEWORKER_TIMING === '1';
const YOUNG_MB = Number(process.env.DBLAYER_NODEWORKER_YOUNG_MB ?? 0);
const T_MAIN = performance.timeOrigin + performance.now();
// V8's code cache for the JavaScript (the SQLite module's 480 KB of glue
// above all), in the OS temp directory: about 4 ms off each start.
if (process.env.DBLAYER_NODEWORKER_COMPILE_CACHE !== '0') enableCompileCache?.();

const require = createRequire(import.meta.url);
const WASM_PACKAGE = require('@sqlite.org/sqlite-wasm/package.json').version;
const SQLITE_VERSION = WASM_PACKAGE.split('-')[0];
const WASM_PATH = require.resolve('@sqlite.org/sqlite-wasm/sqlite3.wasm');

// -- arguments --------------------------------------------------------------------

let debug = false;
let memoryLimitMb = null;
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === '--debug') debug = true;
  else if (a === '--memory-limit-mb' && i + 1 < argv.length) memoryLimitMb = parseInt(argv[++i], 10) || null;
  else if (a === '--version') {
    fs.writeSync(1, `dblayer-nodeworker protocol ${PROTOCOL_VERSION}, SQLite ${SQLITE_VERSION} (WebAssembly, @sqlite.org/sqlite-wasm ${WASM_PACKAGE}), Node ${process.version}\n`);
    process.exit(0);
  } else {
    fs.writeSync(2, 'usage: node nodeworker/src/main.mjs [--memory-limit-mb N] [--debug] [--version]\n');
    process.exit(2);
  }
}

// The parent's Ctrl-C is not ours: we exit when stdin closes.
process.on('SIGINT', () => {});
consoleToStderr();

const lockTable = newLockTable();
const writeLock = newWriteLock();
setWriteLock(writeLock);

let maxFrame = DEFAULT_MAX_FRAME;
let codec = 'json';
let exiting = false;

function exit(code) {
  if (exiting) return;
  exiting = true;
  process.exit(code);
}

function sendFrame(payload, lockTimeoutMs = Infinity) {
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32BE(payload.length);
  writeAll(Buffer.concat([hdr, payload]), { lockTimeoutMs });
}

/** An error reply from this thread. */
function replyError(id, error, extra = {}) {
  try {
    sendFrame(encode(codec, { id: id ?? null, error, ...extra }), 1000);
  } catch (e) {
    if (e instanceof ParentGone) exit(0);
    throw e;
  }
}

// -- executors -----------------------------------------------------------------------

// Compiled once; every executor instantiates the same module.
let wasmModule = null;
let T_COMPILED = 0;
const compiled = WebAssembly.compile(fs.readFileSync(WASM_PATH)).then((m) => {
  T_COMPILED = performance.timeOrigin + performance.now();
  wasmModule = m;
});

class Executor {
  constructor() {
    this.created = performance.timeOrigin + performance.now();
    this.flags = new Int32Array(new SharedArrayBuffer(16));
    this.queue = [];
    this.current = null;
    this.ready = false;
    this.conn = null; // the connection id it serves
    this.tx = null; // open transaction ("tN"), as far as the timers care
    this.timer = null; // its deadline
    this.hardStop = null;
    this.dead = false;
    this.worker = new Worker(new URL('./executor.mjs', import.meta.url), {
      workerData: {
        flags: this.flags, debug, memoryLimitMb, maxFrameBytes: maxFrame, lockTable, writeLock, wasmModule,
        keepMainFds: guard.ENABLED,
        // null while the main thread is still compiling: then it follows as a message
      },
      resourceLimits: {
        // V8's default young generation (16 MB semi-spaces) is most of an
        // idle executor's memory; this thread allocates little JavaScript
        ...(YOUNG_MB ? { maxYoungGenerationSizeMb: YOUNG_MB } : {}),
        ...(memoryLimitMb ? { maxOldGenerationSizeMb: Math.max(64, memoryLimitMb) } : {}),
      },
      // Both of the thread's streams come here and go to fd 2 with writeSync:
      // letting Node pipe them to process.stdout/stderr initialises those
      // streams, which turns fd 1 non-blocking (and replies then crawl
      // through EAGAIN). Measured on Node 24.
      stdout: true,
      stderr: true,
    });
    if (!wasmModule) compiled.then(() => this.worker.postMessage({ kind: 'module', module: wasmModule }));
    const toStderr = (d) => {
      try {
        fs.writeSync(2, d);
      } catch {
        // stderr closed
      }
    };
    this.worker.stdout.on('data', toStderr);
    this.worker.stderr.on('data', toStderr);
    this.worker.on('message', (m) => this.onMessage(m));
    this.worker.on('error', (e) => this.onError(e));
    this.worker.on('exit', (code) => {
      if (this.dead) return;
      this.dead = true;
      console.error(`dblayer-nodeworker: executor thread exited (${code})`);
      this.failCurrent({ type: 'QueryError', message: `worker error: the SQLite thread exited (${code})`, code: 'WORKER' });
      exit(70);
    });
  }

  enqueue(job, front = false) {
    if (front) this.queue.unshift(job);
    else this.queue.push(job);
    this.pump();
  }

  pump() {
    if (!this.ready || this.current || this.queue.length === 0 || this.dead) return;
    const job = (this.current = this.queue.shift());
    Atomics.store(this.flags, 0, job.cancelled ? 1 : 0);
    const m = job.msg;
    if (m && m.type === 'commit' && this.timer && (m.tx === undefined || m.tx === null || m.tx === this.tx)) {
      // A commit that has started is not cut short by the deadline.
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (m && m.type === 'execute' && typeof m.time_limit_ms === 'number') {
      this.armHardStop(job, m.time_limit_ms + HARD_STOP_MS, 'time limit');
    }
    const { msg, ...wire } = job;
    void msg;
    wire.codec = codec;
    wire.maxFrameBytes = maxFrame;
    this.worker.postMessage(wire, wire.payload ? [wire.payload.buffer] : []);
  }

  onMessage(m) {
    if (m.ready) {
      this.ready = true;
      if (m.sqliteVersion !== SQLITE_VERSION) console.error(`dblayer-nodeworker: SQLite ${m.sqliteVersion}, package says ${SQLITE_VERSION}`);
      if (TIMING) {
        // ms since the process started, for benchmarks/nodeworker_bench.py
        const rel = (t) => +(t - performance.timeOrigin).toFixed(2);
        console.error(`dblayer-nodeworker timing ${JSON.stringify({
          main_loaded: rel(T_MAIN), wasm_compiled: rel(T_COMPILED), executor_created: rel(this.created),
          thread_start: rel(m.times.thread_start), sqlite_imported: rel(m.times.imported), sqlite_ready: rel(m.times.ready),
        })}`);
      }
      this.pump();
      return;
    }
    if (!m.done) return;
    const job = this.current;
    this.current = null;
    if (this.hardStop) {
      clearTimeout(this.hardStop);
      this.hardStop = null;
    }
    if (m.txEnded || m.closed) this.endTx();
    if (m.txBegun) {
      this.endTx();
      this.tx = `t${m.txBegun.tx}`;
      if (m.txBegun.deadline_ms !== null) {
        const tx = this.tx;
        this.timer = setTimeout(() => this.onDeadline(tx), Math.max(0, m.txBegun.deadline_ms));
      }
    }
    if (m.streamOpen) streams.set(m.streamOpen.id, this);
    else if (m.streamClosed !== undefined && job && (job.kind === 'more' || job.kind === 'close_stream' || job.msg?.type === 'stream')) {
      streams.delete(m.streamClosed);
    }
    if (m.exit !== undefined) {
      exit(m.exit);
      return;
    }
    if (m.finished) {
      this.retire();
      return;
    }
    this.pump();
  }

  onError(e) {
    if (this.dead) return;
    this.dead = true;
    const oom = e && e.code === 'ERR_WORKER_OUT_OF_MEMORY';
    console.error(`dblayer-nodeworker: executor thread failed: ${e?.stack || e}`);
    this.failCurrent(
      oom
        ? { type: 'ResourceLimitExceeded', message: 'worker ran out of memory (memory limit)' }
        : { type: 'QueryError', message: `worker error: ${e?.name}: ${e?.message}`, code: 'WORKER' },
    );
    exit(oom ? 0 : 70);
  }

  /** Reply an error for the current job from the main thread (the
   * executor is gone), marked fatal: the process exits next. */
  failCurrent(error) {
    const job = this.current;
    if (!job || job.id === undefined || job.kind === 'expire' || job.kind === 'shutdown') return;
    replyError(job.id, error, { fatal: true });
  }

  endTx() {
    this.tx = null;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  /** Server._on_deadline */
  onDeadline(tx) {
    if (this.tx !== tx) return;
    this.timer = null;
    this.tx = null;
    if (this.current && this.current.kind !== 'expire') this.cancelCurrent(3);
    this.enqueue({ kind: 'expire', conn: this.conn, tx: Number(tx.slice(1)) }, true);
  }

  cancelCurrent(reason) {
    Atomics.store(this.flags, 0, reason);
    Atomics.notify(this.flags, 0); // wakes a busy-handler wait
    this.armHardStop(this.current, HARD_STOP_MS, reason === 3 ? 'transaction deadline' : 'cancel');
  }

  /** A statement that does not reach a progress check (one long C call:
   * randomblob(1e9), instr() on long strings) cannot be interrupted inside
   * SQLite. Terminating the executor thread stops even those (V8 checks
   * for termination inside WebAssembly loops). The worker then replies
   * and exits, as a kill would. */
  armHardStop(job, ms, why) {
    if (this.hardStop) clearTimeout(this.hardStop);
    this.hardStop = setTimeout(async () => {
      if (this.current !== job || this.dead) return;
      this.dead = true;
      const t0 = performance.now();
      await this.worker.terminate();
      releaseThread(lockTable, this.worker.threadId);
      console.error(`dblayer-nodeworker: ${why}: the statement did not stop; SQLite thread terminated in ${(performance.now() - t0).toFixed(1)} ms`);
      const message =
        why === 'time limit'
          ? 'query interrupted (time limit exceeded): the statement did not reach a progress check, so the worker killed its SQLite thread'
          : why === 'transaction deadline'
            ? 'transaction passed its deadline; the statement did not stop, so the worker killed its SQLite thread'
            : 'query cancelled: the statement did not stop, so the worker killed its SQLite thread';
      this.failCurrent({ type: why === 'transaction deadline' ? 'TransactionExpired' : 'QueryInterrupted', message });
      exit(0);
    }, ms);
  }

  retire() {
    if (this.conn !== null && executors.get(this.conn) === this) executors.delete(this.conn);
    for (const [sid, ex] of streams) if (ex === this) streams.delete(sid);
    if (guard.drop(this) && !exiting) {
      // Node closes the descriptors a thread opened when it exits, which
      // would drop the guard's fcntl lock: keep the thread, for the next
      // open, until the guard is released.
      this.conn = null;
      this.endTx();
      idle.push(this);
      return;
    }
    this.dead = true;
    this.worker.terminate().then(() => {
      releaseThread(lockTable, this.worker.threadId);
      maybeFinish();
    });
    // the guard is released: the threads kept for it may go too
    for (const ex of idle.splice(0)) ex.retire();
  }
}

const executors = new Map(); // conn id -> Executor
const streams = new Map(); // stream id -> Executor
let spare = null;
let connSeq = 0;

/** An executor for a new connection: the spare if one is started, and a
 * new spare behind it. */
const idle = []; // retired executors kept alive while a guard is held (see retire)

function takeExecutor() {
  const kept = idle.pop();
  if (kept) return kept;
  const ex = spare ?? new Executor();
  spare = SPARES === 'always' ? new Executor() : null;
  return ex;
}

// -- requests ------------------------------------------------------------------------

const CONN_TYPES = new Set(['close', 'execute', 'execute_many', 'batch', 'stream', 'begin', 'commit', 'rollback', 'op', 'schema', 'schema_version']);
let eof = false;
let pending = Promise.resolve(); // keeps opens in arrival order

function executorFor(msg) {
  const c = msg.conn;
  if (c === undefined || c === null) {
    if (executors.size === 1) return executors.values().next().value;
    throw { type: 'QueryError', message: `request has no 'conn' and the worker has ${executors.size} connections`, code: 'PROTOCOL' };
  }
  const ex = executors.get(c);
  if (ex) return ex;
  throw { type: 'QueryError', message: typeof c === 'number' ? `no connection ${c} in this worker` : 'no connection in this worker', code: 'PROTOCOL' };
}

function dispatch(payload) {
  const msg = decode(codec, payload);
  if (msg === null || typeof msg !== 'object' || Array.isArray(msg) || msg instanceof Uint8Array) {
    throw new Error('a message must be a map');
  }
  const type = msg.type;
  const id = msg.id;
  if (type === 'hello') return hello(msg);
  if (type === 'cancel') return cancel(msg.target);
  if (type === 'more') {
    const ex = streams.get(msg.stream);
    if (ex) ex.enqueue({ kind: 'more', id: msg.stream, stream: msg.stream, credit: typeof msg.credit === 'number' ? msg.credit : 1 });
    return;
  }
  const job = { kind: 'req', id, msg, payload };
  if (type === 'open') {
    const connId = ++connSeq;
    job.connId = connId;
    pending = pending.then(async () => {
      const ex = await takeExecutor();
      const refused = await guard.acquire(msg.path, msg.config);
      if (refused) {
        if (spare === null) spare = ex;
        else ex.retire();
        replyError(id, refused);
        return;
      }
      guard.hold(msg.path, ex); // no await since acquire: the guard is still held
      ex.conn = connId;
      executors.set(connId, ex);
      ex.enqueue(job);
    });
    return;
  }
  if (type === 'sleep') {
    pending = pending.then(() => sleep(id));
    return;
  }
  if (type === '_debug' && debug) {
    pending = pending.then(async () => {
      let ex = executors.get(msg.conn) ?? (executors.size === 1 ? executors.values().next().value : null);
      if (!ex) {
        ex = await takeExecutor();
        ex.conn = -1;
      }
      ex.enqueue(job);
    });
    return;
  }
  if (!CONN_TYPES.has(type)) {
    const t = typeof type === 'string' ? `'${type.slice(0, 64)}'` : type === undefined || type === null ? 'None' : String(type);
    replyError(id, { type: 'QueryError', message: `unknown message type ${t}`, code: 'PROTOCOL' });
    return;
  }
  pending = pending.then(() => {
    let ex;
    try {
      ex = executorFor(msg);
    } catch (err) {
      replyError(id, err);
      return;
    }
    // A stream is routable from now, not only once its executor reports
    // it open: the executor writes the first chunk to fd 1 itself, so the
    // parent's `more` (or `cancel`) can arrive on stdin before that report
    // is handled here, and was then dropped: the parent waited for a
    // chunk that never came (seen as a hang on Linux CI). The report of a
    // stream that ended or failed removes the entry again.
    if (type === 'stream') streams.set(id, ex);
    ex.enqueue(job);
  });
}

function cancel(target) {
  pending = pending.then(() => {
    for (const ex of executors.values()) {
      if (ex.current && ex.current.id === target && ex.current.kind !== 'expire') {
        ex.cancelCurrent(1);
        break;
      }
      const queued = ex.queue.find((j) => j.id === target && j.kind !== 'expire');
      if (queued) {
        queued.cancelled = true;
        break;
      }
    }
    // An open stream is also closed, whether or not a job was found (as
    // in the C worker): the job that last pumped it stays `current` until
    // its executor's report is handled, so a cancel arriving after its
    // last frame only marked a finished job and the stream stayed open
    // with no reply. A job still pumping fails and drops the stream first;
    // this close then finds nothing and sends nothing.
    const ex = streams.get(target);
    if (ex) ex.enqueue({ kind: 'close_stream', id: target, stream: target });
  });
}

function hello(msg) {
  const wanted = Array.isArray(msg.codecs) ? msg.codecs : ['json'];
  const chosen = wanted.find((c) => c === 'json' || c === 'msgpack') ?? 'json';
  if (typeof msg.max_frame_bytes === 'number' && msg.max_frame_bytes > 0) maxFrame = msg.max_frame_bytes;
  sendFrame(
    encode('json', {
      id: msg.id ?? null,
      type: 'hello',
      protocol: PROTOCOL_VERSION,
      codec: chosen,
      sqlite_version: SQLITE_VERSION,
      implementation: 'node',
      node: process.versions.node,
      wasm: `@sqlite.org/sqlite-wasm ${WASM_PACKAGE}`,
      pid: process.pid,
      rlimit_as: false,
      ops: false,
      // requests for different connections run at the same time (a thread
      // each): the parent may send one per connection without waiting
      concurrent: true,
      guard: guard.ENABLED,
    }),
  ); // in JSON; everything after it in the agreed codec
  codec = chosen; // executors get it (and maxFrame) with every job
}

/** `sleep`: close every connection, reply, exit. */
async function sleep(id) {
  await shutdownAll();
  try {
    sendFrame(encode(codec, { id: id ?? null, ok: true }));
  } catch {
    // parent gone
  }
  exit(0);
}

let finishing = null;
function maybeFinish() {
  if (finishing && executors.size === 0) finishing();
}

function shutdownAll() {
  return new Promise((resolve) => {
    finishing = resolve;
    for (const ex of executors.values()) ex.enqueue({ kind: 'shutdown' });
    maybeFinish();
  });
}

function onEof() {
  if (eof) return;
  eof = true;
  process.stdin.pause();
  // Exit even if a statement ignores every interrupt.
  setTimeout(() => exit(0), SHUTDOWN_GRACE_MS);
  pending.then(() => {
    for (const ex of executors.values()) {
      for (const j of ex.queue) j.cancelled = true;
      if (ex.current) {
        Atomics.store(ex.flags, 0, 1);
        Atomics.notify(ex.flags, 0);
      }
    }
    return shutdownAll();
  }).then(() => exit(0));
}

// -- input ---------------------------------------------------------------------------

let chunks = [];
let have = 0;
let need = -1;

function take(n) {
  const buf = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, have);
  const out = buf.subarray(0, n);
  const rest = buf.subarray(n);
  chunks = rest.length ? [rest] : [];
  have = rest.length;
  return out;
}

process.stdin.on('data', (chunk) => {
  if (eof) return;
  chunks.push(chunk);
  have += chunk.length;
  try {
    for (;;) {
      if (need < 0) {
        if (have < 4) break;
        const n = take(4).readUInt32BE(0);
        if (n > maxFrame) {
          console.error(`dblayer.worker: frame of ${n} bytes exceeds the ${maxFrame}-byte frame size cap; dropping the connection`);
          onEof();
          return;
        }
        need = n;
      }
      if (have < need) break;
      // its own ArrayBuffer, so it can be transferred to an executor
      const payload = new Uint8Array(take(need));
      need = -1;
      dispatch(payload);
    }
  } catch (e) {
    if (e instanceof ParentGone) exit(0);
    console.error(`dblayer.worker: bad input (${e?.message || e}); dropping the connection`);
    onEof();
  }
});
process.stdin.on('end', onEof);
process.stdin.on('error', onEof);

if (SPARES !== '0') spare = new Executor(); // starts now, while the module compiles
