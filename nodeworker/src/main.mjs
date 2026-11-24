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
//             `cancel` at once; queues everything else and hands the
//             executor one job at a time; owns the timers: transaction
//             deadlines (`expired`), and the hard stop that terminates the
//             executor when a statement does not reach a progress check.
//   executor  (executor.mjs) SQLite and every request that touches it; it
//             writes its own replies to fd 1.
//
// Cancellation reaches a running statement through shared memory
// (flags[0], read by SQLite's progress handler): 1 cancelled, 2 time
// limit, 3 transaction deadline.

import fs from 'node:fs';
import { Worker } from 'node:worker_threads';
import { createRequire } from 'node:module';
import { decode, encode } from './codec.mjs';
import { writeAll, ParentGone, consoleToStderr } from './io.mjs';

const PROTOCOL_VERSION = 1;
const DEFAULT_MAX_FRAME = 256 * 1024 * 1024;
const SHUTDOWN_GRACE_MS = 5000;
/** How long after its time limit (or a cancel) a statement may keep the
 * executor busy before the executor thread is terminated: well inside the
 * parent's kill_grace_ms (250 ms by default), so the worker stops it first
 * and says why. */
const HARD_STOP_MS = Number(process.env.DBLAYER_NODEWORKER_HARD_STOP_MS || 100);

const require = createRequire(import.meta.url);
const WASM_PACKAGE = require('@sqlite.org/sqlite-wasm/package.json').version;
const SQLITE_VERSION = WASM_PACKAGE.split('-')[0];

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

// -- the executor thread ---------------------------------------------------------

const flags = new Int32Array(new SharedArrayBuffer(16));
let maxFrame = DEFAULT_MAX_FRAME;
let codec = 'json';
let ready = false;
let executor = null;
let terminating = false;

function startExecutor() {
  const resourceLimits = memoryLimitMb ? { maxOldGenerationSizeMb: Math.max(64, memoryLimitMb) } : undefined;
  const w = new Worker(new URL('./executor.mjs', import.meta.url), {
    workerData: { flags, debug, memoryLimitMb, maxFrameBytes: maxFrame },
    resourceLimits,
    stdout: true, // the worker's stdout is not our fd 1
    stderr: false,
  });
  w.stdout.on('data', (d) => process.stderr.write(d));
  w.on('message', onExecutorMessage);
  w.on('error', onExecutorError);
  w.on('exit', (code) => {
    if (!terminating) {
      console.error(`dblayer-nodeworker: executor thread exited (${code})`);
      failCurrent({ type: 'QueryError', message: `worker error: the SQLite thread exited (${code})`, code: 'WORKER' }, true);
      exit(70);
    }
  });
  return w;
}

function onExecutorError(e) {
  if (terminating) return;
  terminating = true;
  const oom = e && e.code === 'ERR_WORKER_OUT_OF_MEMORY';
  console.error(`dblayer-nodeworker: executor thread failed: ${e?.stack || e}`);
  failCurrent(
    oom
      ? { type: 'ResourceLimitExceeded', message: 'worker ran out of memory (memory limit)' }
      : { type: 'QueryError', message: `worker error: ${e?.name}: ${e?.message}`, code: 'WORKER' },
    true,
  );
  exit(oom ? 0 : 70);
}

/** Reply an error for the current job from this thread (the executor is
 * gone, so nothing else is writing). */
function failCurrent(error, fatal) {
  const job = current;
  if (!job || job.id === undefined || job.kind === 'expire' || job.kind === 'shutdown') return;
  const msg = { id: job.id, error };
  if (fatal) msg.fatal = true;
  try {
    sendFrame(encode(codec, msg));
  } catch {
    // parent gone
  }
}

function exit(code) {
  try {
    fs.fsyncSync(2);
  } catch {
    // not a file
  }
  process.exit(code);
}

function sendFrame(payload) {
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32BE(payload.length);
  writeAll(Buffer.concat([hdr, payload]));
}

// -- jobs ----------------------------------------------------------------------------

const queue = [];
let current = null;
let hardStop = null;
const conns = new Map(); // conn id -> { tx, timer }
const streams = new Map(); // stream id -> conn id
let eof = false;

function enqueue(job, front = false) {
  if (front) queue.unshift(job);
  else queue.push(job);
  pump();
}

function jobConn(job) {
  if (job.kind === 'more' || job.kind === 'close_stream') return streams.get(job.stream);
  const c = job.msg?.conn;
  if (c === undefined || c === null) return conns.size === 1 ? conns.keys().next().value : undefined;
  return c;
}

function pump() {
  if (!ready || current || queue.length === 0 || terminating) return;
  const job = (current = queue.shift());
  Atomics.store(flags, 0, job.cancelled ? 1 : 0);
  const m = job.msg;
  if (m && m.type === 'commit') {
    // A commit that has started is not cut short by the deadline.
    const c = conns.get(jobConn(job));
    if (c && c.timer && (m.tx === undefined || m.tx === null || m.tx === c.tx)) {
      clearTimeout(c.timer);
      c.timer = null;
    }
  }
  if (m && m.type === 'execute' && typeof m.time_limit_ms === 'number') {
    armHardStop(job, m.time_limit_ms + HARD_STOP_MS, 'time limit');
  }
  const { msg, ...wire } = job;
  void msg;
  executor.postMessage(wire, wire.payload ? [wire.payload.buffer] : []);
}

function onExecutorMessage(m) {
  if (m.ready) {
    ready = true;
    if (m.sqliteVersion !== SQLITE_VERSION) console.error(`dblayer-nodeworker: SQLite ${m.sqliteVersion}, package says ${SQLITE_VERSION}`);
    pump();
    return;
  }
  if (!m.done) return;
  const job = current;
  current = null;
  if (hardStop) {
    clearTimeout(hardStop);
    hardStop = null;
  }
  if (m.opened) conns.set(m.opened, { tx: null, timer: null });
  if (m.closed) {
    endTx(m.closed);
    conns.delete(m.closed);
    for (const [sid, c] of streams) if (c === m.closed) streams.delete(sid);
  }
  if (m.txEnded) endTx(m.txEnded);
  if (m.txBegun) {
    const c = conns.get(m.txBegun.conn);
    if (c) {
      endTx(m.txBegun.conn);
      c.tx = `t${m.txBegun.tx}`;
      if (m.txBegun.deadline_ms !== null) {
        const tx = c.tx;
        c.timer = setTimeout(() => onDeadline(m.txBegun.conn, tx), Math.max(0, m.txBegun.deadline_ms));
      }
    }
  }
  if (m.streamOpen) streams.set(m.streamOpen.id, m.streamOpen.conn);
  else if (m.streamClosed !== undefined && job && (job.kind === 'more' || job.kind === 'close_stream' || job.msg?.type === 'stream')) {
    streams.delete(m.streamClosed);
  }
  if (m.exit !== undefined) exit(m.exit);
  pump();
}

function endTx(connId) {
  const c = conns.get(connId);
  if (!c) return;
  c.tx = null;
  if (c.timer) clearTimeout(c.timer);
  c.timer = null;
}

/** Server._on_deadline */
function onDeadline(connId, tx) {
  const c = conns.get(connId);
  if (!c || c.tx !== tx) return;
  c.timer = null;
  c.tx = null;
  if (current && current.kind !== 'expire' && jobConn(current) === connId) cancelCurrent(3);
  enqueue({ kind: 'expire', conn: connId, tx: Number(tx.slice(1)) }, true);
}

function cancelCurrent(reason) {
  Atomics.store(flags, 0, reason);
  Atomics.notify(flags, 0); // wakes a busy-handler wait
  armHardStop(current, HARD_STOP_MS, reason === 3 ? 'transaction deadline' : 'cancel');
}

/** A statement that does not reach a progress check (one long C call:
 * randomblob(1e9), instr() on long strings) cannot be interrupted inside
 * SQLite. Terminating the executor thread stops even those (V8 checks for
 * termination inside WebAssembly loops); every connection in this process
 * is lost with it, so reply and exit, as a kill would. */
function armHardStop(job, ms, why) {
  if (hardStop) clearTimeout(hardStop);
  hardStop = setTimeout(async () => {
    if (current !== job || terminating) return;
    terminating = true;
    const t0 = performance.now();
    await executor.terminate();
    console.error(`dblayer-nodeworker: ${why}: the statement did not stop; SQLite thread terminated in ${(performance.now() - t0).toFixed(1)} ms`);
    const message =
      why === 'time limit'
        ? 'query interrupted (time limit exceeded): the statement did not reach a progress check, so the worker killed its SQLite thread'
        : why === 'transaction deadline'
          ? 'transaction passed its deadline; the statement did not stop, so the worker killed its SQLite thread'
          : 'query cancelled: the statement did not stop, so the worker killed its SQLite thread';
    failCurrent({ type: why === 'transaction deadline' ? 'TransactionExpired' : 'QueryInterrupted', message }, true);
    exit(0);
  }, ms);
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
      // its own ArrayBuffer, so it can be transferred to the executor
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

function dispatch(payload) {
  const msg = decode(codec, payload);
  if (msg === null || typeof msg !== 'object' || Array.isArray(msg) || msg instanceof Uint8Array) {
    throw new Error('a message must be a map');
  }
  const type = msg.type;
  const id = msg.id;
  if (type === 'hello') return hello(msg);
  if (type === 'cancel') {
    const target = msg.target;
    const pending = queue.find((j) => j.id === target && j.kind !== 'expire');
    if (current && current.id === target && current.kind !== 'expire') cancelCurrent(1);
    else if (pending) pending.cancelled = true;
    else if (streams.has(target)) enqueue({ kind: 'close_stream', id: target, stream: target });
    return;
  }
  if (type === 'more') {
    const credit = typeof msg.credit === 'number' ? msg.credit : 1;
    enqueue({ kind: 'more', id: msg.stream, stream: msg.stream, credit });
    return;
  }
  enqueue({ kind: 'req', id, msg, payload, codec });
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
    }),
  ); // in JSON; everything after it in the agreed codec
  codec = chosen;
  executor.postMessage({ kind: 'codec', codec: chosen, maxFrameBytes: maxFrame });
}

function onEof() {
  if (eof) return;
  eof = true;
  process.stdin.pause();
  for (const j of queue) j.cancelled = true;
  if (current) Atomics.store(flags, 0, 1);
  queue.push({ kind: 'shutdown' });
  pump();
  // Exit even if a statement ignores every interrupt.
  setTimeout(() => exit(0), SHUTDOWN_GRACE_MS);
}

executor = startExecutor();
