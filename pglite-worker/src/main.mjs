#!/usr/bin/env node
// dblayer PGlite worker: PostgreSQL compiled to WebAssembly (PGlite), one
// data directory, in a Node.js process. Speaks docs/worker-protocol.md on
// stdin/stdout.
//
//   node [--liftoff-only] pglite-worker/src/main.mjs [--memory-limit-mb N] [--debug] [--version]
//
// One PGlite instance is one PostgreSQL backend in single-user mode: one
// session. Every dblayer connection the parent opens (`open`) is a name for
// that session with a mode (read or write). Requests run one at a time, in
// arrival order, on the main thread; a query blocks the event loop while it
// runs, so nothing (cancel, a timer) can reach it: the parent's SIGKILL is
// the only stop for a query that will not end.
//
// The session is held by at most one connection at a time: the one with an
// open transaction, or with a stream paused between chunks (a stream runs
// in a transaction block of its own when the connection has none). The
// parent does not send another connection's requests while it is held; the
// worker refuses them (SESSION_BUSY) if it does.
//
// Read connections run statements with default_transaction_read_only on;
// read transactions are REPEATABLE READ READ ONLY; inside a transaction each
// execute / execute_many runs in a savepoint, so a failed statement does not
// abort the transaction (as the psycopg backend does).

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import { decode, encode } from './codec.mjs';
import { writeAll, ParentGone, consoleToStderr } from './io.mjs';
import { LimitError, ParamError, PgError, Session, leadingKeyword } from './session.mjs';
import { setSessionZone } from './values.mjs';
import { errorType } from './errors.mjs';
import { copyText } from './copy.mjs';

const PROTOCOL_VERSION = 1;
const DEFAULT_MAX_FRAME = 256 * 1024 * 1024;
const SHUTDOWN_GRACE_MS = 5000;
const SAVEPOINT = 'dblayer_stmt';
const PAGE = 65536;

const require = createRequire(import.meta.url);
// The package's exports map hides package.json: read it beside dist/.
const PGLITE_VERSION = JSON.parse(
  fs.readFileSync(new URL('../package.json', `file://${require.resolve('@electric-sql/pglite')}`), 'utf8'),
).version;

// -- arguments --------------------------------------------------------------------

let debug = false;
let memoryLimitMb = null;
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === '--debug') debug = true;
  else if (a === '--memory-limit-mb' && i + 1 < argv.length) memoryLimitMb = parseInt(argv[++i], 10) || null;
  else if (a === '--version') {
    fs.writeSync(1, `dblayer-pglite-worker protocol ${PROTOCOL_VERSION}, PGlite ${PGLITE_VERSION}, Node ${process.version}\n`);
    process.exit(0);
  } else {
    fs.writeSync(2, 'usage: node pglite-worker/src/main.mjs [--memory-limit-mb N] [--debug] [--version]\n');
    process.exit(2);
  }
}

process.on('SIGINT', () => {}); // the parent's Ctrl-C is not ours: we exit when stdin closes
consoleToStderr();

// The memory cap: PGlite creates its WebAssembly memory with a 2 GB
// maximum and no option to change it. Clamping the maximum here makes
// PostgreSQL's malloc fail past the limit, which it reports as an ordinary
// "out of memory" error (SQLSTATE 53200) and survives.
if (memoryLimitMb) {
  const Memory = WebAssembly.Memory;
  const capPages = Math.max(Math.floor((memoryLimitMb * 1024 * 1024) / PAGE), 2048); // 128 MB: PGlite's initial size
  const Capped = function Memory_(desc) {
    const d = { ...desc };
    if (d.maximum === undefined || d.maximum > capPages) d.maximum = Math.max(capPages, d.initial);
    return new Memory(d);
  };
  Capped.prototype = Memory.prototype;
  WebAssembly.Memory = Capped;
}

// Start-up timings, reported in the first `open` reply: process start to
// here (Node and module loading), compiling PostgreSQL's WebAssembly (started
// now, so it overlaps `hello`), PGlite's start (instantiate, PostgreSQL
// start-up and WAL recovery, initdb on a new directory), the first query.
const timings = { boot_ms: performance.now() };
const WASM_PATH = new URL('pglite.wasm', `file://${require.resolve('@electric-sql/pglite')}`);
const compiled = (async () => {
  const t0 = performance.now();
  const bytes = fs.readFileSync(WASM_PATH);
  const m = await WebAssembly.compile(bytes);
  timings.compile_ms = performance.now() - t0;
  return m;
})();
compiled.catch(() => {});

let maxFrame = DEFAULT_MAX_FRAME;
let codec = 'json';
let exiting = false;

async function exit(code) {
  if (exiting) return;
  exiting = true;
  process.exit(code);
}

function sendFrame(payload) {
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32BE(payload.length);
  writeAll(Buffer.concat([hdr, payload]));
}

function send(msg) {
  let payload = encode(codec, msg);
  if (payload.length > maxFrame) {
    payload = encode(codec, {
      id: msg.id ?? null,
      error: { type: 'ResourceLimitExceeded', message: `reply of ${payload.length} bytes exceeds the ${maxFrame}-byte frame size cap` },
    });
  }
  try {
    sendFrame(payload);
  } catch (e) {
    if (e instanceof ParentGone) exit(0);
    throw e;
  }
}

// -- errors ---------------------------------------------------------------------------

class WireError extends Error {
  constructor(type, message, code = undefined, extra = {}) {
    super(message);
    this.wire = { type, message, ...(code !== undefined ? { code } : {}), ...extra };
  }
}

function wireError(e) {
  if (e instanceof WireError) return e.wire;
  if (e instanceof PgError) {
    const f = e.fields;
    const state = f.code || '';
    const type = errorType(state);
    const message = type === 'QueryInterrupted' ? 'query interrupted (time limit exceeded or cancelled)' : f.message;
    const err = { type, message, cause: { type: `pglite.${f.severity || 'ERROR'}`, message: f.message } };
    if (type === 'QueryError' || type === 'IntegrityError') err.code = state || undefined;
    return err;
  }
  if (e instanceof ParamError) return { type: 'QueryError', message: e.message, code: 'DATA_ERROR' };
  if (e instanceof LimitError) return { type: 'ResourceLimitExceeded', message: e.message };
  if (e instanceof RangeError && /memory/i.test(e.message)) return { type: 'ResourceLimitExceeded', message: `worker ran out of memory: ${e.message}`, fatal: true };
  return { type: 'QueryError', message: `worker error: ${e?.name}: ${e?.message}`, code: 'WORKER' };
}

// -- state ----------------------------------------------------------------------------

const session = new Session();
let opened = null; // {path, startParams}: the data directory this worker serves
let connSeq = 0;
let txSeq = 0;
const conns = new Map(); // conn id -> {id, write, tx, rawTx}
const streams = new Map(); // stream id -> Stream
let holder = null; // the conn holding the session (open transaction or paused stream)

function connFor(msg) {
  const c = msg.conn;
  if (c === undefined || c === null) {
    if (conns.size === 1) return conns.values().next().value;
    throw new WireError('QueryError', `request has no 'conn' and the worker has ${conns.size} connections`, 'PROTOCOL');
  }
  const wc = conns.get(c);
  if (!wc) throw new WireError('QueryError', `no connection ${c} in this worker`, 'PROTOCOL');
  return wc;
}

function checkHolder(wc) {
  if (holder !== null && holder !== wc) {
    throw new WireError(
      'QueryError',
      `the session is held by connection ${holder.id} (an open transaction or stream); PGlite has one session per database`,
      'SESSION_BUSY',
    );
  }
}

function checkTx(wc, tx) {
  if (tx === undefined || tx === null) {
    if (wc.tx && !wc.tx.expired) return; // the parent may leave tx out on a single connection
    return;
  }
  const t = wc.tx;
  if (!t || t.id !== tx) {
    if (t && t.expiredId === tx) throw expiredError(tx);
    if (wc.expiredId === tx) throw expiredError(tx);
    throw new WireError('TransactionClosed', `transaction ${tx} is not open`);
  }
  if (t.expired) throw expiredError(tx);
}

function expiredError(tx) {
  return new WireError('TransactionExpired', `transaction ${tx} passed its deadline and was rolled back`);
}

function release(wc) {
  if (holder === wc && !wc.tx && !wc.rawTx && ![...streams.values()].some((s) => s.wc === wc)) holder = null;
}

function inTx(wc) {
  return !!(wc && (wc.tx || wc.rawTx));
}

// -- requests --------------------------------------------------------------------------

const TX_CONTROL = new Set(['BEGIN', 'START', 'COMMIT', 'END', 'ROLLBACK', 'ABORT', 'SAVEPOINT', 'RELEASE', 'PREPARE']);
const SESSION_CHANGING = new Set(['SET', 'RESET', 'DISCARD']);

async function beforeStatement(wc) {
  if (!inTx(wc) && session.status === 'I') await session.setReadOnly(!wc.write);
}

function afterStatement(wc, sql) {
  if (sql && SESSION_CHANGING.has(leadingKeyword(sql))) session.readOnly = null; // set it again next time
  if (!wc.tx) {
    // A transaction block the caller opened with plain SQL (BEGIN):
    // treated as the connection's own until it ends.
    wc.rawTx = session.status !== 'I';
    if (wc.rawTx) holder = wc;
    else release(wc);
  }
}

async function rollbackToSavepoint() {
  if (session.status === 'I') return;
  await session.quiet(`ROLLBACK TO SAVEPOINT ${SAVEPOINT}; RELEASE SAVEPOINT ${SAVEPOINT}`);
}

function useSavepoint(wc, sql) {
  return !!wc.tx && (sql === null || !TX_CONTROL.has(leadingKeyword(sql)));
}

function deadlineCheck(wc) {
  const t = wc.tx;
  if (t && t.deadlineAt !== null && performance.now() >= t.deadlineAt && !t.expired) return t;
  return null;
}

async function doExecute(m, wc) {
  const sql = m.sql;
  const sp = useSavepoint(wc, sql);
  await beforeStatement(wc);
  let r;
  try {
    r = await session.execute(sql, m.params, {
      maxRows: typeof m.max_rows === 'number' ? m.max_rows : null,
      maxBytes: typeof m.max_result_bytes === 'number' ? m.max_result_bytes : null,
      pre: sp ? `SAVEPOINT ${SAVEPOINT}` : null,
      post: sp ? `RELEASE SAVEPOINT ${SAVEPOINT}` : null,
    });
  } catch (e) {
    if (sp) await rollbackToSavepoint();
    throw e;
  } finally {
    afterStatement(wc, sql);
  }
  return { result: resultOf(r) };
}

function resultOf(r) {
  const out = { columns: r.columns };
  if (r.rows.length) out.rows = r.rows;
  if (r.types.length && r.types.some((t) => t !== null)) out.types = r.types;
  if (r.truncated) out.truncated = true;
  if (r.rowcount !== -1) out.rowcount = r.rowcount;
  return out;
}

async function doExecuteMany(m, wc) {
  const seq = Array.isArray(m.params_seq) ? m.params_seq : [];
  if (!seq.length) return { result: { rowcount: 0 } };
  const sp = useSavepoint(wc, m.sql);
  await beforeStatement(wc);
  let n;
  try {
    n = await session.executeMany(m.sql, seq, { pre: sp ? `SAVEPOINT ${SAVEPOINT}` : null });
    if (sp) await session.simple(`RELEASE SAVEPOINT ${SAVEPOINT}`);
  } catch (e) {
    if (sp) await rollbackToSavepoint();
    throw e;
  } finally {
    afterStatement(wc, m.sql);
  }
  return { result: { rowcount: n } };
}

/** Several statements in one request, no statement savepoints (a failure
 * aborts an open transaction, as the psycopg backend's pipelined batch).
 * Outside a transaction the batch is one implicit transaction: one
 * snapshot, all or nothing. */
async function doBatch(m, wc) {
  const stmts = Array.isArray(m.statements) ? m.statements : [];
  await beforeStatement(wc);
  const own = !inTx(wc) && stmts.length > 1;
  const results = [];
  try {
    if (own) await session.simple(wc.write ? 'BEGIN' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    for (const [sql, params] of stmts) {
      const r = await session.execute(sql, params, { maxRows: typeof m.max_rows === 'number' ? m.max_rows : null });
      results.push(resultOf(r));
    }
    if (own) await session.simple('COMMIT');
  } catch (e) {
    if (own) await session.quiet('ROLLBACK');
    throw e;
  } finally {
    afterStatement(wc, null);
  }
  return { result: results };
}

async function doBegin(m, wc) {
  if (wc.tx || wc.rawTx) throw new WireError('QueryError', 'a transaction is already open on this connection', 'PROTOCOL');
  const mode = m.mode === 'read' ? 'read' : 'write';
  await session.simple(mode === 'write' ? 'BEGIN READ WRITE' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  const id = `t${++txSeq}`;
  const t = { id, mode, deadlineAt: null, timer: null, expired: false };
  if (typeof m.deadline_ms === 'number') {
    t.deadlineAt = performance.now() + Math.max(0, m.deadline_ms);
    t.timer = setTimeout(() => {
      t.timer = null;
      enqueue(() => expire(wc, t));
    }, Math.max(0, m.deadline_ms));
  }
  wc.tx = t;
  holder = wc;
  return { ok: true, tx: id };
}

function endTx(wc) {
  const t = wc.tx;
  if (t && t.timer) clearTimeout(t.timer);
  wc.tx = null;
  wc.rawTx = false;
  release(wc);
}

async function closeStreamsOf(wc) {
  for (const s of [...streams.values()]) if (s.wc === wc) await s.close(false);
}

/** The transaction's deadline passed: roll back, tell the parent. */
async function expire(wc, t) {
  if (wc.tx !== t || t.expired) return;
  t.expired = true;
  await closeStreamsOf(wc);
  await session.quiet('ROLLBACK');
  wc.expiredId = t.id;
  endTx(wc);
  send({ type: 'expired', conn: wc.id, tx: t.id });
}

async function doCommit(m, wc) {
  if (wc.tx && m.tx && wc.tx.id !== m.tx) throw new WireError('TransactionClosed', `transaction ${m.tx} is not open`);
  if (!wc.tx && !wc.rawTx) {
    if (m.tx && wc.expiredId === m.tx) throw expiredError(m.tx);
    return { ok: true };
  }
  if (wc.tx && wc.tx.timer) clearTimeout(wc.tx.timer); // a commit that has started is not cut short
  await closeStreamsOf(wc);
  let msgs;
  try {
    msgs = await session.simple('COMMIT');
  } catch (e) {
    if (session.status !== 'I') await session.quiet('ROLLBACK');
    endTx(wc);
    throw e;
  }
  endTx(wc);
  if (msgs.some((x) => x.name === 'commandComplete' && x.text === 'ROLLBACK')) {
    throw new WireError('QueryError', 'transaction was aborted by an earlier error and has been rolled back', '25P02');
  }
  return { ok: true };
}

async function doRollback(m, wc) {
  if (!wc.tx && !wc.rawTx) {
    if (m.tx && wc.expiredId === m.tx) return { ok: true, expired: true };
    return { ok: true };
  }
  await closeStreamsOf(wc);
  if (session.status !== 'I') await session.quiet('ROLLBACK');
  endTx(wc);
  return { ok: true };
}

// -- streams -----------------------------------------------------------------------------

let portalSeq = 0;

class Stream {
  constructor(id, wc, m) {
    this.id = id;
    this.wc = wc;
    this.sql = m.sql;
    this.params = m.params;
    this.chunk = Math.max(1, Number(m.chunk_rows) || 1000);
    this.credit = Number(m.credit) || 1;
    this.maxBytes = typeof m.max_result_bytes === 'number' ? m.max_result_bytes : null;
    this.bytes = 0;
    this.first = true;
    this.portal = null;
    this.ownTx = false;
  }

  async start() {
    const wc = this.wc;
    if (!inTx(wc)) {
      await beforeStatement(wc);
      await session.simple(wc.write ? 'BEGIN' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      this.ownTx = true;
    }
    streams.set(this.id, this);
    holder = wc;
    this.portal = await session.openPortal(`dblayer_s${++portalSeq}`, this.sql, this.params);
  }

  async pump() {
    while (this.credit > 0) {
      const msg = { id: this.id };
      let rows = [];
      if (!this.portal.done) {
        const r = await session.fetch(this.portal, this.chunk, this.maxBytes, this.bytes);
        rows = r.rows;
        this.bytes = r.bytes;
      }
      if (this.first) {
        msg.columns = this.portal.columns;
        if (this.portal.types.some((t) => t !== null)) msg.types = this.portal.types;
        this.first = false;
      }
      if (!rows.length) {
        await this.close(true);
        msg.end = true;
        send(msg);
        return;
      }
      msg.rows = rows;
      this.credit--;
      send(msg);
    }
  }

  async close(finished) {
    if (!streams.has(this.id)) return;
    streams.delete(this.id);
    if (this.portal) await session.closePortal(this.portal);
    if (this.ownTx && session.status !== 'I') await session.quiet(finished && session.status === 'T' ? 'COMMIT' : 'ROLLBACK');
    release(this.wc);
  }
}

async function doStream(m, wc, id) {
  const s = new Stream(id, wc, m);
  try {
    await s.start();
    await s.pump();
  } catch (e) {
    await s.close(false);
    throw e;
  }
  return null; // frames already sent
}

async function doMore(m) {
  const s = streams.get(m.stream);
  if (!s) return;
  s.credit += Number(m.credit) || 1;
  try {
    await s.pump();
  } catch (e) {
    await s.close(false);
    send({ id: s.id, error: wireError(e), ...replyTx(s.wc) });
  }
}

async function doCancel(target) {
  const s = streams.get(target);
  if (!s) return; // a running request cannot be reached; a finished one needs nothing
  await s.close(false);
  send({ id: target, end: true, cancelled: true });
}

// -- open / close / sleep ----------------------------------------------------------------

function startParams(config) {
  // PGlite's defaults (single-user mode, fsync off) plus ours: small shared
  // buffers (an awake database should cost little memory), the session
  // time zone UTC.
  const p = [
    '--single', '-F', '-O', '-j',
    '-c', 'search_path=public', '-c', 'exit_on_error=false', '-c', 'log_checkpoints=false',
    '-c', 'max_worker_processes=0', '-c', 'max_parallel_workers=0', '-c', 'max_parallel_workers_per_gather=0',
    '-c', 'io_method=sync', '-c', 'max_parallel_maintenance_workers=0',
    '-c', `shared_buffers=${config.shared_buffers ?? '16MB'}`,
  ];
  for (const [k, v] of Object.entries(config.settings ?? {})) p.push('-c', `${k}=${v}`);
  return p;
}

async function doOpen(m) {
  const path = m.path;
  const config = m.config ?? {};
  if (typeof path !== 'string' || !path) throw new WireError('QueryError', 'open needs a path', 'PROTOCOL');
  if (opened === null) {
    if (config.create === false && !fs.existsSync(`${path}/PG_VERSION`)) {
      throw new WireError('QueryError', `no PGlite database at ${path} (create=False)`, 'CANTOPEN');
    }
    const template = config.template;
    if (template && !fs.existsSync(`${path}/PG_VERSION`) && fs.existsSync(`${template}/PG_VERSION`)) {
      fs.cpSync(template, path, { recursive: true, errorOnExist: false });
    }
    if (!fs.existsSync(`${path}/PG_VERSION`)) initdbInChild(path);
    const t0 = performance.now();
    const wasmModule = await compiled;
    const t1 = performance.now();
    await session.open(path, { startParams: startParams(config), wasmModule });
    const t2 = performance.now();
    timings.wait_compile_ms = t1 - t0;
    timings.open_ms = t2 - t1;
    const r = await session.execute("select current_setting('server_version'), current_setting('TimeZone')", []);
    session.version = r.rows[0][0];
    setSessionZone(r.rows[0][1]);
    timings.first_query_ms = performance.now() - t2;
    opened = { path };
  } else if (opened.path !== path) {
    throw new WireError('NotSupported', `this worker serves ${opened.path}; one PGlite instance per process`);
  }
  const wc = { id: ++connSeq, write: m.mode === 'write', tx: null, rawTx: false, expiredId: null };
  conns.set(wc.id, wc);
  const t = {};
  for (const [k, v] of Object.entries(timings)) t[k] = Math.round(v * 100) / 100;
  return { ok: true, conn: wc.id, timings: t };
}

/** A new data directory: initdb in a short-lived child process. Run here,
 * initdb leaves this process's WebAssembly memory at about 830 MB for as
 * long as the database stays awake (WebAssembly memory never shrinks); in a
 * child it costs one more Node start (about 50 ms) and the memory goes
 * with the child. */
function initdbInChild(path) {
  const script = new URL('../scripts/init_template.mjs', import.meta.url).pathname;
  const r = spawnSync(process.execPath, [...process.execArgv, script, path], { stdio: ['ignore', 'ignore', 'pipe'] });
  if (r.status !== 0) {
    const why = r.error ? r.error.message : `${r.stderr}`.trim().slice(-500) || `exit status ${r.status}`;
    throw new WireError('QueryError', `initdb for ${path} failed: ${why}`, 'CANTOPEN');
  }
}

async function closeConn(wc) {
  await closeStreamsOf(wc);
  if (inTx(wc) && session.status !== 'I') await session.quiet('ROLLBACK');
  endTx(wc);
  conns.delete(wc.id);
  if (holder === wc) holder = null;
}

async function shutdown() {
  for (const wc of [...conns.values()]) await closeConn(wc);
  if (session.status !== 'I') await session.quiet('ROLLBACK');
  await session.close();
}

// -- dispatch ----------------------------------------------------------------------------

function replyTx(wc) {
  return inTx(wc) ? { in_tx: true } : {};
}

let pending = Promise.resolve();
function enqueue(fn) {
  pending = pending.then(fn).catch((e) => {
    if (e instanceof ParentGone) exit(0);
    console.error(`dblayer-pglite-worker: internal error: ${e?.stack || e}`);
  });
}

const CONN_TYPES = new Set(['close', 'execute', 'execute_many', 'batch', 'stream', 'begin', 'commit', 'rollback', 'op', 'schema', 'schema_version', 'copy']);

async function handle(msg) {
  const type = msg.type;
  const id = msg.id;
  if (type === 'more') return doMore(msg);
  if (type === 'cancel') return doCancel(msg.target);
  if (type === 'sleep') {
    await shutdown();
    send({ id: id ?? null, ok: true });
    exit(0);
    return;
  }
  if (type === 'open') {
    try {
      send({ id, ...(await doOpen(msg)) });
    } catch (e) {
      send({ id, error: wireError(e) });
    }
    return;
  }
  if (type === '_debug' && debug) return doDebug(msg);
  if (!CONN_TYPES.has(type)) {
    const t = typeof type === 'string' ? `'${type.slice(0, 64)}'` : type === undefined || type === null ? 'None' : String(type);
    send({ id, error: { type: 'QueryError', message: `unknown message type ${t}`, code: 'PROTOCOL' } });
    return;
  }
  let wc = null;
  try {
    wc = connFor(msg);
    if (type === 'close') {
      await closeConn(wc);
      send({ id, ok: true });
      return;
    }
    if (type === 'rollback') {
      send({ id, ...(await doRollback(msg, wc)) });
      return;
    }
    checkHolder(wc);
    if (type === 'commit') {
      send({ id, ...(await doCommit(msg, wc)) });
      return;
    }
    if (type === 'begin') {
      send({ id, ...(await doBegin(msg, wc)), in_tx: true });
      return;
    }
    checkTx(wc, msg.tx);
    let reply;
    switch (type) {
      case 'execute': reply = await doExecute(msg, wc); break;
      case 'execute_many': reply = await doExecuteMany(msg, wc); break;
      case 'batch': reply = await doBatch(msg, wc); break;
      case 'copy': reply = await doCopy(msg, wc); break;
      case 'stream': reply = await doStream(msg, wc, id); break;
      case 'op': throw new WireError('NotSupported', 'the PGlite worker runs no named operations', 'PROTOCOL');
      default: throw new WireError('NotSupported', `'${type}' is answered by the parent (it sends the catalog queries)`, 'PROTOCOL');
    }
    const late = deadlineCheck(wc);
    if (late) {
      // The statement finished after the deadline (nothing can interrupt it
      // while it runs): its transaction is rolled back, not its result sent.
      await expire(wc, late);
      throw expiredError(late.id);
    }
    if (reply !== null) send({ id, ...reply, ...replyTx(wc) });
  } catch (e) {
    send({ id, error: wireError(e), ...replyTx(wc) });
    if (wireError(e).fatal) exit(1);
  }
}

/** `copy`: rows (lists of wire values) into a table by COPY FROM STDIN,
 * in text format, so the server converts each value as it would a literal. */
async function doCopy(m, wc) {
  const sp = useSavepoint(wc, null);
  await beforeStatement(wc);
  try {
    if (sp) await session.simple(`SAVEPOINT ${SAVEPOINT}`);
    const n = await session.copyIn(m.statement, copyText(m.rows));
    if (sp) await session.simple(`RELEASE SAVEPOINT ${SAVEPOINT}`);
    return { result: { rowcount: n } };
  } catch (e) {
    if (sp) await rollbackToSavepoint();
    throw e;
  } finally {
    afterStatement(wc, null);
  }
}

async function doDebug(msg) {
  const id = msg.id;
  const action = msg.action;
  if (action === 'sleep') {
    const end = Date.now() + Number(msg.seconds ?? 1) * 1000;
    while (Date.now() < end); // blocks the whole worker, as a query does
    send({ id, ok: true });
  } else if (action === 'oversized_frame') {
    const b = Buffer.alloc(20, 120);
    b.writeUInt32BE(2 ** 31, 0);
    writeAll(b);
  } else if (action === 'truncated_frame') {
    const b = Buffer.concat([Buffer.from([0, 0, 0, 100]), Buffer.from('{"id":')]);
    writeAll(b);
    process.exit(3);
  } else if (action === 'garbage') {
    const p = Buffer.from([0xff, 0xfe, 0x20, 0x6e, 0x6f, 0x74]);
    const h = Buffer.alloc(4);
    h.writeUInt32BE(p.length);
    writeAll(Buffer.concat([h, p]));
  } else if (action === 'exit') {
    process.exit(Number(msg.code ?? 1));
  } else if (action === 'print') {
    console.log(String(msg.text ?? ''));
    send({ id, ok: true });
  } else if (action === 'status') {
    send({ id, ok: true, status: session.status, holder: holder ? holder.id : null, streams: streams.size, conns: conns.size, memory: process.memoryUsage() });
  } else {
    send({ id, error: { type: 'QueryError', message: `unknown debug action ${JSON.stringify(action)}`, code: 'PROTOCOL' } });
  }
}

function hello(msg) {
  const wanted = Array.isArray(msg.codecs) ? msg.codecs : ['json'];
  const chosen = wanted.find((c) => c === 'json' || c === 'msgpack') ?? 'json';
  if (typeof msg.max_frame_bytes === 'number' && msg.max_frame_bytes > 0) maxFrame = msg.max_frame_bytes;
  const reply = {
    id: msg.id ?? null,
    type: 'hello',
    protocol: PROTOCOL_VERSION,
    codec: chosen,
    implementation: 'pglite',
    engine: 'postgres',
    pglite: PGLITE_VERSION,
    node: process.versions.node,
    pid: process.pid,
    rlimit_as: false,
    memory_limit_mb: memoryLimitMb,
    ops: false,
  };
  const payload = encode('json', reply); // in JSON; everything after it in the agreed codec
  codec = chosen;
  sendFrame(payload);
}

// -- input -------------------------------------------------------------------------------

let eof = false;

function onEof() {
  if (eof) return;
  eof = true;
  process.stdin.pause();
  setTimeout(() => exit(0), SHUTDOWN_GRACE_MS).unref();
  enqueue(async () => {
    await shutdown();
    exit(0);
  });
}

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

function dispatch(payload) {
  const msg = decode(codec, payload);
  if (msg === null || typeof msg !== 'object' || Array.isArray(msg) || msg instanceof Uint8Array) {
    throw new Error('a message must be a map');
  }
  if (msg.type === 'hello') {
    hello(msg);
    return;
  }
  enqueue(() => handle(msg));
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
