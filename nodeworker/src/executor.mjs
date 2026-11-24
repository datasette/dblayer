// An executor thread: one connection, in a WebAssembly instance of its own,
// and every request on it. The main thread (main.mjs) reads frames, keeps
// each connection's queue and the timers, and hands this thread one job at
// a time; this thread runs it, writes the reply frames to fd 1 itself
// (under a write lock shared with the other threads), and posts back what
// changed (a connection opened, a transaction begun or ended, a stream left
// open). Connections run at the same time, as threads do in-process; file
// locks between them live in shared memory (locks.mjs).
//
// A WebAssembly call blocks this thread's event loop, so cancellation
// arrives through shared memory: the main thread stores a reason in
// `flags[0]`, and SQLite's progress handler (a JavaScript callback) reads
// it with Atomics.load and stops the statement. A statement that never
// reaches a progress check (one long C call) is stopped by the main thread
// terminating this thread.
//
// Results follow CPython's sqlite3 module, as the in-process backend sees
// them (rowcount and lastrowid rules, error classes, codes and messages),
// ported from the C worker (cworker/src/worker.c).

import fs from 'node:fs';
import { enableCompileCache } from 'node:module';
import { parentPort, workerData, threadId } from 'node:worker_threads';
import { Writer, Raw, PyFloat, Ext, decode, EXT_CODES } from './codec.mjs';
import { writeAll, ParentGone, consoleToStderr, setWriteLock } from './io.mjs';
import { installNodeVfs } from './vfs.mjs';
import { KW, leadingKeyword, lstripSqlBytes, isDml, pathAsUri } from './sqlutil.mjs';
import { readSchema } from './schema.mjs';

const now = () => performance.timeOrigin + performance.now();
const T_START = now();
consoleToStderr();

const { flags, debug, memoryLimitMb, maxFrameBytes: initialMaxFrame, lockTable, writeLock } = workerData;
setWriteLock(writeLock);
if (process.env.DBLAYER_NODEWORKER_COMPILE_CACHE !== '0') enableCompileCache?.();
// The compiled module: in workerData, or (for a thread started while the
// main thread was still compiling it) the first message.
const modulePromise = workerData.wasmModule
  ? Promise.resolve(workerData.wasmModule)
  : new Promise((resolve) => parentPort.once('message', (m) => resolve(m.module)));
const { default: sqlite3InitModule } = await import('@sqlite.org/sqlite-wasm');
const T_IMPORTED = now();
const wasmModule = await modulePromise;

// WebAssembly memory: our own, so its maximum is the worker's memory cap
// (per connection, as RLIMIT_AS is per process for the Python worker).
const PAGE = 65536;
const BASE_MB = 24; // runtime, stack and static data before any database
const maxPages = memoryLimitMb ? Math.min(65536, Math.ceil(((memoryLimitMb + BASE_MB) * 1048576) / PAGE)) : 32768;
const wasmMemory = new WebAssembly.Memory({ initial: Math.min(256, maxPages), maximum: maxPages });
const sqlite3 = await sqlite3InitModule({
  print: console.error,
  printErr: console.error,
  wasmMemory,
  // the module compiled once by the main thread: no compile per thread
  instantiateWasm: wasmModule
    ? (imports, ok) => {
        WebAssembly.instantiate(wasmModule, imports).then((inst) => ok(inst, wasmModule));
        return {};
      }
    : undefined,
});
const { capi, wasm } = sqlite3;
const X = wasm.exports;
const C = capi;
const vfs = installNodeVfs(sqlite3, {
  lockTable,
  threadId,
  keepMainFds: workerData.keepMainFds,
});
const T_READY = now();

const SQLITE_TRANSIENT = -1;
const FETCH_BATCH = 256;
const MAX_EXPIRED = 64;
const SQLITE_ENGINE = 'dblayer.worker.engines:sqlite';
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

let H = new Uint8Array(wasmMemory.buffer);
const heap = () => (H.length === 0 || H.buffer !== wasmMemory.buffer ? (H = new Uint8Array(wasmMemory.buffer)) : H);
const te = new TextEncoder();
const tdFatal = new TextDecoder('utf-8', { fatal: true });
const tdLossy = new TextDecoder('utf-8');

// Pointer-out slots and a reusable scratch buffer in WebAssembly memory.
const slots = X.malloc(32);
const PP_STMT = slots;
const PP_TAIL = slots + 8;
const PP_DB = slots + 16;
let scratchPtr = 0;
let scratchSize = 0;
function scratch(n) {
  if (n > scratchSize) {
    if (scratchPtr) X.free(scratchPtr);
    scratchSize = Math.max(n, 4096);
    scratchPtr = X.malloc(scratchSize);
    if (!scratchPtr) {
      scratchSize = 0;
      throw new WorkerErr('ResourceLimitExceeded', 'worker ran out of memory (memory limit)');
    }
  }
  return scratchPtr;
}
const peekPtr = (p) => wasm.peekPtr(p);
const cstr = (p) => (p ? wasm.cstrToJs(p) : null);
/** A JS string as UTF-8 in scratch memory: [pointer, byte length]. */
function utf8In(s, extra = 0) {
  const p = scratch(s.length * 3 + 1 + extra);
  const { written } = te.encodeInto(s, heap().subarray(p, p + s.length * 3 + 1));
  return [p, written];
}

// -- errors ---------------------------------------------------------------------

class WorkerErr extends Error {
  constructor(type, message, { code = null, causeType = null, causeMessage = null, builtin = false, fatal = false, ext = 0 } = {}) {
    super(message);
    this.type = type;
    this.code = code;
    this.causeType = causeType;
    this.causeMessage = causeMessage;
    this.builtin = builtin;
    this.fatal = fatal;
    this.ext = ext;
  }
}

/** A sqlite3.ProgrammingError raised by CPython's _sqlite module. */
const progErr = (msg) => new WorkerErr('QueryError', msg, { causeType: 'sqlite3.ProgrammingError', causeMessage: msg });
const builtinErr = (type, msg) => new WorkerErr(type, msg, { builtin: true });
const interrupted = () =>
  new WorkerErr('QueryInterrupted', 'query interrupted (time limit exceeded or cancelled)', {
    causeType: 'sqlite3.OperationalError',
    causeMessage: 'interrupted',
  });

/** The class CPython's _pysqlite_seterror() raises for a primary code. */
function pyExc(primary) {
  switch (primary) {
    case C.SQLITE_INTERNAL: case C.SQLITE_NOTFOUND: return 'sqlite3.InternalError';
    case C.SQLITE_NOMEM: return 'MemoryError';
    case C.SQLITE_ERROR: case C.SQLITE_PERM: case C.SQLITE_ABORT: case C.SQLITE_BUSY: case C.SQLITE_LOCKED:
    case C.SQLITE_READONLY: case C.SQLITE_INTERRUPT: case C.SQLITE_IOERR: case C.SQLITE_FULL:
    case C.SQLITE_CANTOPEN: case C.SQLITE_PROTOCOL: case C.SQLITE_EMPTY: case C.SQLITE_SCHEMA:
      return 'sqlite3.OperationalError';
    case C.SQLITE_CORRUPT: return 'sqlite3.DatabaseError';
    case C.SQLITE_TOOBIG: return 'sqlite3.DataError';
    case C.SQLITE_CONSTRAINT: case C.SQLITE_MISMATCH: return 'sqlite3.IntegrityError';
    case C.SQLITE_MISUSE: case C.SQLITE_RANGE: return 'sqlite3.InterfaceError';
    default: return 'sqlite3.DatabaseError';
  }
}

/** SqliteDialect.map_exception, applied to the error SQLite reports. */
function sqliteErr(db, rc) {
  let ext = db ? X.sqlite3_extended_errcode(db) : rc;
  if (ext === 0 || (ext & 0xff) !== (rc & 0xff)) ext = rc;
  const primary = ext & 0xff;
  const msg = db ? cstr(X.sqlite3_errmsg(db)) : cstr(X.sqlite3_errstr(rc));
  const name = C.sqlite3_js_rc_str(ext) || null;
  const cls = pyExc(primary);
  const opts = { causeType: cls, causeMessage: msg, ext };
  if (primary === C.SQLITE_INTERRUPT) return new WorkerErr('QueryInterrupted', 'query interrupted (time limit exceeded or cancelled)', opts);
  if (cls === 'sqlite3.IntegrityError') return new WorkerErr('IntegrityError', msg, { ...opts, code: name });
  if (ext === C.SQLITE_NOMEM || ext === C.SQLITE_TOOBIG || ext === C.SQLITE_FULL) return new WorkerErr('ResourceLimitExceeded', msg, opts);
  if (name === 'SQLITE_BUSY_SNAPSHOT') return new WorkerErr('WriteConflict', msg, opts);
  return new WorkerErr('QueryError', msg, { ...opts, code: name });
}

// -- output -------------------------------------------------------------------------

let codec = 'json';
let maxFrame = initialMaxFrame;
let frameW = new Writer('json');
let rowsW = new Writer('json');

function setCodec(name) {
  codec = name;
  frameW = new Writer(name);
  rowsW = new Writer(name);
}

class TooLarge extends Error {}

/** Encode and send one message. A reply over the frame cap becomes a
 * ResourceLimitExceeded error (Server.send). */
function send(msg) {
  const w = frameW;
  w.reset();
  w.ensure(4);
  w.pos = 4;
  let tooLarge = null;
  try {
    w.value(msg);
    if (w.pos - 4 > maxFrame) tooLarge = w.pos - 4;
  } catch (e) {
    if (!(e instanceof TooLarge || e instanceof RangeError)) throw e;
    tooLarge = w.pos - 4;
  }
  if (tooLarge !== null) {
    if (msg.id === undefined) return;
    w.reset();
    w.ensure(4);
    w.pos = 4;
    w.value({
      id: msg.id,
      error: { type: 'ResourceLimitExceeded', message: `reply too large: frame of ${tooLarge} bytes exceeds the ${maxFrame}-byte frame size cap` },
    });
  }
  w.dv.setUint32(0, w.pos - 4);
  writeAll(w.buf.subarray(0, w.pos));
  if (w.buf.length > 1 << 22) w.reset();
}

// -- statements ---------------------------------------------------------------------

let stmtDeadline = 0; // performance.now() deadline of the running statement, 0: none
const progressFn = wasm.installFunction('i(p)', () =>
  Atomics.load(flags, 0) !== 0 || (stmtDeadline !== 0 && performance.now() >= stmtDeadline) ? 1 : 0,
);

/** _TimeLimit: a progress-handler deadline (checked every instruction for
 * limits up to 20 ms, as Datasette does), which also sees cancellation. */
function limitInstall(wc, tlMs) {
  stmtDeadline = tlMs === null ? 0 : performance.now() + Math.max(tlMs, 0) || 1e-9;
  X.sqlite3_progress_handler(wc.db, tlMs !== null && tlMs <= 20 ? 1 : 1000, progressFn, 0);
}

function limitRemove(wc) {
  stmtDeadline = 0;
  X.sqlite3_progress_handler(wc.db, 0, 0, 0);
}

/** SqliteConnection._statement: refuse to start once cancelled. */
function stmtEnter() {
  if (Atomics.load(flags, 0) !== 0) throw interrupted();
}

/** CPython's pysqlite_statement_create: a statement pointer, or 0 for an
 * empty statement. */
function prepare(wc, sql) {
  if (typeof sql !== 'string') throw builtinErr('TypeError', 'sql must be a string');
  if (sql.includes('\0')) throw progErr('the query contains a null character');
  const [p, n] = utf8In(sql);
  if (n > X.sqlite3_limit(wc.db, C.SQLITE_LIMIT_SQL_LENGTH, -1)) {
    throw new WorkerErr('QueryError', 'query string is too large', { causeType: 'sqlite3.DataError', causeMessage: 'query string is too large' });
  }
  wasm.pokePtr(PP_STMT, 0);
  wasm.pokePtr(PP_TAIL, 0);
  const rc = X.sqlite3_prepare_v2(wc.db, p, n, PP_STMT, PP_TAIL);
  const st = peekPtr(PP_STMT);
  if (rc !== 0) {
    const e = sqliteErr(wc.db, rc);
    if (st) X.sqlite3_finalize(st);
    throw e;
  }
  const tail = peekPtr(PP_TAIL);
  if (tail && lstripSqlBytes(heap(), tail, p + n) >= 0) {
    if (st) X.sqlite3_finalize(st);
    throw progErr('You can only execute one statement at a time.');
  }
  return st;
}

const EXT_NAMES = {
  16: 'datetime.datetime', 17: 'datetime.date', 18: 'datetime.time', 19: 'decimal.Decimal',
  20: 'UUID', 21: 'datetime.timedelta', 22: 'dict', 23: 'tuple',
};

function bindText(wc, st, i, s) {
  if (!s.isWellFormed()) {
    const k = [...s].findIndex((ch) => ch.length === 1 && ch.charCodeAt(0) >= 0xd800 && ch.charCodeAt(0) <= 0xdfff);
    const ch = s.codePointAt(k < 0 ? 0 : k);
    throw builtinErr('UnicodeEncodeError', `'utf-8' codec can't encode character '\\u${ch.toString(16)}' in position ${k}: surrogates not allowed`);
  }
  const [p, n] = utf8In(s);
  return X.sqlite3_bind_text(st, i, p, n, SQLITE_TRANSIENT);
}

function bindValue(wc, st, i, v) {
  let rc;
  switch (typeof v) {
    case 'number':
      if (Number.isInteger(v)) rc = v >= -2147483648 && v <= 2147483647 ? X.sqlite3_bind_int(st, i, v) : X.sqlite3_bind_int64(st, i, BigInt(v));
      else rc = X.sqlite3_bind_double(st, i, v);
      break;
    case 'string':
      rc = bindText(wc, st, i, v);
      break;
    case 'bigint':
      if (v < INT64_MIN || v > INT64_MAX) throw builtinErr('OverflowError', 'Python int too large to convert to SQLite INTEGER');
      rc = X.sqlite3_bind_int64(st, i, v);
      break;
    case 'boolean':
      rc = X.sqlite3_bind_int(st, i, v ? 1 : 0);
      break;
    case 'undefined':
      rc = X.sqlite3_bind_null(st, i);
      break;
    default:
      if (v === null) rc = X.sqlite3_bind_null(st, i);
      else if (v instanceof PyFloat) rc = X.sqlite3_bind_double(st, i, v.v);
      else if (v instanceof Uint8Array) {
        if (v.length === 0) rc = X.sqlite3_bind_zeroblob(st, i, 0);
        else {
          const p = scratch(v.length);
          heap().set(v, p);
          rc = X.sqlite3_bind_blob(st, i, p, v.length, SQLITE_TRANSIENT);
        }
      } else if (v instanceof Ext) {
        // Python's sqlite3 still has default adapters for datetime
        // (isoformat(" ")) and date (isoformat()); every other value type
        // is a binding error naming the Python type.
        let inner = v.data;
        if (v.code === EXT_CODES.datetime && Array.isArray(inner) && inner.length === 2) inner = inner[0];
        if ((v.code === EXT_CODES.datetime || v.code === EXT_CODES.date) && typeof inner === 'string') {
          let s = inner;
          if (v.code === EXT_CODES.datetime && s.length > 10 && s[10] === 'T') s = s.slice(0, 10) + ' ' + s.slice(11);
          rc = bindText(wc, st, i, s);
        } else {
          throw progErr(`Error binding parameter ${i}: type '${EXT_NAMES[v.code] ?? `ext ${v.code}`}' is not supported`);
        }
      } else {
        throw progErr(`Error binding parameter ${i}: type '${Array.isArray(v) ? 'list' : 'dict'}' is not supported`);
      }
  }
  if (rc !== 0) throw sqliteErr(wc.db, rc);
}

/** CPython's bind_parameters (3.14 rules: a sequence for named
 * placeholders is an error). */
function bindParams(wc, st, params) {
  const num = st ? X.sqlite3_bind_parameter_count(st) : 0;
  if (params === null || params === undefined || Array.isArray(params)) {
    const got = Array.isArray(params) ? params.length : 0;
    if (got !== num) {
      throw progErr(`Incorrect number of bindings supplied. The current statement uses ${num}, and there are ${got} supplied.`);
    }
    for (let i = 1; i <= num; i++) {
      const name = cstr(X.sqlite3_bind_parameter_name(st, i));
      if (name && name[0] !== '?') {
        throw progErr(`Binding ${i} ('${name}') is a named parameter, but you supplied a sequence which requires nameless (qmark) placeholders.`);
      }
      bindValue(wc, st, i, params[i - 1]);
    }
    return;
  }
  if (typeof params !== 'object' || params instanceof Uint8Array || params instanceof Ext || params instanceof PyFloat) {
    throw progErr('parameters are of unsupported type');
  }
  for (let i = 1; i <= num; i++) {
    const name = cstr(X.sqlite3_bind_parameter_name(st, i));
    if (name === null) throw progErr(`Binding ${i} has no name, but you supplied a dictionary (which has only names).`);
    const key = name.slice(1);
    if (!Object.prototype.hasOwnProperty.call(params, key)) throw progErr(`You did not supply a value for binding parameter ${name}.`);
    bindValue(wc, st, i, params[key]);
  }
}

function columnNames(st, ncols) {
  const out = new Array(ncols);
  for (let i = 0; i < ncols; i++) {
    const p = X.sqlite3_column_name(st, i);
    out[i] = p ? lossy(p, cstrLen(p)) : '';
  }
  return out;
}

function cstrLen(p) {
  const h = heap();
  let e = p;
  while (h[e] !== 0) e++;
  return e - p;
}

const lossy = (p, n) => tdLossy.decode(heap().subarray(p, p + n));

/** UTF-8 check of heap[p, p+n): the number of characters, or -1. */
function utf8Chars(h, p, n) {
  let chars = 0;
  const end = p + n;
  let i = p;
  while (i < end) {
    const c = h[i];
    if (c < 0x80) { i++; chars++; continue; }
    let need;
    let min;
    if (c >= 0xc2 && c <= 0xdf) { need = 1; min = 0x80; }
    else if (c >= 0xe0 && c <= 0xef) { need = 2; min = 0x800; }
    else if (c >= 0xf0 && c <= 0xf4) { need = 3; min = 0x10000; }
    else return -1;
    if (i + need >= end) return -1;
    let cp = c & (0x3f >> need);
    for (let k = 1; k <= need; k++) {
      const d = h[i + k];
      if ((d & 0xc0) !== 0x80) return -1;
      cp = (cp << 6) | (d & 0x3f);
    }
    if (cp < min || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return -1;
    i += need + 1;
    chars++;
  }
  return chars;
}

const HEX = '0123456789abcdef';

/** Text column bytes heap[p, p+n) as a JSON string literal. */
function jsonText(w, h, p, n) {
  w.ensure(n * 6 + 2);
  const buf = w.buf;
  let o = w.pos;
  buf[o++] = 34;
  for (let i = p, end = p + n; i < end; i++) {
    const c = h[i];
    if (c >= 0x20 && c !== 34 && c !== 92) { buf[o++] = c; continue; }
    buf[o++] = 92;
    if (c === 34 || c === 92) buf[o++] = c;
    else if (c === 10) buf[o++] = 110;
    else if (c === 13) buf[o++] = 114;
    else if (c === 9) buf[o++] = 116;
    else if (c === 8) buf[o++] = 98;
    else if (c === 12) buf[o++] = 102;
    else {
      buf[o++] = 117; buf[o++] = 48; buf[o++] = 48;
      buf[o++] = HEX.charCodeAt(c >> 4);
      buf[o++] = HEX.charCodeAt(c & 15);
    }
  }
  buf[o++] = 34;
  w.pos = o;
}

/** Encode the current row (if `encode`) and return its size as
 * SqliteConnection._row_bytes() counts it. */
function emitRow(st, ncols, w, encode) {
  let size = 0;
  const json = w.json;
  if (encode) {
    if (json) w.byte(91);
    else w.arrayHeader(ncols);
  }
  for (let i = 0; i < ncols; i++) {
    if (encode && json && i) w.byte(44);
    switch (X.sqlite3_column_type(st, i)) {
      case 1: { // INTEGER
        size += 8;
        if (encode) {
          const b = X.sqlite3_column_int64(st, i);
          if (json) w.ascii(b.toString());
          else {
            const n = Number(b);
            w.int(Number.isSafeInteger(n) ? n : b);
          }
        }
        break;
      }
      case 2: // FLOAT
        size += 8;
        if (encode) w.float(X.sqlite3_column_double(st, i));
        break;
      case 3: { // TEXT
        const p = X.sqlite3_column_text(st, i);
        const n = X.sqlite3_column_bytes(st, i);
        const h = heap();
        const chars = utf8Chars(h, p, n);
        if (chars < 0) {
          const msg = `Could not decode to UTF-8 column '${columnNames(st, ncols)[i]}' with text '${lossy(p, n)}'`;
          throw new WorkerErr('QueryError', msg, { causeType: 'sqlite3.OperationalError', causeMessage: msg });
        }
        size += chars;
        if (encode) {
          if (json) jsonText(w, h, p, n);
          else {
            w.strHeader(n);
            w.bytes(h.subarray(p, p + n));
          }
        }
        break;
      }
      case 4: { // BLOB
        const p = X.sqlite3_column_blob(st, i);
        const n = X.sqlite3_column_bytes(st, i);
        size += n;
        if (encode) w.bin(heap().subarray(p, p + n));
        break;
      }
      default:
        size += 1;
        if (encode) w.nil();
    }
  }
  if (encode && json) w.byte(93);
  if (w.pos > maxFrame) throw new TooLarge();
  return size;
}

const stepOk = (rc) => rc === C.SQLITE_ROW || rc === C.SQLITE_DONE;

function getLimits(m) {
  const num = (k) => (typeof m[k] === 'number' ? m[k] : m[k] instanceof PyFloat ? m[k].v : typeof m[k] === 'bigint' ? Number(m[k]) : null);
  return { tl: num('time_limit_ms'), maxRows: num('max_rows'), maxBytes: num('max_result_bytes') };
}

/** Rows written so far into `w`, as a fragment for the reply. */
function rowsFragment(w, count) {
  if (w.json) return new Raw(w.take());
  const head = new Writer('msgpack');
  head.arrayHeader(count);
  head.bytes(w.buf.subarray(0, w.pos));
  return new Raw(head.take());
}

/** SqliteConnection._execute: the result dict (results_to_wire). */
function runExecute(wc, sql, params, lim, recover) {
  for (let attempt = 0; ; attempt++) {
    try {
      return executeOnce(wc, sql, params, lim);
    } catch (e) {
      // _execute_recovering: a hot journal left by a killed writer.
      if (recover && attempt === 0 && !wc.write && e instanceof WorkerErr && e.ext === C.SQLITE_READONLY_ROLLBACK) {
        recoverHotJournal(wc.path, wc.busyMs);
        continue;
      }
      throw e;
    }
  }
}

const LASTROWID_SENTINEL = -(2n ** 63n);

function executeOnce(wc, sql, params, lim) {
  const sqlText = typeof sql === 'string' ? sql : '';
  const kw = leadingKeyword(sqlText);
  const isWrite = kw === KW.INSERT || kw === KW.UPDATE || kw === KW.DELETE || kw === KW.REPLACE;
  const isInsert = kw === KW.INSERT || kw === KW.REPLACE;
  const dml = isDml(sqlText);
  const db = wc.db;
  const rowidBefore = X.sqlite3_last_insert_rowid(db);
  // A sentinel, not a before/after comparison (as the C worker): an insert
  // that reuses the previous insert's rowid (insert 5, delete, insert 5)
  // must report it, and an upsert that updates that row must not. Put
  // back if unused.
  if (isInsert) X.sqlite3_set_last_insert_rowid(db, LASTROWID_SENTINEL);
  let st = 0;
  let rc = C.SQLITE_DONE;
  limitInstall(wc, lim.tl);
  try {
    stmtEnter();
    st = prepare(wc, sql);
    bindParams(wc, st, params);
    if (st) {
      rc = X.sqlite3_step(st);
      if (!stepOk(rc)) throw sqliteErr(db, rc);
    }
    let lastrowid = X.sqlite3_last_insert_rowid(db);
    const ncols = st ? X.sqlite3_column_count(st) : 0;
    const out = { columns: st ? columnNames(st, ncols) : [] };
    let truncated = false;
    if (ncols > 0) {
      const w = rowsW;
      w.reset();
      if (w.json) w.byte(91);
      const hasMr = lim.maxRows !== null;
      const limit = hasMr ? lim.maxRows + 1 : -1;
      const hasMb = lim.maxBytes !== null;
      let count = 0;
      let encoded = 0;
      let size = 0;
      let batch = 0;
      while (rc === C.SQLITE_ROW && (limit < 0 || count < limit)) {
        const encode = !hasMr || count < lim.maxRows;
        if (encode && w.json && encoded) w.byte(44);
        size += emitRow(st, ncols, w, encode);
        if (encode) encoded++;
        count++;
        batch++;
        rc = X.sqlite3_step(st);
        if (!stepOk(rc)) throw sqliteErr(db, rc);
        if (hasMb && (batch === FETCH_BATCH || rc === C.SQLITE_DONE || count === limit)) {
          batch = 0;
          if (size > lim.maxBytes) throw new WorkerErr('ResourceLimitExceeded', `result exceeded max_result_bytes=${lim.maxBytes}`);
        }
      }
      if (encoded) {
        if (w.json) w.byte(93);
        out.rows = rowsFragment(w, encoded);
      }
      if (hasMr && count > lim.maxRows) {
        truncated = true;
        if (isWrite) {
          // sqlite3 reports rowcount for RETURNING statements only once
          // they have run to completion.
          while (rc === C.SQLITE_ROW) rc = X.sqlite3_step(st);
          if (!stepOk(rc)) throw sqliteErr(db, rc);
        }
      }
    }
    let rowcount = -1;
    if (dml && rc === C.SQLITE_DONE) rowcount = Number(X.sqlite3_changes64(db));
    if (isWrite && rowcount === -1) {
      // "WITH ... INSERT": sqlite3 does not see DML; ask SQLite.
      rowcount = Number(X.sqlite3_changes64(db));
      lastrowid = X.sqlite3_last_insert_rowid(db);
    }
    if (truncated) out.truncated = true;
    if (rowcount !== -1) out.rowcount = rowcount;
    if (isInsert && rowcount > 0 && lastrowid !== LASTROWID_SENTINEL) out.last_pk = intValue(lastrowid);
    return out;
  } finally {
    if (st) X.sqlite3_finalize(st);
    limitRemove(wc);
    if (isInsert && X.sqlite3_last_insert_rowid(db) === LASTROWID_SENTINEL) X.sqlite3_set_last_insert_rowid(db, rowidBefore);
  }
}

const intValue = (b) => {
  const n = Number(b);
  return Number.isSafeInteger(n) ? n : b;
};

/** Run one statement that returns no rows we keep (BEGIN, COMMIT, ...). */
function execSimple(wc, sql) {
  limitInstall(wc, null);
  let st = 0;
  try {
    stmtEnter();
    const [p, n] = utf8In(sql);
    let rc = X.sqlite3_prepare_v2(wc.db, p, n, PP_STMT, 0);
    st = rc === 0 ? peekPtr(PP_STMT) : 0;
    if (rc === 0 && st) {
      do rc = X.sqlite3_step(st);
      while (rc === C.SQLITE_ROW);
    }
    if (!stepOk(rc) && rc !== 0) throw sqliteErr(wc.db, rc);
  } finally {
    if (st) X.sqlite3_finalize(st);
    limitRemove(wc);
  }
}

/** sqlite3_exec without error reporting (cleanup paths). */
function execQuiet(db, sql) {
  const [p] = utf8In(sql);
  heap()[p + Buffer.byteLength(sql)] = 0;
  X.sqlite3_exec(db, p, 0, 0, 0);
}

/** sqlite3_busy_timeout()'s waiting (the same delays as SQLite's default
 * busy handler), except that it sleeps on flags[0]: a cancel, time limit
 * or deadline wakes it and ends the wait at once. */
const BUSY_DELAYS = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
const BUSY_TOTALS = [0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228];
const busyTimeouts = new Map(); // conn id -> ms
const busyFn = wasm.installFunction('i(pi)', (ctx, count) => {
  const timeout = busyTimeouts.get(Number(ctx)) ?? 0;
  let delay;
  let prior;
  if (count < BUSY_DELAYS.length) {
    delay = BUSY_DELAYS[count];
    prior = BUSY_TOTALS[count];
  } else {
    delay = BUSY_DELAYS[BUSY_DELAYS.length - 1];
    prior = BUSY_TOTALS[BUSY_TOTALS.length - 1] + delay * (count - (BUSY_DELAYS.length - 1));
  }
  if (prior + delay > timeout) {
    delay = timeout - prior;
    if (delay <= 0) return 0;
  }
  if (Atomics.load(flags, 0) !== 0) return 0;
  Atomics.wait(flags, 0, 0, delay);
  return Atomics.load(flags, 0) !== 0 ? 0 : 1;
});

const inTx = (wc) => !!(wc && wc.db && X.sqlite3_get_autocommit(wc.db) === 0);

// -- connections and transactions ------------------------------------------------------

const conns = new Map(); // id -> Conn
const streams = new Map(); // stream id -> Stream
let connSeq = 0;

function connFor(m, job) {
  const v = m.conn;
  if (v === undefined || v === null) {
    if (conns.size === 1) return (job.wc = conns.values().next().value);
    throw new WorkerErr('QueryError', `request has no 'conn' and the worker has ${conns.size} connections`, { code: 'PROTOCOL' });
  }
  const wc = typeof v === 'number' ? conns.get(v) : undefined;
  if (wc) return (job.wc = wc);
  if (typeof v === 'number') throw new WorkerErr('QueryError', `no connection ${v} in this worker`, { code: 'PROTOCOL' });
  throw new WorkerErr('QueryError', 'no connection in this worker', { code: 'PROTOCOL' });
}

const txNumber = (v) => (typeof v === 'string' && /^t[0-9]{1,11}$/.test(v) ? Number(v.slice(1)) : -1);
const txText = (v) => (typeof v === 'string' ? v.slice(0, 64) : typeof v === 'number' || typeof v === 'bigint' ? String(v) : 'None');

/** Server.check_tx */
function checkTx(m, wc) {
  const v = m.tx;
  if (v === undefined || v === null) return;
  const n = txNumber(v);
  if (n > 0 && n === wc.tx) return;
  if (n > 0 && wc.expired.includes(n)) throw new WorkerErr('TransactionExpired', `transaction ${txText(v)} passed its deadline and was rolled back`);
  throw new WorkerErr('TransactionClosed', `transaction ${txText(v)} is not open`);
}

function recoverHotJournal(path, busyMs) {
  if (!fs.existsSync(path + '-journal')) return;
  const db = openDb(pathAsUri(path) + '?mode=rw');
  if (db.rc === 0) {
    X.sqlite3_busy_timeout(db.db, busyMs);
    execQuiet(db.db, 'select count(*) from sqlite_master');
  }
  if (db.db) X.sqlite3_close_v2(db.db);
}

function openDb(uri) {
  const [p, n] = utf8In(uri);
  heap()[p + n] = 0;
  wasm.pokePtr(PP_DB, 0);
  const rc = X.sqlite3_open_v2(p, PP_DB, C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_URI, 0);
  return { rc, db: peekPtr(PP_DB) };
}

const SQL_LIMITS = {
  attached: C.SQLITE_LIMIT_ATTACHED, column: C.SQLITE_LIMIT_COLUMN, compound_select: C.SQLITE_LIMIT_COMPOUND_SELECT,
  expr_depth: C.SQLITE_LIMIT_EXPR_DEPTH, function_arg: C.SQLITE_LIMIT_FUNCTION_ARG, length: C.SQLITE_LIMIT_LENGTH,
  like_pattern_length: C.SQLITE_LIMIT_LIKE_PATTERN_LENGTH, sql_length: C.SQLITE_LIMIT_SQL_LENGTH,
  trigger_depth: C.SQLITE_LIMIT_TRIGGER_DEPTH, variable_number: C.SQLITE_LIMIT_VARIABLE_NUMBER,
  vdbe_op: C.SQLITE_LIMIT_VDBE_OP, worker_threads: C.SQLITE_LIMIT_WORKER_THREADS,
};

function cfgBool(cfg, k, dflt) {
  const v = cfg ? cfg[k] : undefined;
  if (v === undefined || v === null) return dflt;
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  return dflt;
}

const num = (v) => (v instanceof PyFloat ? v.v : typeof v === 'bigint' ? Number(v) : v);

function hOpen(job, m) {
  const engine = m.engine;
  if (engine !== undefined && engine !== null && engine !== SQLITE_ENGINE) {
    throw new WorkerErr('NotSupported', `the Node worker runs SQLite only; engine ${typeof engine === 'string' ? engine : '?'} is not supported`);
  }
  const path = m.path;
  if (typeof path !== 'string') throw builtinErr('KeyError', "'path'");
  const write = m.mode === 'write';
  const cfg = m.config && typeof m.config === 'object' ? m.config : {};
  if (Array.isArray(cfg.init) && cfg.init.length > 0) {
    throw new WorkerErr('NotSupported', "init hooks ('module:function') need the Python worker");
  }
  if (Array.isArray(cfg.extensions) && cfg.extensions.length > 0) {
    throw new WorkerErr('NotSupported', 'the Node worker cannot load SQLite extensions (the WebAssembly build omits load_extension)');
  }
  const limits = cfg.sql_limits && typeof cfg.sql_limits === 'object' ? cfg.sql_limits : null;
  if (limits) {
    for (const k of Object.keys(limits)) if (!(k in SQL_LIMITS)) throw builtinErr('ValueError', `unknown SQL limit '${k}'`);
  }
  const immutable = cfgBool(cfg, 'immutable', false);
  const create = cfgBool(cfg, 'create', true) && !immutable;
  const busy = typeof num(cfg.busy_timeout) === 'number' ? num(cfg.busy_timeout) : 5.0;
  const heapMb = typeof num(cfg.memory_limit_mb) === 'number' ? num(cfg.memory_limit_mb) : 0;
  if (write && immutable) throw new WorkerErr('NotSupported', 'database is immutable; writes are not allowed');
  const busyMs = Math.trunc(busy * 1000);
  if (create && !write) {
    // A read before the first write: an empty file is a valid database.
    try {
      fs.closeSync(fs.openSync(path, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY, 0o644));
    } catch (e) {
      if (e.code !== 'EEXIST') {
        throw new WorkerErr('QueryError', `worker error: OSError: [Errno ${-e.errno}] ${e.message}: '${path}'`, { code: 'WORKER' });
      }
    }
  }
  if (!write && !immutable) recoverHotJournal(path, busyMs);
  const q = write ? (create ? '?mode=rwc' : '?mode=rw') : immutable ? '?immutable=1' : '?mode=ro';
  const { rc, db } = openDb(pathAsUri(path) + q);
  if (rc !== 0) {
    const e = sqliteErr(db, rc);
    if (db) X.sqlite3_close_v2(db);
    throw e;
  }
  X.sqlite3_extended_result_codes(db, 1);
  const id = job.connId ?? ++connSeq;
  busyTimeouts.set(id, busyMs);
  X.sqlite3_busy_handler(db, busyFn, id);
  // What CPython's build of the same SQLite does by default, where the
  // WebAssembly build's compile-time defaults differ.
  X.sqlite3__wasm_db_config_ip(db, C.SQLITE_DBCONFIG_DQS_DML, 1, 0);
  X.sqlite3__wasm_db_config_ip(db, C.SQLITE_DBCONFIG_DQS_DDL, 1, 0);
  execQuiet(db, 'pragma recursive_triggers = 0');
  execQuiet(db, 'pragma cache_size = -2000');
  if (write) execQuiet(db, 'pragma page_size = 4096');
  if (heapMb > 0) execQuiet(db, `pragma hard_heap_limit = ${Math.trunc(heapMb) * 1024 * 1024}`);
  if (limits) {
    for (const [k, v] of Object.entries(limits)) if (typeof v === 'number') X.sqlite3_limit(db, SQL_LIMITS[k], v);
  }
  conns.set(id, { id, db, write, tx: 0, txSeq: 0, expired: [], path, busyMs });
  job.opened = id;
  return { id: job.id, ok: true, conn: id };
}

function closeConn(wc) {
  conns.delete(wc.id);
  busyTimeouts.delete(wc.id);
  wc.tx = 0;
  for (const [sid, s] of streams) {
    if (s.wc === wc) {
      if (s.st) X.sqlite3_finalize(s.st);
      streams.delete(sid);
    }
  }
  if (inTx(wc)) execQuiet(wc.db, 'ROLLBACK');
  X.sqlite3_close_v2(wc.db);
  wc.db = 0;
}

const replyInTx = (wc, body) => {
  if (inTx(wc)) body.in_tx = true;
  return body;
};

function hExecute(job, m, wc) {
  checkTx(m, wc);
  const result = runExecute(wc, m.sql, m.params, getLimits(m), true);
  return replyInTx(wc, { id: job.id, result });
}

function hBatch(job, m, wc) {
  checkTx(m, wc);
  const lim = getLimits(m);
  lim.maxBytes = null;
  if (!Array.isArray(m.statements)) throw builtinErr('KeyError', "'statements'");
  const results = [];
  for (const pair of m.statements) {
    if (!Array.isArray(pair) || pair.length !== 2) throw builtinErr('ValueError', 'a batch statement is [sql, params]');
    results.push(runExecute(wc, pair[0], pair[1], lim, false));
  }
  return replyInTx(wc, { id: job.id, result: results });
}

function hExecuteMany(job, m, wc) {
  checkTx(m, wc);
  const sql = m.sql;
  const dml = isDml(typeof sql === 'string' ? sql : '');
  let rowcount = dml ? 0 : -1;
  let st = 0;
  limitInstall(wc, null);
  try {
    stmtEnter();
    st = prepare(wc, sql);
    if (st && X.sqlite3_stmt_readonly(st)) throw progErr('executemany() can only execute DML statements.');
    const seq = Array.isArray(m.params_seq) ? m.params_seq : [];
    for (let i = 0; i < seq.length && st; i++) {
      X.sqlite3_reset(st);
      bindParams(wc, st, seq[i]);
      const rc = X.sqlite3_step(st);
      if (!stepOk(rc)) throw sqliteErr(wc.db, rc);
      if (rc === C.SQLITE_DONE && dml) rowcount += Number(X.sqlite3_changes64(wc.db));
    }
  } finally {
    if (st) X.sqlite3_finalize(st);
    limitRemove(wc);
  }
  return replyInTx(wc, { id: job.id, result: { rowcount } });
}

function hBegin(job, m, wc) {
  if (wc.tx !== 0) throw new WorkerErr('QueryError', 'cannot start a transaction within a transaction', { code: 'SQLITE_ERROR' });
  const mode = m.mode;
  let sql;
  if (mode === undefined || mode === null || mode === 'write') sql = 'BEGIN IMMEDIATE';
  else if (mode === 'read') sql = 'BEGIN';
  else throw builtinErr('ValueError', `unknown transaction mode '${txText(mode)}'`);
  execSimple(wc, sql);
  const tx = (wc.tx = ++wc.txSeq);
  const deadline = num(m.deadline_ms);
  job.txBegun = { conn: wc.id, tx, deadline_ms: typeof deadline === 'number' ? deadline : null };
  return { id: job.id, ok: true, tx: `t${tx}` };
}

function hCommit(job, m, wc) {
  checkTx(m, wc);
  if (inTx(wc)) execSimple(wc, 'COMMIT');
  wc.tx = 0;
  job.txEnded = wc.id;
  return { id: job.id, ok: true };
}

function hRollback(job, m, wc) {
  const n = txNumber(m.tx);
  if (n > 0 && wc.tx !== n) {
    const k = wc.expired.indexOf(n);
    if (k >= 0) {
      wc.expired.splice(k, 1);
      return { id: job.id, ok: true, expired: true };
    }
  }
  checkTx(m, wc);
  if (inTx(wc)) execSimple(wc, 'ROLLBACK');
  wc.tx = 0;
  job.txEnded = wc.id;
  return { id: job.id, ok: true };
}

function hOp(job, m, wc) {
  checkTx(m, wc);
  const name = typeof m.name === 'string' ? m.name : 'None';
  throw new WorkerErr('NotSupported', `named operation ${name} is not implemented by the Node worker (the parent runs it)`, { code: 'NOT_IMPLEMENTED' });
}

function hSchemaVersion(job, m, wc) {
  checkTx(m, wc);
  const rows = queryRows(wc.db, 'pragma schema_version');
  return replyInTx(wc, { id: job.id, result: rows[0][0] });
}

/** All rows of a small internal query, as JS values. */
function queryRows(db, sql, arg = null) {
  const [p, n] = utf8In(sql);
  let rc = X.sqlite3_prepare_v2(db, p, n, PP_STMT, 0);
  if (rc !== 0) throw sqliteErr(db, rc);
  const st = peekPtr(PP_STMT);
  const out = [];
  try {
    if (arg !== null) {
      rc = bindText(null, st, 1, arg);
      if (rc !== 0) throw sqliteErr(db, rc);
    }
    const ncols = X.sqlite3_column_count(st);
    while ((rc = X.sqlite3_step(st)) === C.SQLITE_ROW) {
      const row = new Array(ncols);
      for (let i = 0; i < ncols; i++) row[i] = columnValue(st, i);
      out.push(row);
    }
    if (rc !== C.SQLITE_DONE) throw sqliteErr(db, rc);
  } finally {
    X.sqlite3_finalize(st);
  }
  return out;
}

function columnValue(st, i) {
  switch (X.sqlite3_column_type(st, i)) {
    case 1: return intValue(X.sqlite3_column_int64(st, i));
    case 2: {
      const d = X.sqlite3_column_double(st, i);
      return Number.isInteger(d) ? new PyFloat(d) : d;
    }
    case 3: {
      const p = X.sqlite3_column_text(st, i);
      return lossy(p, X.sqlite3_column_bytes(st, i));
    }
    case 4: {
      const p = X.sqlite3_column_blob(st, i);
      return heap().slice(p, p + X.sqlite3_column_bytes(st, i));
    }
    default: return null;
  }
}

function hSchema(job, m, wc) {
  checkTx(m, wc);
  const own = X.sqlite3_get_autocommit(wc.db) !== 0;
  if (own) execSimple(wc, 'BEGIN');
  let result;
  try {
    result = readSchema((sql, arg) => queryRows(wc.db, sql, arg));
  } finally {
    if (own && inTx(wc)) execQuiet(wc.db, 'ROLLBACK');
  }
  return replyInTx(wc, { id: job.id, result });
}

// -- streams ------------------------------------------------------------------------------

/** _SqliteStream._next_chunk: true (rows written into rowsW), false (end). */
function nextChunk(s) {
  const wc = s.wc;
  const start = performance.now();
  let count = 0;
  limitInstall(wc, s.remainingMs === null ? null : Math.max(s.remainingMs, 0));
  try {
    stmtEnter();
    if (s.remainingMs !== null && s.remainingMs <= 0) throw interrupted();
    if (!s.started) {
      s.started = true;
      s.st = prepare(wc, s.sql);
      bindParams(wc, s.st, s.params);
      s.ncols = s.st ? X.sqlite3_column_count(s.st) : 0;
      if (s.st) {
        const rc = X.sqlite3_step(s.st);
        if (!stepOk(rc)) throw sqliteErr(wc.db, rc);
        s.hasRow = rc === C.SQLITE_ROW;
      }
      s.columns = s.st ? columnNames(s.st, s.ncols) : [];
      if (s.ncols === 0) return false;
    }
    const w = rowsW;
    w.reset();
    if (w.json) w.byte(91);
    let size = 0;
    while (count < s.chunkRows && s.hasRow) {
      if (count && w.json) w.byte(44);
      size += emitRow(s.st, s.ncols, w, true);
      count++;
      const rc = X.sqlite3_step(s.st);
      if (!stepOk(rc)) throw sqliteErr(wc.db, rc);
      s.hasRow = rc === C.SQLITE_ROW;
    }
    if (w.json) w.byte(93);
    if (count > 0 && s.maxBytes !== null) {
      s.bytes += size;
      if (s.bytes > s.maxBytes) throw new WorkerErr('ResourceLimitExceeded', `result exceeded max_result_bytes=${s.maxBytes}`);
    }
    s.count = count;
    return count > 0;
  } finally {
    limitRemove(wc);
    if (s.remainingMs !== null) s.remainingMs -= performance.now() - start;
  }
}

function dropStream(s) {
  streams.delete(s.id);
  if (s.st) X.sqlite3_finalize(s.st);
  s.st = 0;
}

/** Server._pump: send up to `credit` frames, then leave the cursor open. */
function pump(job, s) {
  while (s.credit > 0) {
    if (Atomics.load(flags, 0) !== 0) throw new WorkerErr('QueryInterrupted', 'query cancelled');
    const more = nextChunk(s);
    const msg = { id: s.id };
    if (s.first) {
      msg.columns = s.columns;
      s.first = false;
    }
    if (!more) {
      dropStream(s);
      msg.end = true;
      send(msg);
      return;
    }
    msg.rows = rowsFragment(rowsW, s.count);
    s.credit--;
    send(msg);
  }
}

function hStream(job, m, wc) {
  checkTx(m, wc);
  const chunk = m.chunk_rows === undefined || m.chunk_rows === null ? 1000 : num(m.chunk_rows);
  if (!(chunk >= 1)) throw builtinErr('ValueError', 'chunk_rows must be at least 1');
  const lim = getLimits(m);
  const old = streams.get(job.id);
  if (old) dropStream(old);
  const s = {
    id: job.id, wc, sql: m.sql, params: m.params, st: 0, started: false, hasRow: false, first: true,
    ncols: 0, columns: [], count: 0, credit: m.credit === undefined ? 1 : num(m.credit), chunkRows: chunk,
    remainingMs: lim.tl, maxBytes: lim.maxBytes, bytes: 0,
  };
  streams.set(job.id, s);
  job.stream = s;
  pump(job, s);
  return null;
}

function hMore(job) {
  const s = streams.get(job.stream);
  if (!s) return null; // finished or closed meanwhile
  job.wc = s.wc;
  job.stream = s;
  s.credit += job.credit;
  pump(job, s);
  return null;
}

function hCloseStream(job) {
  const s = streams.get(job.stream);
  if (!s) return null; // its last frame was already sent
  job.wc = s.wc;
  dropStream(s);
  return { id: job.id, end: true, cancelled: true };
}

function closeAll() {
  for (const wc of [...conns.values()]) closeConn(wc);
  for (const s of [...streams.values()]) dropStream(s);
}

// -- deadlines -------------------------------------------------------------------------

/** Server._expire: the deadline passed; roll back and tell the parent. */
function runExpire(job) {
  const wc = conns.get(job.conn);
  if (!wc || wc.tx !== job.tx) return;
  wc.tx = 0;
  wc.expired.push(job.tx);
  if (wc.expired.length > MAX_EXPIRED) wc.expired.shift();
  if (inTx(wc)) {
    const saved = Atomics.load(flags, 0);
    Atomics.store(flags, 0, 0);
    try {
      execSimple(wc, 'ROLLBACK');
    } catch (e) {
      console.error(`dblayer.worker: rollback at deadline failed: ${e.message}`);
    } finally {
      Atomics.store(flags, 0, saved);
    }
  }
  job.txEnded = wc.id;
  send({ type: 'expired', conn: wc.id, tx: `t${job.tx}` });
}

// -- test hooks (only with --debug) ---------------------------------------------------

const debugAllocs = [];
function hDebug(job, m) {
  const action = m.action;
  if (action === 'sleep') {
    const secs = typeof num(m.seconds) === 'number' ? num(m.seconds) : 1;
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, secs * 1000);
    return { id: job.id, ok: true };
  }
  if (action === 'oversized_frame') {
    writeAll(Buffer.concat([Buffer.from([0x80, 0, 0, 0]), Buffer.from('x'.repeat(16))]));
    return null;
  }
  if (action === 'truncated_frame') {
    writeAll(Buffer.concat([Buffer.from([0, 0, 0, 100]), Buffer.from('{"id":')]));
    job.exit = 3;
    return null;
  }
  if (action === 'garbage') {
    const junk = Buffer.from([0xff, 0xfe, ...Buffer.from(' not a valid payload')]);
    const hdr = Buffer.alloc(4);
    hdr.writeUInt32BE(junk.length);
    writeAll(Buffer.concat([hdr, junk]));
    return null;
  }
  if (action === 'alloc') {
    // WebAssembly memory outside SQLite's heap limit (malloc, not
    // sqlite3_malloc): bounded by the memory's maximum
    const mb = num(m.mb) || 0;
    let got = 0;
    for (; got < mb; got++) {
      const p = X.malloc(1048576);
      if (!p) break;
      heap().fill(1, p, p + 1048576);
      debugAllocs.push(p);
    }
    while (debugAllocs.length) X.free(debugAllocs.pop());
    if (got < mb) throw new WorkerErr('ResourceLimitExceeded', 'worker ran out of memory (memory limit)', { fatal: true });
    return { id: job.id, ok: true, allocated_mb: got };
  }
  if (action === 'exit') {
    job.exit = typeof num(m.code) === 'number' ? num(m.code) : 1;
    return null;
  }
  if (action === 'print') {
    console.error(typeof m.text === 'string' ? m.text : '');
    return { id: job.id, ok: true };
  }
  if (action === 'locks') return { id: job.id, ok: true, locks: vfs.locks() };
  throw new WorkerErr('QueryError', `unknown debug action '${txText(action)}'`, { code: 'PROTOCOL' });
}

// -- one job ----------------------------------------------------------------------------

/** Server.translate */
function translate(e) {
  const reason = Atomics.load(flags, 0);
  if (reason === 0 || e.type !== 'QueryInterrupted') return e;
  if (reason === 3) return new WorkerErr('TransactionExpired', 'transaction passed its deadline and was rolled back by the worker');
  if (reason === 2) return new WorkerErr('QueryInterrupted', 'query interrupted (time limit exceeded)');
  return new WorkerErr('QueryInterrupted', 'query cancelled');
}

function errorPayload(e) {
  const err = { type: e.type, message: e.message };
  if (e.builtin) err.builtin = true;
  else {
    if (e.code) err.code = e.code;
    if (e.causeType) err.cause = { type: e.causeType, message: e.causeMessage ?? '' };
  }
  return err;
}

const CONN_TYPES = new Set(['close', 'execute', 'execute_many', 'batch', 'stream', 'begin', 'commit', 'rollback', 'op', 'schema', 'schema_version']);

function runJob(job) {
  let m = null;
  let streaming = false;
  try {
    if (job.kind === 'expire') return runExpire(job);
    if (Atomics.load(flags, 0) !== 0) throw new WorkerErr('QueryInterrupted', 'query cancelled');
    if (job.kind === 'more') {
      streaming = true;
      return hMore(job);
    }
    if (job.kind === 'close_stream') {
      const r = hCloseStream(job);
      if (r) send(r);
      return;
    }
    m = decode(job.codec, job.payload);
    const type = m.type;
    let reply;
    if (type === 'open') reply = hOpen(job, m);
    else if (type === 'sleep') {
      closeAll();
      job.exitAfter = true;
      reply = { id: job.id, ok: true };
    } else if (type === '_debug' && debug) reply = hDebug(job, m);
    else if (CONN_TYPES.has(type)) {
      const wc = connFor(m, job);
      switch (type) {
        case 'close':
          job.wc = null;
          closeConn(wc);
          job.closed = wc.id;
          reply = { id: job.id, ok: true };
          break;
        case 'execute': reply = hExecute(job, m, wc); break;
        case 'execute_many': reply = hExecuteMany(job, m, wc); break;
        case 'batch': reply = hBatch(job, m, wc); break;
        case 'stream': streaming = true; reply = hStream(job, m, wc); break;
        case 'begin': reply = hBegin(job, m, wc); break;
        case 'commit': reply = hCommit(job, m, wc); break;
        case 'rollback': reply = hRollback(job, m, wc); break;
        case 'op': reply = hOp(job, m, wc); break;
        case 'schema': reply = hSchema(job, m, wc); break;
        default: reply = hSchemaVersion(job, m, wc);
      }
    } else {
      const t = typeof type === 'string' ? `'${type.slice(0, 64)}'` : txText(type);
      throw new WorkerErr('QueryError', `unknown message type ${t}`, { code: 'PROTOCOL' });
    }
    if (reply && !streaming && Atomics.load(flags, 0) !== 0) {
      // cancelled while it ran: the caller gets the cancellation
      throw new WorkerErr('QueryInterrupted', 'query cancelled');
    }
    if (reply) send(reply);
  } catch (e0) {
    if (e0 instanceof ParentGone) throw e0;
    let e = e0;
    if (e instanceof TooLarge) {
      e = new WorkerErr('ResourceLimitExceeded', `reply too large: frame of more than ${maxFrame} bytes exceeds the ${maxFrame}-byte frame size cap`);
    } else if (!(e instanceof WorkerErr)) {
      if (e instanceof RangeError && /memory|allocation/i.test(e.message)) {
        e = new WorkerErr('ResourceLimitExceeded', 'worker ran out of memory (memory limit)');
      } else {
        console.error(e?.stack || String(e));
        e = new WorkerErr('QueryError', `worker error: ${e?.name || 'Error'}: ${e?.message || e}`, { code: 'WORKER' });
      }
    }
    e = translate(e);
    const reply = { id: job.id ?? null, error: errorPayload(e) };
    if (job.wc && conns.has(job.wc.id) && inTx(job.wc)) reply.in_tx = true;
    if (e.fatal) {
      reply.fatal = true;
      job.exitAfter = true;
    }
    send(reply);
    if (streaming || m?.type === 'stream') {
      const s = streams.get(job.kind === 'more' ? job.stream?.id ?? job.stream : job.id);
      if (s) dropStream(s);
    }
  } finally {
    if (rowsW.buf.length > 1 << 22) rowsW.reset();
  }
}

parentPort.on('message', (job) => {
  const done = { done: true };
  if (job.codec && job.codec !== codec) setCodec(job.codec);
  if (job.maxFrameBytes) maxFrame = job.maxFrameBytes;
  try {
    if (job.kind === 'shutdown') {
      closeAll();
      done.finished = true;
    } else {
      runJob(job);
      if (job.opened) done.opened = job.opened;
      if (job.closed) done.closed = job.closed;
      if (job.txBegun) done.txBegun = job.txBegun;
      if (job.txEnded) done.txEnded = job.txEnded;
      const sid = job.kind === 'more' ? (job.stream?.id ?? job.stream) : job.id;
      const s = streams.get(sid);
      if (s) done.streamOpen = { id: s.id, conn: s.wc.id };
      else done.streamClosed = sid;
      if (job.exit !== undefined) done.exit = job.exit;
      else if (job.exitAfter) {
        closeAll();
        done.exit = 0;
      }
      if (job.closed) done.finished = true;
    }
  } catch (e) {
    if (e instanceof ParentGone) {
      done.exit = 0;
    } else {
      console.error(e?.stack || String(e));
      done.exit = 70;
    }
  }
  parentPort.postMessage(done);
});

parentPort.postMessage({
  ready: true,
  sqliteVersion: capi.sqlite3_libversion(),
  times: { thread_start: T_START, imported: T_IMPORTED, ready: T_READY },
});
