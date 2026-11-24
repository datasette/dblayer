// The cross-process guard: while this process has a database open, other
// processes cannot open it.
//
// Node has no API for the POSIX advisory (fcntl) locks native SQLite uses,
// so the VFS's locks (locks.mjs) are invisible outside this process, and a
// native writer next to us could have its live journal "recovered" and the
// file corrupted. Node does ship a native SQLite, though: `node:sqlite`. The
// main thread opens the file with it and holds `BEGIN EXCLUSIVE` (with
// `journal_mode=MEMORY`, so it never touches the -journal file). That is an
// EXCLUSIVE fcntl lock owned by this process: every other SQLite (the
// `sqlite3` shell, Python's sqlite3, another worker) gets SQLITE_BUSY, and
// our own WebAssembly connections, which take no fcntl locks, are not
// affected (fcntl locks never conflict within one process). The native
// connection reads the file header and rolls back a hot journal it finds;
// it never reads or writes data.
//
// POSIX drops every lock a process holds on a file when the process closes
// *any* descriptor of it. So while a guard is held, the VFS does not close
// main-database descriptors: it parks them here (`park`), and they are
// closed after the guard is released, when the last connection on the
// file has closed.
//
// No guard: an immutable open, a database in WAL mode (the VFS cannot open
// one anyway), a file that does not exist and may not be created, or
// DBLAYER_NODEWORKER_GUARD=0 (tests that reproduce the hazard).

import fs from 'node:fs';
import nodePath from 'node:path';

export const ENABLED = process.env.DBLAYER_NODEWORKER_GUARD !== '0';

let DatabaseSync = null;

function sqliteModule() {
  if (DatabaseSync) return DatabaseSync;
  // node:sqlite prints an ExperimentalWarning on first load; it is not ours to show
  const emit = process.emitWarning;
  process.emitWarning = () => {};
  try {
    DatabaseSync = process.getBuiltinModule('node:sqlite').DatabaseSync;
  } finally {
    process.emitWarning = emit;
  }
  return DatabaseSync;
}

/** path -> { db, holders: Set, parked: number[], mode } */
const guards = new Map();

const isBusy = (e) => /SQLITE_BUSY|database is locked/i.test(`${e?.code} ${e?.errcode} ${e?.errstr} ${e?.message}`);

function isWal(path) {
  let fd;
  try {
    fd = fs.openSync(path, 'r');
    const hdr = Buffer.alloc(20);
    const n = fs.readSync(fd, hdr, 0, 20, 0);
    return n === 20 && (hdr[18] === 2 || hdr[19] === 2);
  } catch {
    return false;
  } finally {
    // no guard on this file yet (we are deciding whether to take one), so
    // closing this descriptor drops nothing
    if (fd !== undefined) fs.closeSync(fd);
  }
}

function tryTake(path, readOnly) {
  const Db = sqliteModule();
  const db = new Db(path, { timeout: 0, readOnly });
  try {
    db.exec('PRAGMA journal_mode = MEMORY');
    if (readOnly) {
      // a read-only file: a SHARED lock (writers elsewhere cannot commit)
      db.exec('BEGIN');
      db.prepare('SELECT count(*) FROM sqlite_master').get();
      return { db, mode: 'shared' };
    }
    db.exec('BEGIN EXCLUSIVE');
    return { db, mode: 'exclusive' };
  } catch (e) {
    db.close();
    throw e;
  }
}

/** Take the guard for `path` (or find it held) for an `open`. Resolves to
 * null on success (or when no guard applies), else an error payload. */
export async function acquire(path, cfg) {
  if (!ENABLED || typeof path !== 'string' || path === '' || path === ':memory:') return null;
  const key = nodePath.resolve(path);
  if (guards.has(key)) return null;
  const immutable = cfg?.immutable === true || cfg?.immutable === 1;
  const create = cfg?.create === undefined || cfg?.create === null ? true : !!cfg.create;
  if (immutable) return null;
  const exists = fs.existsSync(key);
  if (!exists && !create) return null;
  if (exists && isWal(key)) return null;
  let readOnly = false;
  if (exists) {
    try {
      fs.accessSync(key, fs.constants.W_OK);
    } catch {
      readOnly = true;
    }
  }
  const busyS = typeof cfg?.busy_timeout === 'number' ? cfg.busy_timeout : 5.0;
  const end = performance.now() + Math.max(0, busyS * 1000);
  for (let wait = 1; ; wait = Math.min(wait * 2, 50)) {
    if (guards.has(key)) return null; // taken by an open that raced this one
    try {
      const { db, mode } = tryTake(key, readOnly);
      guards.set(key, { db, mode, holders: new Set(), parked: [] });
      return null;
    } catch (e) {
      if (!isBusy(e)) {
        console.error(`dblayer-nodeworker: no cross-process guard on ${key}: ${e?.message || e}`);
        return null;
      }
      if (performance.now() >= end) {
        const message = 'database is locked';
        return {
          type: 'QueryError',
          message: `${message} (another process has the file open; a Node worker needs it to itself)`,
          code: 'SQLITE_BUSY',
          cause: { type: 'sqlite3.OperationalError', message },
        };
      }
    }
    await new Promise((r) => setTimeout(r, wait));
  }
}

/** The executor `holder` serves a connection on `path`. */
export function hold(path, holder) {
  const g = typeof path === 'string' ? guards.get(nodePath.resolve(path)) : undefined;
  if (g) g.holders.add(holder);
}

/** `holder` is gone: release every guard it was the last holder of. */
export function drop(holder) {
  for (const [key, g] of guards) {
    if (!g.holders.delete(holder) || g.holders.size > 0) continue;
    release(key, g);
  }
}

function release(key, g) {
  guards.delete(key);
  try {
    g.db.exec('ROLLBACK');
  } catch {
    // nothing to roll back
  }
  try {
    g.db.close();
  } catch {
    // closing releases the lock regardless
  }
  for (const fd of g.parked) closeFd(fd);
}

function closeFd(fd) {
  try {
    fs.closeSync(fd);
  } catch {
    // already closed
  }
}

/** A main-database descriptor the VFS of an executor thread is done with. */
export function park(path, fd) {
  const g = guards.get(nodePath.resolve(path));
  if (g) g.parked.push(fd);
  else closeFd(fd);
}

/** For tests and debugging: path -> lock mode and parked descriptors. */
export function dump() {
  return Object.fromEntries([...guards].map(([k, g]) => [k, { mode: g.mode, holders: g.holders.size, parked: g.parked.length }]));
}
