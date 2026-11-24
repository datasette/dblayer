// SQLite's file lock states (os_unix.c's in-process half), in shared memory
// so every executor thread of this process sees the same table.
//
// Node has no API for POSIX advisory locks, so these locks are visible
// only inside this process. Each executor thread runs one connection in
// its own WebAssembly instance; their file handles take holder slots in the
// entry for the file's path. A 32-bit hash identifies the path: a
// collision makes two files share lock state, which can only cause
// spurious SQLITE_BUSY, never two writers.
//
// Layout (Int32Array): [0] table mutex, then ENTRIES entries of
//   [hash, refs, owner x HOLDERS, level x HOLDERS]
// owner is the holding thread's id + 1 (0: free slot).

export const NONE = 0;
export const SHARED = 1;
export const RESERVED = 2;
export const PENDING = 3;
export const EXCLUSIVE = 4;

const ENTRIES = 64;
const HOLDERS = 32;
const ENTRY = 2 + 2 * HOLDERS;
const BUSY = 5; // SQLITE_BUSY

export function newLockTable() {
  return new Int32Array(new SharedArrayBuffer(4 * (1 + ENTRIES * ENTRY)));
}

function hashPath(path) {
  let h = 0x811c9dc5;
  for (let i = 0; i < path.length; i++) {
    h ^= path.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h | 1; // never 0: 0 marks a free entry
}

function acquire(t) {
  for (let spins = 0; Atomics.compareExchange(t, 0, 0, 1) !== 0; spins++) {
    if (spins > 100) Atomics.wait(t, 0, 1, 1);
  }
}

function release(t) {
  Atomics.store(t, 0, 0);
  Atomics.notify(t, 0, 1);
}

const base = (e) => 1 + e * ENTRY;

/** A lock handle for one open file: claims a holder slot in the path's
 * entry. Returns null if the table is full (the caller then fails the
 * open). */
export function openHandle(t, path, threadId) {
  const hash = hashPath(path);
  acquire(t);
  try {
    let entry = -1;
    let free = -1;
    for (let e = 0; e < ENTRIES; e++) {
      const h = t[base(e)];
      if (h === hash) { entry = e; break; }
      if (h === 0 && free < 0) free = e;
    }
    if (entry < 0) {
      if (free < 0) return null;
      entry = free;
      t[base(entry)] = hash;
      t[base(entry) + 1] = 0;
    }
    const b = base(entry);
    for (let k = 0; k < HOLDERS; k++) {
      if (t[b + 2 + k] === 0) {
        t[b + 2 + k] = threadId + 1;
        t[b + 2 + HOLDERS + k] = NONE;
        t[b + 1]++;
        return { t, b, k };
      }
    }
    if (t[b + 1] === 0) t[b] = 0;
    return null;
  } finally {
    release(t);
  }
}

export function closeHandle(h) {
  const { t, b, k } = h;
  acquire(t);
  try {
    freeSlot(t, b, k);
  } finally {
    release(t);
  }
}

function freeSlot(t, b, k) {
  if (t[b + 2 + k] === 0) return;
  t[b + 2 + k] = 0;
  t[b + 2 + HOLDERS + k] = NONE;
  if (--t[b + 1] <= 0) {
    t[b + 1] = 0;
    t[b] = 0;
  }
}

/** Release every slot held by a thread that is gone (terminated). */
export function releaseThread(t, threadId) {
  acquire(t);
  try {
    for (let e = 0; e < ENTRIES; e++) {
      const b = base(e);
      if (t[b] === 0) continue;
      for (let k = 0; k < HOLDERS; k++) if (t[b + 2 + k] === threadId + 1) freeSlot(t, b, k);
    }
  } finally {
    release(t);
  }
}

/** os_unix.c's unixLock(), without the fcntl() half. */
export function lock(h, level) {
  const { t, b, k } = h;
  acquire(t);
  try {
    const mine = t[b + 2 + HOLDERS + k];
    if (mine >= level) return 0;
    let maxOther = NONE;
    let sharedOthers = 0;
    for (let j = 0; j < HOLDERS; j++) {
      if (j === k || t[b + 2 + j] === 0) continue;
      const l = t[b + 2 + HOLDERS + j];
      if (l > maxOther) maxOther = l;
      if (l >= SHARED) sharedOthers++;
    }
    if (maxOther > mine && (maxOther >= PENDING || level > SHARED)) return BUSY;
    if (level === EXCLUSIVE && sharedOthers > 0) {
      // others still read: hold PENDING, so no new reader starts, and let
      // the busy handler retry
      t[b + 2 + HOLDERS + k] = PENDING;
      return BUSY;
    }
    t[b + 2 + HOLDERS + k] = level;
    return 0;
  } finally {
    release(t);
  }
}

export function unlock(h, level) {
  const { t, b, k } = h;
  acquire(t);
  try {
    if (t[b + 2 + HOLDERS + k] > level) t[b + 2 + HOLDERS + k] = level;
    return 0;
  } finally {
    release(t);
  }
}

/** xCheckReservedLock: does any handle hold RESERVED or above? */
export function reserved(h) {
  const { t, b } = h;
  acquire(t);
  try {
    for (let j = 0; j < HOLDERS; j++) if (t[b + 2 + j] !== 0 && t[b + 2 + HOLDERS + j] > SHARED) return true;
    return false;
  } finally {
    release(t);
  }
}

/** Every held lock, for tests: [{hash, levels}]. */
export function dump(t) {
  const out = [];
  acquire(t);
  try {
    for (let e = 0; e < ENTRIES; e++) {
      const b = base(e);
      if (t[b] === 0) continue;
      const levels = [];
      for (let k = 0; k < HOLDERS; k++) if (t[b + 2 + k] !== 0) levels.push([t[b + 2 + k] - 1, t[b + 2 + HOLDERS + k]]);
      out.push({ hash: t[b], levels });
    }
  } finally {
    release(t);
  }
  return out;
}
