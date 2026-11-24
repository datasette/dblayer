// Copied unchanged from nodeworker/src/io.mjs (commit fcdd9c6). This worker
// has one thread, so it never sets the write lock.
//
// Blocking writes to the protocol stream (fd 1), from either thread.
//
// process.stdout is never touched: Node would make the pipe non-blocking
// and buffer writes in the event loop. A write blocks while the parent is
// not reading, as the Python and C workers do (the parent's timers then
// kill the worker). EAGAIN (a non-blocking pipe inherited from somewhere)
// is waited out; EPIPE means the parent is gone.

import fs from 'node:fs';

export const OUT_FD = 1;
const pause = new Int32Array(new SharedArrayBuffer(4));

export class ParentGone extends Error {}

// Every thread writes whole frames under one lock (shared memory), so
// frames from different connections never interleave.
let writeLock = null;

export function newWriteLock() {
  return new Int32Array(new SharedArrayBuffer(4));
}

export function setWriteLock(lock) {
  writeLock = lock;
}

/** Take the write lock. `force` after `timeoutMs`: for the main thread
 * when the holder may have been terminated mid-write (the process is about
 * to exit anyway). */
function acquire(timeoutMs = Infinity) {
  if (writeLock === null) return;
  const end = timeoutMs === Infinity ? Infinity : performance.now() + timeoutMs;
  while (Atomics.compareExchange(writeLock, 0, 0, 1) !== 0) {
    if (performance.now() > end) return;
    Atomics.wait(writeLock, 0, 1, 5);
  }
}

function release() {
  if (writeLock === null) return;
  Atomics.store(writeLock, 0, 0);
  Atomics.notify(writeLock, 0, 1);
}

export function writeAll(buf, { fd = OUT_FD, lockTimeoutMs = Infinity } = {}) {
  acquire(lockTimeoutMs);
  try {
    let off = 0;
    while (off < buf.length) {
      try {
        off += fs.writeSync(fd, buf, off, buf.length - off);
      } catch (e) {
        if (e.code === 'EAGAIN') {
          Atomics.wait(pause, 0, 0, 1);
          continue;
        }
        if (e.code === 'EINTR') continue;
        throw new ParentGone(e.code);
      }
    }
  } finally {
    release();
  }
}

/** Point console output at stderr, so stray logging (from the SQLite
 * module's loader, say) cannot corrupt the protocol stream. */
export function consoleToStderr() {
  const err = (...args) => {
    try {
      fs.writeSync(2, args.map((a) => (typeof a === 'string' ? a : String(a))).join(' ') + '\n');
    } catch {
      // stderr closed: nothing to do
    }
  };
  console.log = err;
  console.info = err;
  console.debug = err;
  console.warn = err;
  console.error = err;
}
