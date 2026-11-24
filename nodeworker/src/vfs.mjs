// "nodefs": a SQLite VFS over Node's synchronous fs calls, installed into
// the official SQLite WebAssembly build (which has no file VFS for Node).
//
// Files are real files: other processes and native SQLite can open them.
// Locking is NOT shared with other processes. Node has no API for the
// POSIX advisory (fcntl) locks native SQLite uses, so this VFS keeps
// SQLite's lock states (SHARED, RESERVED, PENDING, EXCLUSIVE) in a table in
// shared memory (locks.mjs), with os_unix.c's in-process rules: the
// connections of one worker process (one executor thread each) exclude
// each other correctly, and nothing outside the process sees them. Hence
// the backend's rule that one Node process owns a database file while it
// is open (see dblayer/backends/sqlite_nodeworker.py).
//
// No shared memory for WAL (xShm*), so WAL mode is unavailable:
// `journal_mode=wal` leaves the database in rollback-journal mode, and a
// database already in WAL mode cannot be read.

import fs from 'node:fs';
import os from 'node:os';
import nodePath from 'node:path';
import { randomFillSync } from 'node:crypto';
import { openHandle, closeHandle, lock as lockH, unlock as unlockH, reserved, dump } from './locks.mjs';

export function installNodeVfs(sqlite3, { name = 'nodefs', lockTable, threadId }) {
  const { capi, wasm } = sqlite3;
  const C = capi;
  const mem = () => wasm.heap8u();
  const files = new Map(); // pFile -> file
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  let lastError = 0;

  const ioErr = (e, code) => {
    lastError = e?.errno ? -e.errno : 0;
    return code;
  };

  const syncDir = (path) => {
    try {
      const fd = fs.openSync(nodePath.dirname(path), 'r');
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    } catch {
      // as os_unix.c: a directory that cannot be synced is not an error
    }
  };

  const io = new C.sqlite3_io_methods();
  io.$iVersion = 1;
  const ioMethods = {
    xClose(pFile) {
      const f = files.get(pFile);
      if (!f) return 0;
      files.delete(pFile);
      if (f.lock) closeHandle(f.lock);
      try {
        fs.closeSync(f.fd);
        if (f.deleteOnClose) fs.rmSync(f.path, { force: true });
      } catch (e) {
        return ioErr(e, C.SQLITE_IOERR_CLOSE);
      }
      return 0;
    },
    xRead(pFile, pDest, n, offset64) {
      const f = files.get(pFile);
      const dest = Number(pDest);
      const off = Number(offset64);
      let got = 0;
      try {
        while (got < n) {
          const k = fs.readSync(f.fd, mem(), dest + got, n - got, off + got);
          if (k === 0) break;
          got += k;
        }
      } catch (e) {
        return ioErr(e, C.SQLITE_IOERR_READ);
      }
      if (got < n) {
        mem().fill(0, dest + got, dest + n);
        return C.SQLITE_IOERR_SHORT_READ;
      }
      return 0;
    },
    xWrite(pFile, pSrc, n, offset64) {
      const f = files.get(pFile);
      const src = Number(pSrc);
      const off = Number(offset64);
      let put = 0;
      try {
        while (put < n) put += fs.writeSync(f.fd, mem(), src + put, n - put, off + put);
      } catch (e) {
        return ioErr(e, e.code === 'ENOSPC' ? C.SQLITE_FULL : C.SQLITE_IOERR_WRITE);
      }
      return 0;
    },
    xTruncate(pFile, size64) {
      try {
        fs.ftruncateSync(files.get(pFile).fd, Number(size64));
      } catch (e) {
        return ioErr(e, C.SQLITE_IOERR_TRUNCATE);
      }
      return 0;
    },
    xSync(pFile, flags) {
      const f = files.get(pFile);
      try {
        if (flags & C.SQLITE_SYNC_DATAONLY) fs.fdatasyncSync(f.fd);
        else fs.fsyncSync(f.fd);
      } catch (e) {
        return ioErr(e, C.SQLITE_IOERR_FSYNC);
      }
      if (f.syncDir) {
        f.syncDir = false;
        syncDir(f.path);
      }
      return 0;
    },
    xFileSize(pFile, pSize64) {
      try {
        wasm.poke64(pSize64, BigInt(fs.fstatSync(files.get(pFile).fd).size));
      } catch (e) {
        return ioErr(e, C.SQLITE_IOERR_FSTAT);
      }
      return 0;
    },
    xLock(pFile, level) {
      const f = files.get(pFile);
      return f.lock ? lockH(f.lock, level) : 0;
    },
    xUnlock(pFile, level) {
      const f = files.get(pFile);
      return f.lock ? unlockH(f.lock, level) : 0;
    },
    xCheckReservedLock(pFile, pOut) {
      const f = files.get(pFile);
      wasm.poke32(pOut, f.lock && reserved(f.lock) ? 1 : 0);
      return 0;
    },
    xFileControl() {
      return C.SQLITE_NOTFOUND;
    },
    xSectorSize() {
      return 4096;
    },
    xDeviceCharacteristics() {
      return C.SQLITE_IOCAP_POWERSAFE_OVERWRITE;
    },
  };

  const vfs = new C.sqlite3_vfs();
  vfs.$iVersion = 2;
  vfs.$szOsFile = C.sqlite3_file.structInfo.sizeof;
  vfs.$mxPathname = 1024;
  let tempSeq = 0;
  const vfsMethods = {
    xOpen(pVfs, zName, pFile, flags, pOutFlags) {
      wasm.poke32(pFile, 0); // pMethods = NULL until opened
      let path;
      let deleteOnClose = !!(flags & C.SQLITE_OPEN_DELETEONCLOSE);
      if (zName) {
        path = wasm.cstrToJs(zName);
      } else {
        path = nodePath.join(os.tmpdir(), `dblayer-nodeworker-${process.pid}-${++tempSeq}-${Date.now()}.tmp`);
        deleteOnClose = true;
      }
      const create = !!(flags & C.SQLITE_OPEN_CREATE);
      let fd;
      let outFlags = flags;
      try {
        if (flags & C.SQLITE_OPEN_READWRITE) {
          let oflags = fs.constants.O_RDWR;
          if (create) oflags |= fs.constants.O_CREAT;
          if (flags & C.SQLITE_OPEN_EXCLUSIVE) oflags |= fs.constants.O_EXCL;
          try {
            fd = fs.openSync(path, oflags, 0o644);
          } catch (e) {
            if (e.code !== 'EACCES' && e.code !== 'EROFS' && e.code !== 'EPERM') throw e;
            // as os_unix.c: fall back to read-only, and say so
            fd = fs.openSync(path, fs.constants.O_RDONLY);
            outFlags = (flags & ~(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE)) | C.SQLITE_OPEN_READONLY;
          }
        } else {
          fd = fs.openSync(path, fs.constants.O_RDONLY);
        }
      } catch (e) {
        return ioErr(e, C.SQLITE_CANTOPEN);
      }
      const isJournal = !!(flags & (C.SQLITE_OPEN_MAIN_JOURNAL | C.SQLITE_OPEN_SUPER_JOURNAL | C.SQLITE_OPEN_WAL));
      let handle = null;
      if (flags & C.SQLITE_OPEN_MAIN_DB) {
        handle = openHandle(lockTable, nodePath.resolve(path), threadId);
        if (handle === null) {
          fs.closeSync(fd);
          console.error('dblayer-nodeworker: the lock table is full');
          return C.SQLITE_CANTOPEN;
        }
      }
      const f = { fd, path, deleteOnClose, syncDir: create && isJournal, lock: handle };
      files.set(pFile, f);
      wasm.poke32(pFile, io.pointer);
      if (pOutFlags) wasm.poke32(pOutFlags, outFlags);
      return 0;
    },
    xDelete(pVfs, zName, doSyncDir) {
      const path = wasm.cstrToJs(zName);
      try {
        fs.unlinkSync(path);
      } catch (e) {
        if (e.code === 'ENOENT') return C.SQLITE_IOERR_DELETE_NOENT;
        return ioErr(e, C.SQLITE_IOERR_DELETE);
      }
      if (doSyncDir) syncDir(path);
      return 0;
    },
    xAccess(pVfs, zName, flags, pOut) {
      const path = wasm.cstrToJs(zName);
      let ok = 0;
      try {
        if (flags === C.SQLITE_ACCESS_EXISTS) {
          const st = fs.statSync(path);
          ok = !st.isFile() || st.size > 0 ? 1 : 0;
        } else {
          fs.accessSync(path, flags === C.SQLITE_ACCESS_READWRITE ? fs.constants.R_OK | fs.constants.W_OK : fs.constants.R_OK);
          ok = 1;
        }
      } catch {
        ok = 0;
      }
      wasm.poke32(pOut, ok);
      return 0;
    },
    xFullPathname(pVfs, zName, nOut, pOut) {
      const full = Buffer.from(nodePath.resolve(wasm.cstrToJs(zName)), 'utf8');
      if (full.length >= nOut) return C.SQLITE_CANTOPEN;
      const h = mem();
      h.set(full, Number(pOut));
      h[Number(pOut) + full.length] = 0;
      return 0;
    },
    xRandomness(pVfs, nOut, pOut) {
      randomFillSync(mem(), Number(pOut), nOut);
      return nOut;
    },
    xSleep(pVfs, micros) {
      Atomics.wait(sleeper, 0, 0, Math.max(micros / 1000, 0));
      return micros;
    },
    xCurrentTime(pVfs, pOut) {
      wasm.poke64f(pOut, 2440587.5 + Date.now() / 864e5);
      return 0;
    },
    xCurrentTimeInt64(pVfs, pOut) {
      wasm.poke64(pOut, 210866760000000n + BigInt(Date.now()));
      return 0;
    },
    xGetLastError(pVfs, nOut, pOut) {
      if (nOut > 0) wasm.poke8(pOut, 0);
      return lastError;
    },
  };
  sqlite3.vfs.installVfs({
    io: { struct: io, methods: ioMethods },
    vfs: { struct: vfs, methods: vfsMethods, name, asDefault: true },
  });
  return {
    name,
    /** Lock levels held, by path (for tests and debugging). */
    locks: () => dump(lockTable),
  };
}
