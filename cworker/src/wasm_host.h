/* Functions the host (dblayer.backends.sqlite_wasm, in Python) provides to
 * the WebAssembly build, imported from module "dblayer". All file access
 * goes through them: the module has no WASI preopened directory at all.
 *
 * The host owns every file descriptor and implements SQLite's locking with
 * real fcntl() byte-range locks on the same bytes the native unix VFS uses,
 * so other processes running ordinary SQLite see and respect them. See
 * docs/sqlite-wasm.md. */
#ifndef DBLAYER_WASM_HOST_H
#define DBLAYER_WASM_HOST_H

#include <stdint.h>

#define DBL_IMPORT(name) __attribute__((import_module("dblayer"), import_name(name)))

/* Open `path` (NULL / 0: an anonymous temporary file) with SQLite open
 * flags. Returns a handle >= 0, or minus a SQLite error code. *out_flags
 * gets the flags actually used (read-only fallback). Only the database file
 * the instance was created for and its "-journal" may be opened. */
DBL_IMPORT("open") int32_t host_open(const char *path, int32_t n, int32_t flags, int32_t *out_flags);
DBL_IMPORT("close") int32_t host_close(int32_t h);
/* Bytes read (short at end of file), or minus a SQLite error code. */
DBL_IMPORT("read") int32_t host_read(int32_t h, void *buf, int32_t n, int64_t off);
DBL_IMPORT("write") int32_t host_write(int32_t h, const void *buf, int32_t n, int64_t off);
DBL_IMPORT("truncate") int32_t host_truncate(int32_t h, int64_t size);
DBL_IMPORT("sync") int32_t host_sync(int32_t h, int32_t flags);
/* File size, or minus a SQLite error code. */
DBL_IMPORT("size") int64_t host_size(int32_t h);
/* SQLite lock levels (SQLITE_LOCK_*): SQLITE_OK, SQLITE_BUSY or an error. */
DBL_IMPORT("lock") int32_t host_lock(int32_t h, int32_t level);
DBL_IMPORT("unlock") int32_t host_unlock(int32_t h, int32_t level);
/* 1 if any connection, in any process, holds RESERVED or more; 0; or minus
 * an error code. */
DBL_IMPORT("check_reserved") int32_t host_check_reserved(int32_t h);
DBL_IMPORT("delete") int32_t host_delete(const char *path, int32_t n, int32_t sync_dir);
/* 1 if the file exists (and is accessible as asked), else 0. */
DBL_IMPORT("access") int32_t host_access(const char *path, int32_t n, int32_t flags);
/* Create an empty file if none exists. 0, or minus an error code. */
DBL_IMPORT("touch") int32_t host_touch(const char *path, int32_t n);
DBL_IMPORT("sleep") int32_t host_sleep(int32_t micros);

/* Registers the host VFS as the default (wasm_vfs.c). */
int dbl_vfs_register(void);

#endif
