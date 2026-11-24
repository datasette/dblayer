/* The only SQLite VFS in the WebAssembly build (SQLITE_OS_OTHER): every file
 * operation and every lock is a host function (wasm_host.h). Version 1 I/O
 * methods, so no shared memory and no mmap: WAL is not available, and
 * SQLite refuses to open a database whose header says WAL unless
 * locking_mode=EXCLUSIVE. Rollback-journal modes work as natively. */
#define _GNU_SOURCE
#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "sqlite3.h"
#include "wasm_host.h"

typedef struct {
    sqlite3_file base;
    int32_t h;
    int lock;
} hfile;

static int h_close(sqlite3_file *f) {
    hfile *p = (hfile *)f;
    int32_t rc = host_close(p->h);
    p->h = -1;
    return rc < 0 ? -rc : SQLITE_OK;
}

static int h_read(sqlite3_file *f, void *buf, int n, sqlite3_int64 off) {
    hfile *p = (hfile *)f;
    int32_t got = host_read(p->h, buf, n, off);
    if (got < 0)
        return -got;
    if (got < n) {
        memset((char *)buf + got, 0, (size_t)(n - got));
        return SQLITE_IOERR_SHORT_READ;
    }
    return SQLITE_OK;
}

static int h_write(sqlite3_file *f, const void *buf, int n, sqlite3_int64 off) {
    int32_t rc = host_write(((hfile *)f)->h, buf, n, off);
    return rc < 0 ? -rc : SQLITE_OK;
}

static int h_truncate(sqlite3_file *f, sqlite3_int64 size) {
    int32_t rc = host_truncate(((hfile *)f)->h, size);
    return rc < 0 ? -rc : SQLITE_OK;
}

static int h_sync(sqlite3_file *f, int flags) {
    int32_t rc = host_sync(((hfile *)f)->h, flags);
    return rc < 0 ? -rc : SQLITE_OK;
}

static int h_size(sqlite3_file *f, sqlite3_int64 *size) {
    int64_t s = host_size(((hfile *)f)->h);
    if (s < 0)
        return (int)-s;
    *size = s;
    return SQLITE_OK;
}

static int h_lock(sqlite3_file *f, int level) {
    hfile *p = (hfile *)f;
    if (p->lock >= level)
        return SQLITE_OK;
    int32_t rc = host_lock(p->h, level);
    if (rc == SQLITE_OK)
        p->lock = level;
    return rc;
}

static int h_unlock(sqlite3_file *f, int level) {
    hfile *p = (hfile *)f;
    if (p->lock <= level)
        return SQLITE_OK;
    int32_t rc = host_unlock(p->h, level);
    if (rc == SQLITE_OK)
        p->lock = level;
    return rc;
}

static int h_check_reserved(sqlite3_file *f, int *out) {
    int32_t rc = host_check_reserved(((hfile *)f)->h);
    if (rc < 0)
        return -rc;
    *out = rc;
    return SQLITE_OK;
}

static int h_file_control(sqlite3_file *f, int op, void *arg) {
    hfile *p = (hfile *)f;
    switch (op) {
    case SQLITE_FCNTL_LOCKSTATE:
        *(int *)arg = p->lock;
        return SQLITE_OK;
    case SQLITE_FCNTL_VFSNAME:
        *(char **)arg = sqlite3_mprintf("%s", "dblayer-host");
        return SQLITE_OK;
    case SQLITE_FCNTL_POWERSAFE_OVERWRITE:
        if (*(int *)arg < 0)
            *(int *)arg = 1;
        return SQLITE_OK;
    case SQLITE_FCNTL_HAS_MOVED:
        *(int *)arg = 0;
        return SQLITE_OK;
    default:
        return SQLITE_NOTFOUND;
    }
}

/* As the unix VFS's defaults. */
static int h_sector_size(sqlite3_file *f) { (void)f; return 4096; }
static int h_device_characteristics(sqlite3_file *f) { (void)f; return SQLITE_IOCAP_POWERSAFE_OVERWRITE; }

static const sqlite3_io_methods H_METHODS = {
    1, h_close, h_read, h_write, h_truncate, h_sync, h_size, h_lock, h_unlock,
    h_check_reserved, h_file_control, h_sector_size, h_device_characteristics,
    0, 0, 0, 0, 0, 0,
};

static int v_open(sqlite3_vfs *vfs, sqlite3_filename name, sqlite3_file *f, int flags, int *out_flags) {
    (void)vfs;
    hfile *p = (hfile *)f;
    memset(p, 0, sizeof *p);
    p->h = -1;
    int32_t got = flags;
    int32_t h = host_open(name, name ? (int32_t)strlen(name) : 0, flags, &got);
    if (h < 0)
        return -h;
    p->h = h;
    p->base.pMethods = &H_METHODS;
    if (out_flags)
        *out_flags = got;
    return SQLITE_OK;
}

static int v_delete(sqlite3_vfs *vfs, const char *name, int sync_dir) {
    (void)vfs;
    int32_t rc = host_delete(name, (int32_t)strlen(name), sync_dir);
    return rc < 0 ? -rc : SQLITE_OK;
}

static int v_access(sqlite3_vfs *vfs, const char *name, int flags, int *out) {
    (void)vfs;
    int32_t rc = host_access(name, (int32_t)strlen(name), flags);
    if (rc < 0)
        return -rc;
    *out = rc;
    return SQLITE_OK;
}

static int v_full_pathname(sqlite3_vfs *vfs, const char *name, int n, char *out) {
    (void)vfs;
    /* The host only hands out absolute paths and checks every one. */
    sqlite3_snprintf(n, out, "%s", name);
    return SQLITE_OK;
}

static int v_randomness(sqlite3_vfs *vfs, int n, char *out) {
    (void)vfs;
    int done = 0;
    while (done < n) {
        int k = n - done > 256 ? 256 : n - done;
        if (getentropy(out + done, (size_t)k) != 0)
            break;
        done += k;
    }
    return done;
}

static int v_sleep(sqlite3_vfs *vfs, int micros) {
    (void)vfs;
    host_sleep(micros);
    return micros;
}

static int v_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out) {
    (void)vfs;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    *out = (sqlite3_int64)210866760000000LL + (sqlite3_int64)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    return SQLITE_OK;
}

static int v_current_time(sqlite3_vfs *vfs, double *out) {
    sqlite3_int64 ms;
    v_current_time_int64(vfs, &ms);
    *out = (double)ms / 86400000.0;
    return SQLITE_OK;
}

static int v_get_last_error(sqlite3_vfs *vfs, int n, char *out) {
    (void)vfs;
    (void)n;
    (void)out;
    return errno;
}

static sqlite3_vfs H_VFS = {
    2, sizeof(hfile), 512, 0, "dblayer-host", 0,
    v_open, v_delete, v_access, v_full_pathname,
    0, 0, 0, 0, /* no dlopen */
    v_randomness, v_sleep, v_current_time, v_get_last_error, v_current_time_int64,
    0, 0, 0,
};

int dbl_vfs_register(void) { return sqlite3_vfs_register(&H_VFS, 1); }

/* SQLITE_OS_OTHER: SQLite calls these from sqlite3_initialize(). */
int sqlite3_os_init(void) { return dbl_vfs_register(); }
int sqlite3_os_end(void) { return SQLITE_OK; }
