/* dblayer C worker: a drop-in replacement for `python -m dblayer.worker`
 * running the SQLite engine. Speaks docs/worker-protocol.md on stdin/stdout.
 *
 *   dblayer-cworker [--memory-limit-mb N] [--debug]
 *
 * Threads
 *   main      runs requests one at a time, in arrival order, and writes replies.
 *   reader    reads and decodes frames; answers `hello`; acts on `cancel` at
 *             once; queues everything else; on end of input cancels whatever
 *             runs and exits after 5 s if the main thread has not.
 *   watchdog  transaction deadlines (rolls back and sends `expired`), the
 *             time-limit backstop, and re-interrupting a cancelled statement
 *             every 5 ms until it returns.
 *
 * Results must match the in-process backend (dblayer/backends/sqlite.py over
 * Python's sqlite3 module): rowcount, last_pk, error classes, codes and
 * messages follow CPython's _sqlite module. Named operations (`op`) and
 * Python init hooks are not supported: `hello` says "ops": false and the
 * parent runs operations itself. */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "sqlite3.h"

#include "codec.h"
#include "errnames.h"
#include "sqlutil.h"

#define PROTOCOL_VERSION 1
#define DEFAULT_MAX_FRAME ((size_t)256 * 1024 * 1024)
#define FETCH_BATCH 256
#define REINTERRUPT_NS 5000000LL
#define SHUTDOWN_GRACE_S 5
#define MAX_EXPIRED 64
#define SQLITE_ENGINE "dblayer.worker.engines:sqlite"

/* -- globals ----------------------------------------------------------------- */

static int g_out_fd = 1;
static pthread_mutex_t g_write_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_int g_codec = CODEC_JSON;
static _Atomic size_t g_max_frame = DEFAULT_MAX_FRAME;
static atomic_int g_parent_gone;
static int g_debug;
static long g_memory_limit_mb;
static int g_rlimit_as;
static int g_exit_requested;

static int64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (p == NULL) {
        fprintf(stderr, "dblayer-cworker: out of memory\n");
        _exit(70);
    }
    return p;
}

static char *xstrndup(const char *s, size_t n) {
    char *p = xmalloc(n + 1);
    memcpy(p, s, n);
    p[n] = 0;
    return p;
}

static char *xasprintf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    char *p = xmalloc((size_t)n + 1);
    va_start(ap, fmt);
    vsnprintf(p, (size_t)n + 1, fmt, ap);
    va_end(ap);
    return p;
}

/* -- errors ------------------------------------------------------------------ */

typedef struct {
    const char *type;       /* exception class name; NULL: no error */
    char *msg;
    char *code;             /* QueryError.code */
    const char *cause_type; /* the "driver exception" class */
    char *cause_msg;
    int builtin, fatal;
    int sqlite_ext;         /* extended result code, when from SQLite */
} err_t;

static void err_clear(err_t *e) {
    free(e->msg);
    free(e->code);
    free(e->cause_msg);
    memset(e, 0, sizeof *e);
}

static void err_set(err_t *e, const char *type, const char *code, const char *fmt, ...) {
    err_clear(e);
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    e->msg = xmalloc((size_t)n + 1);
    va_start(ap, fmt);
    vsnprintf(e->msg, (size_t)n + 1, fmt, ap);
    va_end(ap);
    e->type = type;
    e->code = code ? xstrndup(code, strlen(code)) : NULL;
}

static void err_cause(err_t *e, const char *ctype, const char *cmsg) {
    e->cause_type = ctype;
    free(e->cause_msg);
    e->cause_msg = xstrndup(cmsg, strlen(cmsg));
}

/* A sqlite3.ProgrammingError raised by CPython's _sqlite module. */
static void err_prog(err_t *e, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    char buf[1024];
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    err_set(e, "QueryError", NULL, "%s", buf);
    err_cause(e, "sqlite3.ProgrammingError", buf);
}

/* sqlite3.OperationalError("interrupted") raised without a SQLite code. */
static void err_interrupted(err_t *e) {
    err_set(e, "QueryInterrupted", NULL, "query interrupted (time limit exceeded or cancelled)");
    err_cause(e, "sqlite3.OperationalError", "interrupted");
}

static const char *errname(int code) {
    for (size_t i = 0; i < sizeof ERRNAMES / sizeof ERRNAMES[0]; i++)
        if (ERRNAMES[i].code == code)
            return ERRNAMES[i].name;
    return NULL;
}

/* The class CPython's _pysqlite_seterror() raises for a primary code. */
static const char *py_exc(int primary) {
    switch (primary) {
    case SQLITE_INTERNAL: case SQLITE_NOTFOUND:
        return "sqlite3.InternalError";
    case SQLITE_NOMEM:
        return "MemoryError";
    case SQLITE_ERROR: case SQLITE_PERM: case SQLITE_ABORT: case SQLITE_BUSY: case SQLITE_LOCKED:
    case SQLITE_READONLY: case SQLITE_INTERRUPT: case SQLITE_IOERR: case SQLITE_FULL:
    case SQLITE_CANTOPEN: case SQLITE_PROTOCOL: case SQLITE_EMPTY: case SQLITE_SCHEMA:
        return "sqlite3.OperationalError";
    case SQLITE_CORRUPT:
        return "sqlite3.DatabaseError";
    case SQLITE_TOOBIG:
        return "sqlite3.DataError";
    case SQLITE_CONSTRAINT: case SQLITE_MISMATCH:
        return "sqlite3.IntegrityError";
    case SQLITE_MISUSE: case SQLITE_RANGE:
        return "sqlite3.InterfaceError";
    default:
        return "sqlite3.DatabaseError";
    }
}

/* SqliteDialect.map_exception, applied to the error SQLite reports. */
static void err_sqlite(err_t *e, sqlite3 *db, int rc) {
    int ext = db ? sqlite3_extended_errcode(db) : rc;
    if (ext == SQLITE_OK || (ext & 0xff) != (rc & 0xff))
        ext = rc;
    int primary = ext & 0xff;
    const char *msg = db ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
    const char *name = errname(ext);
    const char *cls = py_exc(primary);
    char *m = xstrndup(msg, strlen(msg));
    if (primary == SQLITE_INTERRUPT) {
        err_set(e, "QueryInterrupted", NULL, "query interrupted (time limit exceeded or cancelled)");
    } else if (strcmp(cls, "sqlite3.IntegrityError") == 0) {
        err_set(e, "IntegrityError", name, "%s", m);
    } else if (ext == SQLITE_NOMEM || ext == SQLITE_TOOBIG || ext == SQLITE_FULL) {
        /* Python's sqlite3 raises MemoryError for SQLITE_NOMEM and its
         * worker then exits; here the heap limit leaves the process intact,
         * so it is an ordinary ResourceLimitExceeded. */
        err_set(e, "ResourceLimitExceeded", NULL, "%s", m);
    } else if (name && strcmp(name, "SQLITE_BUSY_SNAPSHOT") == 0) {
        err_set(e, "WriteConflict", NULL, "%s", m);
    } else {
        err_set(e, "QueryError", name, "%s", m);
    }
    err_cause(e, cls, m);
    e->sqlite_ext = ext;
    free(m);
}

/* -- output ------------------------------------------------------------------ */

static void write_all(const uint8_t *p, size_t n) {
    pthread_mutex_lock(&g_write_mu);
    while (n > 0 && !atomic_load(&g_parent_gone)) {
        ssize_t k = write(g_out_fd, p, n);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            atomic_store(&g_parent_gone, 1);
            break;
        }
        p += k;
        n -= (size_t)k;
    }
    pthread_mutex_unlock(&g_write_mu);
}

static void send_frame(wbuf *w) {
    w_finish_frame(w);
    if (!w->failed)
        write_all(w->buf, w->len);
}

/* -- state --------------------------------------------------------------------- */

enum { J_REQ, J_MORE, J_CLOSE_STREAM, J_EXPIRE, J_SHUTDOWN };
enum { R_CANCELLED, R_TIME_LIMIT, R_DEADLINE };

typedef struct conn conn;
struct conn {
    conn *next;
    int id;
    sqlite3 *db;
    int write;
    int tx;            /* open transaction number (tN), 0 if none   [g_mu] */
    int tx_seq;
    int64_t deadline_ns; /* 0: none                                  [g_mu] */
    int expired[MAX_EXPIRED];
    int nexpired;
    char *path;
    int busy_ms;
};

typedef struct job job;
struct job {
    job *next;
    int kind;
    int has_id;
    int64_t id;
    uint8_t *payload;
    arena ar;
    val msg;
    atomic_int cancelled;
    int reason;             /* [g_mu] */
    conn *wc;               /* [g_mu] */
    int active;             /* inside a statement               [g_mu] */
    int64_t backstop_ns;    /* time-limit backstop              [g_mu] */
    int64_t next_interrupt_ns;
    int64_t credit;
    int exp_conn, exp_tx;
};

typedef struct stream stream;
struct stream {
    stream *next;
    int64_t id;
    conn *wc;
    sqlite3_stmt *st;
    int started, has_row, first, ncols;
    int64_t credit, chunk_rows;
    int has_limit;
    double remaining_ms;
    int has_max;
    int64_t max_bytes, bytes;
};

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_qcond;
static pthread_cond_t g_wcond;
static job *q_head, *q_tail;
static job *g_current;
static conn *g_conns;
static stream *g_streams;
static int g_conn_seq;
static int g_eof;
static int g_started; /* the reader waits until RLIMIT_AS is set */
static pthread_cond_t g_startcond = PTHREAD_COND_INITIALIZER;
static wbuf g_w;

static void cond_init(pthread_cond_t *c) {
    pthread_condattr_t a;
    pthread_condattr_init(&a);
#ifndef __APPLE__
    pthread_condattr_setclock(&a, CLOCK_MONOTONIC);
#endif
    pthread_cond_init(c, &a);
    pthread_condattr_destroy(&a);
}

/* Wait on c (holding g_mu) until woken or `until` (monotonic ns). */
static void cond_wait_until(pthread_cond_t *c, int64_t until) {
    int64_t now = mono_ns();
    if (until <= now)
        return;
#ifdef __APPLE__
    int64_t rel = until - now;
    struct timespec ts = {(time_t)(rel / 1000000000LL), (long)(rel % 1000000000LL)};
    pthread_cond_timedwait_relative_np(c, &g_mu, &ts);
#else
    struct timespec ts = {(time_t)(until / 1000000000LL), (long)(until % 1000000000LL)};
    pthread_cond_timedwait(c, &g_mu, &ts);
#endif
}

static job *job_new(int kind) {
    job *j = xmalloc(sizeof *j);
    memset(j, 0, sizeof *j);
    j->kind = kind;
    atomic_init(&j->cancelled, 0);
    return j;
}

static void job_free(job *j) {
    arena_free(&j->ar);
    free(j->payload);
    free(j);
}

/* [g_mu] */
static void enqueue(job *j, int front) {
    if (front) {
        j->next = q_head;
        q_head = j;
        if (q_tail == NULL)
            q_tail = j;
    } else {
        j->next = NULL;
        if (q_tail)
            q_tail->next = j;
        else
            q_head = j;
        q_tail = j;
    }
    pthread_cond_signal(&g_qcond);
}

static job *dequeue(void) {
    pthread_mutex_lock(&g_mu);
    while (q_head == NULL)
        pthread_cond_wait(&g_qcond, &g_mu);
    job *j = q_head;
    q_head = j->next;
    if (q_head == NULL)
        q_tail = NULL;
    g_current = j;
    pthread_mutex_unlock(&g_mu);
    return j;
}

/* [g_mu] Cancel a job: it fails with an error for `reason`. A running
 * statement is interrupted now and every 5 ms until it returns. */
static void cancel_job(job *j, int reason) {
    atomic_store(&j->cancelled, 1);
    j->reason = reason;
    if (j == g_current && j->active && j->wc && j->wc->db) {
        sqlite3_interrupt(j->wc->db);
        j->next_interrupt_ns = mono_ns() + REINTERRUPT_NS;
        pthread_cond_signal(&g_wcond);
    }
}

static int conn_in_tx(conn *c) { return c && c->db && !sqlite3_get_autocommit(c->db); }

/* [g_mu] */
static stream *stream_find(int64_t id) {
    for (stream *s = g_streams; s; s = s->next)
        if (s->id == id)
            return s;
    return NULL;
}

static void stream_drop(stream *s) {
    pthread_mutex_lock(&g_mu);
    for (stream **pp = &g_streams; *pp; pp = &(*pp)->next)
        if (*pp == s) {
            *pp = s->next;
            break;
        }
    pthread_mutex_unlock(&g_mu);
    if (s->st)
        sqlite3_finalize(s->st);
    free(s);
}

static void stream_drop_id(int64_t id) {
    pthread_mutex_lock(&g_mu);
    stream *s = stream_find(id);
    pthread_mutex_unlock(&g_mu);
    if (s)
        stream_drop(s);
}

/* -- statements ------------------------------------------------------------- */

typedef struct {
    job *j;
    int64_t deadline_ns;
} progress_ctx;

static int progress_cb(void *p) {
    progress_ctx *c = p;
    if (atomic_load_explicit(&c->j->cancelled, memory_order_relaxed))
        return 1;
    if (c->deadline_ns && mono_ns() >= c->deadline_ns)
        return 1;
    return 0;
}

/* _TimeLimit: a progress-handler deadline (checked every instruction for
 * limits up to 20 ms, as Datasette does), which also sees cancellation. */
static void limit_install(conn *wc, progress_ctx *pc, job *j, int has_tl, int64_t tl_ms) {
    pc->j = j;
    pc->deadline_ns = has_tl ? mono_ns() + tl_ms * 1000000LL : 0;
    if (has_tl && pc->deadline_ns == 0)
        pc->deadline_ns = 1;
    sqlite3_progress_handler(wc->db, has_tl && tl_ms <= 20 ? 1 : 1000, progress_cb, pc);
}

static void limit_remove(conn *wc) { sqlite3_progress_handler(wc->db, 0, NULL, NULL); }

/* SqliteConnection._statement: refuse to start once cancelled. */
static int stmt_enter(job *j, err_t *e) {
    pthread_mutex_lock(&g_mu);
    if (atomic_load(&j->cancelled)) {
        pthread_mutex_unlock(&g_mu);
        err_interrupted(e);
        return -1;
    }
    j->active = 1;
    pthread_mutex_unlock(&g_mu);
    return 0;
}

static void stmt_leave(job *j) {
    pthread_mutex_lock(&g_mu);
    j->active = 0;
    pthread_mutex_unlock(&g_mu);
}

/* CPython's pysqlite_statement_create. *st is NULL for an empty statement. */
static int prepare(conn *wc, const val *sqlv, sqlite3_stmt **st, err_t *e) {
    *st = NULL;
    if (sqlv == NULL || sqlv->t != V_STR) {
        err_set(e, "TypeError", NULL, "sql must be a string");
        e->builtin = 1;
        return -1;
    }
    const char *sql = sqlv->u.s.p;
    size_t n = sqlv->u.s.n;
    if (memchr(sql, 0, n)) {
        err_prog(e, "the query contains a null character");
        return -1;
    }
    if ((int64_t)n > sqlite3_limit(wc->db, SQLITE_LIMIT_SQL_LENGTH, -1)) {
        err_set(e, "QueryError", NULL, "query string is too large");
        err_cause(e, "sqlite3.DataError", "query string is too large");
        return -1;
    }
    const char *tail = NULL;
    int rc = sqlite3_prepare_v2(wc->db, sql, (int)n, st, &tail);
    if (rc != SQLITE_OK) {
        err_sqlite(e, wc->db, rc);
        sqlite3_finalize(*st);
        *st = NULL;
        return -1;
    }
    if (tail && lstrip_sql(tail, sql + n) != NULL) {
        sqlite3_finalize(*st);
        *st = NULL;
        err_prog(e, "You can only execute one statement at a time.");
        return -1;
    }
    return 0;
}

static int bind_value(conn *wc, sqlite3_stmt *st, int i, const val *v, int transient, err_t *e) {
    int rc;
    sqlite3_destructor_type d = transient ? SQLITE_TRANSIENT : SQLITE_STATIC;
    switch (v->t) {
    case V_NIL: rc = sqlite3_bind_null(st, i); break;
    case V_BOOL: rc = sqlite3_bind_int64(st, i, v->u.b); break;
    case V_INT: rc = sqlite3_bind_int64(st, i, v->u.i); break;
    case V_BIGINT:
        err_set(e, "OverflowError", NULL, "Python int too large to convert to SQLite INTEGER");
        e->builtin = 1;
        return -1;
    case V_FLOAT: rc = sqlite3_bind_double(st, i, v->u.d); break;
    case V_STR: rc = sqlite3_bind_text64(st, i, v->u.s.p, v->u.s.n, d, SQLITE_UTF8); break;
    case V_BIN:
        rc = v->u.s.n ? sqlite3_bind_blob64(st, i, v->u.s.p, v->u.s.n, d) : sqlite3_bind_zeroblob(st, i, 0);
        break;
    default:
        err_prog(e, "Error binding parameter %d: type '%s' is not supported", i, v->t == V_ARR ? "list" : "dict");
        return -1;
    }
    if (rc != SQLITE_OK) {
        err_sqlite(e, wc->db, rc);
        return -1;
    }
    return 0;
}

/* CPython's bind_parameters (3.14 rules: a sequence for named
 * placeholders is an error). */
static int bind_params(conn *wc, sqlite3_stmt *st, const val *params, int transient, err_t *e) {
    int num = st ? sqlite3_bind_parameter_count(st) : 0;
    if (params == NULL || params->t == V_NIL || params->t == V_ARR) {
        size_t got = (params && params->t == V_ARR) ? params->u.a.n : 0;
        if (got != (size_t)num) {
            err_prog(e, "Incorrect number of bindings supplied. The current statement uses %d, and there are %zu supplied.",
                     num, got);
            return -1;
        }
        for (int i = 1; i <= num; i++) {
            const char *name = sqlite3_bind_parameter_name(st, i);
            if (name && name[0] != '?') {
                err_prog(e, "Binding %d ('%s') is a named parameter, but you supplied a sequence which requires "
                            "nameless (qmark) placeholders.", i, name);
                return -1;
            }
            if (bind_value(wc, st, i, &params->u.a.v[i - 1], transient, e) < 0)
                return -1;
        }
        return 0;
    }
    if (params->t != V_MAP) {
        err_prog(e, "parameters are of unsupported type");
        return -1;
    }
    for (int i = 1; i <= num; i++) {
        const char *name = sqlite3_bind_parameter_name(st, i);
        if (name == NULL) {
            err_prog(e, "Binding %d has no name, but you supplied a dictionary (which has only names).", i);
            return -1;
        }
        const val *v = map_get(params, name + 1);
        if (v == NULL) {
            err_prog(e, "You did not supply a value for binding parameter %s.", name);
            return -1;
        }
        if (bind_value(wc, st, i, v, transient, e) < 0)
            return -1;
    }
    return 0;
}

static void write_columns(wbuf *w, sqlite3_stmt *st, int ncols) {
    w_cstr(w, "columns");
    w_array(w, (uint32_t)ncols);
    for (int i = 0; i < ncols; i++) {
        const char *name = sqlite3_column_name(st, i);
        w_str_lossy(w, name ? name : "", name ? strlen(name) : 0);
    }
    w_end(w);
}

/* Convert (and, if `encode`, write) the current row as CPython would,
 * adding its size as _row_bytes() counts it. */
static int emit_row(sqlite3_stmt *st, int ncols, wbuf *w, int encode, int64_t *size, err_t *e) {
    if (encode)
        w_array(w, (uint32_t)ncols);
    for (int i = 0; i < ncols; i++) {
        switch (sqlite3_column_type(st, i)) {
        case SQLITE_INTEGER:
            *size += 8;
            if (encode) w_int(w, sqlite3_column_int64(st, i));
            break;
        case SQLITE_FLOAT:
            *size += 8;
            if (encode) w_double(w, sqlite3_column_double(st, i));
            break;
        case SQLITE_TEXT: {
            const unsigned char *p = sqlite3_column_text(st, i);
            size_t n = (size_t)sqlite3_column_bytes(st, i);
            size_t chars = 0;
            if (p == NULL)
                p = (const unsigned char *)"";
            if (!utf8_check(p, n, &chars)) {
                const char *cname = sqlite3_column_name(st, i);
                char *txt = utf8_lossy((const char *)p, n);
                char *cn = utf8_lossy(cname ? cname : "", cname ? strlen(cname) : 0);
                err_set(e, "QueryError", NULL, "Could not decode to UTF-8 column '%s' with text '%s'",
                        cn ? cn : "", txt ? txt : "");
                err_cause(e, "sqlite3.OperationalError", e->msg);
                free(txt);
                free(cn);
                return -1;
            }
            *size += (int64_t)chars;
            if (encode) w_str(w, (const char *)p, n);
            break;
        }
        case SQLITE_BLOB: {
            const void *p = sqlite3_column_blob(st, i);
            size_t n = (size_t)sqlite3_column_bytes(st, i);
            *size += (int64_t)n;
            if (encode) w_bin(w, p ? p : "", n);
            break;
        }
        default:
            *size += 1;
            if (encode) w_nil(w);
        }
    }
    if (encode)
        w_end(w);
    return 0;
}

static int step_ok(int rc) { return rc == SQLITE_ROW || rc == SQLITE_DONE; }

/* -- execute ------------------------------------------------------------------ */

static void recover_hot_journal(conn *wc);

typedef struct {
    int has_tl, has_mr, has_mb;
    int64_t tl, max_rows, max_bytes;
} limits;

static limits get_limits(const val *m) {
    limits l;
    l.tl = get_int(m, "time_limit_ms", 0, &l.has_tl);
    l.max_rows = get_int(m, "max_rows", 0, &l.has_mr);
    l.max_bytes = get_int(m, "max_result_bytes", 0, &l.has_mb);
    return l;
}

/* SqliteConnection._execute: writes the result map into w. */
static int run_execute(job *j, conn *wc, const val *sqlv, const val *params, limits lim, wbuf *w,
                       err_t *e, int recover) {
    const char *sql = sqlv && sqlv->t == V_STR ? sqlv->u.s.p : "";
    size_t n = sqlv && sqlv->t == V_STR ? sqlv->u.s.n : 0;
    int kw = leading_keyword(sql, n);
    int is_write = kw == KW_INSERT || kw == KW_UPDATE || kw == KW_DELETE || kw == KW_REPLACE;
    int is_insert = kw == KW_INSERT || kw == KW_REPLACE;
    int dml = cpython_is_dml(sql, n);
    wmark mark = w_mark(w);
    int retried = 0;
again:;
    sqlite3 *db = wc->db;
    sqlite3_int64 rowid_before = sqlite3_last_insert_rowid(db);
    progress_ctx pc;
    sqlite3_stmt *st = NULL;
    int rc = SQLITE_DONE;
    limit_install(wc, &pc, j, lim.has_tl, lim.tl);
    if (stmt_enter(j, e) < 0) {
        limit_remove(wc);
        return -1;
    }
    w_map_open(w);
    if (prepare(wc, sqlv, &st, e) < 0 || bind_params(wc, st, params, 0, e) < 0)
        goto fail;
    if (st) {
        rc = sqlite3_step(st);
        if (!step_ok(rc)) {
            err_sqlite(e, db, rc);
            goto fail;
        }
    }
    sqlite3_int64 lastrowid = sqlite3_last_insert_rowid(db);
    int ncols = st ? sqlite3_column_count(st) : 0;
    write_columns(w, st, ncols);
    int truncated = 0;
    if (ncols > 0) {
        int64_t limit = lim.has_mr ? lim.max_rows + 1 : -1;
        int64_t count = 0, size = 0, batch = 0;
        int rows_open = 0;
        while (rc == SQLITE_ROW && (limit < 0 || count < limit)) {
            int encode = !lim.has_mr || count < lim.max_rows;
            if (encode && !rows_open) {
                w_cstr(w, "rows");
                w_array_open(w);
                rows_open = 1;
            }
            if (emit_row(st, ncols, w, encode, &size, e) < 0)
                goto fail;
            count++;
            batch++;
            rc = sqlite3_step(st);
            if (!step_ok(rc)) {
                err_sqlite(e, db, rc);
                goto fail;
            }
            if (lim.has_mb && (batch == FETCH_BATCH || rc == SQLITE_DONE || count == limit)) {
                batch = 0;
                if (size > lim.max_bytes) {
                    err_set(e, "ResourceLimitExceeded", NULL, "result exceeded max_result_bytes=%lld",
                            (long long)lim.max_bytes);
                    goto fail;
                }
            }
            if (w->failed)
                goto fail;
        }
        if (rows_open)
            w_end(w);
        if (lim.has_mr && count > lim.max_rows) {
            truncated = 1;
            if (is_write) {
                /* sqlite3 reports rowcount for RETURNING statements only once
                 * they have run to completion. */
                while (rc == SQLITE_ROW)
                    rc = sqlite3_step(st);
                if (!step_ok(rc)) {
                    err_sqlite(e, db, rc);
                    goto fail;
                }
            }
        }
    }
    sqlite3_int64 rowcount = -1;
    if (dml && rc == SQLITE_DONE)
        rowcount = sqlite3_changes64(db);
    if (is_write && rowcount == -1) {
        /* "WITH ... INSERT": sqlite3 does not see DML; ask SQLite. */
        rowcount = sqlite3_changes64(db);
        lastrowid = sqlite3_last_insert_rowid(db);
    }
    sqlite3_finalize(st);
    limit_remove(wc);
    stmt_leave(j);
    if (truncated) {
        w_cstr(w, "truncated");
        w_bool(w, 1);
    }
    if (rowcount != -1) {
        w_cstr(w, "rowcount");
        w_int(w, rowcount);
    }
    if (is_insert && rowcount > 0 && lastrowid != rowid_before) {
        w_cstr(w, "last_pk");
        w_int(w, lastrowid);
    }
    w_end(w);
    return 0;
fail:
    sqlite3_finalize(st);
    limit_remove(wc);
    stmt_leave(j);
    if (recover && !retried && !wc->write && e->sqlite_ext == SQLITE_READONLY_ROLLBACK) {
        /* _execute_recovering: a hot journal left by a killed writer. */
        retried = 1;
        recover_hot_journal(wc);
        err_clear(e);
        w_rewind(w, mark);
        goto again;
    }
    return -1;
}

/* Run one statement that returns no rows we keep (BEGIN, COMMIT, ...). */
static int exec_simple(job *j, conn *wc, const char *sql, err_t *e) {
    progress_ctx pc;
    sqlite3_stmt *st = NULL;
    limit_install(wc, &pc, j, 0, 0);
    if (stmt_enter(j, e) < 0) {
        limit_remove(wc);
        return -1;
    }
    int rc = sqlite3_prepare_v2(wc->db, sql, -1, &st, NULL);
    if (rc == SQLITE_OK) {
        do
            rc = sqlite3_step(st);
        while (rc == SQLITE_ROW);
    }
    if (!step_ok(rc))
        err_sqlite(e, wc->db, rc);
    sqlite3_finalize(st);
    limit_remove(wc);
    stmt_leave(j);
    return step_ok(rc) ? 0 : -1;
}

/* -- requests ---------------------------------------------------------------- */

static void reply_begin(wbuf *w, job *j) {
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map_open(w);
    w_cstr(w, "id");
    if (j->has_id)
        w_int(w, j->id);
    else
        w_nil(w);
}

static void reply_ok(wbuf *w, job *j) {
    reply_begin(w, j);
    w_cstr(w, "ok");
    w_bool(w, 1);
}

static void reply_in_tx(wbuf *w, conn *wc) {
    if (conn_in_tx(wc)) {
        w_cstr(w, "in_tx");
        w_bool(w, 1);
    }
}

static const val *field(job *j, const char *k) { return map_get(&j->msg, k); }

static int tx_number(const val *v) {
    /* "t<N>" -> N; anything else -> -1 */
    if (v == NULL || v->t != V_STR || v->u.s.n < 2 || v->u.s.n > 12 || v->u.s.p[0] != 't')
        return -1;
    int n = 0;
    for (size_t i = 1; i < v->u.s.n; i++) {
        char c = v->u.s.p[i];
        if (c < '0' || c > '9')
            return -1;
        n = n * 10 + (c - '0');
    }
    return n;
}

static void tx_text(const val *v, char *buf, size_t n) {
    if (v && v->t == V_STR)
        snprintf(buf, n, "%.*s", (int)(v->u.s.n > 64 ? 64 : v->u.s.n), v->u.s.p);
    else if (v && v->t == V_INT)
        snprintf(buf, n, "%lld", (long long)v->u.i);
    else
        snprintf(buf, n, "%s", "None");
}

static int expired_has(conn *wc, int tx) {
    for (int i = 0; i < wc->nexpired; i++)
        if (wc->expired[i] == tx)
            return 1;
    return 0;
}

/* Server.check_tx */
static int check_tx(job *j, conn *wc, err_t *e) {
    const val *v = field(j, "tx");
    if (v == NULL || v->t == V_NIL)
        return 0;
    int n = tx_number(v);
    pthread_mutex_lock(&g_mu);
    int cur = wc->tx, exp = n > 0 && expired_has(wc, n);
    pthread_mutex_unlock(&g_mu);
    if (n > 0 && n == cur)
        return 0;
    char t[80];
    tx_text(v, t, sizeof t);
    if (exp)
        err_set(e, "TransactionExpired", NULL, "transaction %s passed its deadline and was rolled back", t);
    else
        err_set(e, "TransactionClosed", NULL, "transaction %s is not open", t);
    return -1;
}

static conn *conn_for(job *j, err_t *e) {
    const val *v = field(j, "conn");
    pthread_mutex_lock(&g_mu);
    conn *found = NULL;
    int count = 0;
    for (conn *c = g_conns; c; c = c->next) {
        count++;
        if (v && v->t == V_INT && c->id == v->u.i)
            found = c;
    }
    if ((v == NULL || v->t == V_NIL) && count == 1)
        found = g_conns;
    if (found)
        j->wc = found;
    pthread_mutex_unlock(&g_mu);
    if (found)
        return found;
    if (v == NULL || v->t == V_NIL)
        err_set(e, "QueryError", "PROTOCOL", "request has no 'conn' and the worker has %d connections", count);
    else if (v->t == V_INT)
        err_set(e, "QueryError", "PROTOCOL", "no connection %lld in this worker", (long long)v->u.i);
    else
        err_set(e, "QueryError", "PROTOCOL", "no connection in this worker");
    return NULL;
}

static void recover_hot_journal(conn *wc) {
    char *jpath = xasprintf("%s-journal", wc->path);
    struct stat sb;
    int exists = stat(jpath, &sb) == 0;
    free(jpath);
    if (!exists)
        return;
    char *base = path_as_uri(wc->path);
    char *uri = xasprintf("%s?mode=rw", base ? base : "");
    free(base);
    sqlite3 *rw = NULL;
    if (sqlite3_open_v2(uri, &rw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL) == SQLITE_OK) {
        sqlite3_busy_timeout(rw, wc->busy_ms);
        sqlite3_exec(rw, "select count(*) from sqlite_master", NULL, NULL, NULL);
    }
    sqlite3_close_v2(rw);
    free(uri);
}

static const struct { const char *name; int id; } SQL_LIMITS[] = {
    {"attached", SQLITE_LIMIT_ATTACHED}, {"column", SQLITE_LIMIT_COLUMN},
    {"compound_select", SQLITE_LIMIT_COMPOUND_SELECT}, {"expr_depth", SQLITE_LIMIT_EXPR_DEPTH},
    {"function_arg", SQLITE_LIMIT_FUNCTION_ARG}, {"length", SQLITE_LIMIT_LENGTH},
    {"like_pattern_length", SQLITE_LIMIT_LIKE_PATTERN_LENGTH}, {"sql_length", SQLITE_LIMIT_SQL_LENGTH},
    {"trigger_depth", SQLITE_LIMIT_TRIGGER_DEPTH}, {"variable_number", SQLITE_LIMIT_VARIABLE_NUMBER},
    {"vdbe_op", SQLITE_LIMIT_VDBE_OP}, {"worker_threads", SQLITE_LIMIT_WORKER_THREADS},
};

static int cfg_bool(const val *cfg, const char *k, int dflt) {
    const val *v = map_get(cfg, k);
    if (v == NULL || v->t == V_NIL)
        return dflt;
    if (v->t == V_BOOL)
        return v->u.b;
    if (v->t == V_INT)
        return v->u.i != 0;
    return dflt;
}

static int h_open(job *j, wbuf *w, err_t *e) {
    const val *engine = field(j, "engine");
    if (engine && engine->t != V_NIL && !val_is_str(engine, SQLITE_ENGINE)) {
        err_set(e, "NotSupported", NULL, "the C worker runs SQLite only; engine %.*s is not supported",
                engine->t == V_STR ? (int)engine->u.s.n : 1, engine->t == V_STR ? engine->u.s.p : "?");
        return -1;
    }
    const val *pathv = field(j, "path");
    if (pathv == NULL || pathv->t != V_STR) {
        err_set(e, "KeyError", NULL, "'path'");
        e->builtin = 1;
        return -1;
    }
    int write = val_is_str(field(j, "mode"), "write");
    const val *cfg = field(j, "config");
    const val *init = map_get(cfg, "init");
    if (init && init->t == V_ARR && init->u.a.n > 0) {
        err_set(e, "NotSupported", NULL, "init hooks ('module:function') need the Python worker");
        return -1;
    }
    const val *limits_v = map_get(cfg, "sql_limits");
    if (limits_v && limits_v->t == V_MAP) {
        for (size_t i = 0; i < limits_v->u.a.n; i++) {
            const val *k = &limits_v->u.a.v[2 * i];
            int ok = 0;
            for (size_t q = 0; q < sizeof SQL_LIMITS / sizeof SQL_LIMITS[0]; q++)
                ok |= val_is_str(k, SQL_LIMITS[q].name);
            if (!ok) {
                err_set(e, "ValueError", NULL, "unknown SQL limit '%.*s'", k->t == V_STR ? (int)k->u.s.n : 0,
                        k->t == V_STR ? k->u.s.p : "");
                e->builtin = 1;
                return -1;
            }
        }
    }
    int immutable = cfg_bool(cfg, "immutable", 0);
    int create = cfg_bool(cfg, "create", 1) && !immutable;
    double busy = 5.0;
    const val *bv = map_get(cfg, "busy_timeout");
    if (bv && bv->t == V_FLOAT) busy = bv->u.d;
    else if (bv && bv->t == V_INT) busy = (double)bv->u.i;
    int64_t heap_mb = get_int(cfg, "memory_limit_mb", 0, NULL);
    if (write && immutable) {
        err_set(e, "NotSupported", NULL, "database is immutable; writes are not allowed");
        return -1;
    }
    char *path = xstrndup(pathv->u.s.p, pathv->u.s.n);
    conn tmp = {0};
    tmp.path = path;
    tmp.busy_ms = (int)(busy * 1000);
    if (create && !write) {
        /* A read before the first write: an empty file is a valid database. */
        int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0644);
        if (fd >= 0) {
            close(fd);
        } else if (errno != EEXIST) {
            err_set(e, "QueryError", "WORKER", "worker error: OSError: [Errno %d] %s: '%s'", errno,
                    strerror(errno), path);
            free(path);
            return -1;
        }
    }
    if (!write && !immutable)
        recover_hot_journal(&tmp);
    char *base = path_as_uri(path);
    const char *q = write ? (create ? "?mode=rwc" : "?mode=rw") : (immutable ? "?immutable=1" : "?mode=ro");
    char *uri = xasprintf("%s%s", base ? base : "", q);
    free(base);
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(uri, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
    free(uri);
    if (rc != SQLITE_OK) {
        err_sqlite(e, db, rc);
        sqlite3_close_v2(db);
        free(path);
        return -1;
    }
    sqlite3_busy_timeout(db, tmp.busy_ms);
    if (heap_mb > 0)
        sqlite3_hard_heap_limit64(heap_mb * 1024 * 1024);
    if (limits_v && limits_v->t == V_MAP) {
        for (size_t i = 0; i < limits_v->u.a.n; i++) {
            const val *k = &limits_v->u.a.v[2 * i], *v = &limits_v->u.a.v[2 * i + 1];
            for (size_t q2 = 0; q2 < sizeof SQL_LIMITS / sizeof SQL_LIMITS[0]; q2++)
                if (val_is_str(k, SQL_LIMITS[q2].name) && v->t == V_INT)
                    sqlite3_limit(db, SQL_LIMITS[q2].id, (int)v->u.i);
        }
    }
    const val *ext = map_get(cfg, "extensions");
    if (ext && ext->t == V_ARR && ext->u.a.n > 0) {
#ifdef CWORKER_NO_LOAD_EXTENSION
        sqlite3_close_v2(db);
        free(path);
        err_set(e, "NotSupported", NULL, "this build of the C worker cannot load SQLite extensions");
        return -1;
#else
        sqlite3_enable_load_extension(db, 1);
        for (size_t i = 0; i < ext->u.a.n; i++) {
            const val *p = &ext->u.a.v[i];
            char *ep = p->t == V_STR ? xstrndup(p->u.s.p, p->u.s.n) : xstrndup("", 0);
            char *msg = NULL;
            rc = sqlite3_load_extension(db, ep, NULL, &msg);
            if (rc != SQLITE_OK) {
                err_set(e, "QueryError", "INIT", "cannot load extension %s: %s", ep, msg ? msg : "error");
                sqlite3_free(msg);
                free(ep);
                sqlite3_close_v2(db);
                free(path);
                return -1;
            }
            free(ep);
        }
        sqlite3_enable_load_extension(db, 0);
#endif
    }
    conn *c = xmalloc(sizeof *c);
    *c = tmp;
    c->db = db;
    c->write = write;
    pthread_mutex_lock(&g_mu);
    c->id = ++g_conn_seq;
    c->next = g_conns;
    g_conns = c;
    pthread_mutex_unlock(&g_mu);
    reply_ok(w, j);
    w_cstr(w, "conn");
    w_int(w, c->id);
    return 0;
}

static void close_conn(conn *wc) {
    pthread_mutex_lock(&g_mu);
    for (conn **pp = &g_conns; *pp; pp = &(*pp)->next)
        if (*pp == wc) {
            *pp = wc->next;
            break;
        }
    wc->tx = 0;
    wc->deadline_ns = 0;
    stream *drop = NULL;
    for (stream **pp = &g_streams; *pp;) {
        stream *s = *pp;
        if (s->wc == wc) {
            *pp = s->next;
            s->next = drop;
            drop = s;
        } else {
            pp = &s->next;
        }
    }
    pthread_mutex_unlock(&g_mu);
    while (drop) {
        stream *s = drop;
        drop = s->next;
        sqlite3_finalize(s->st);
        free(s);
    }
    if (conn_in_tx(wc))
        sqlite3_exec(wc->db, "ROLLBACK", NULL, NULL, NULL);
    sqlite3_close_v2(wc->db);
    free(wc->path);
    free(wc);
}

static int h_close(job *j, conn *wc, wbuf *w, err_t *e) {
    (void)e;
    pthread_mutex_lock(&g_mu);
    j->wc = NULL; /* about to be freed */
    pthread_mutex_unlock(&g_mu);
    close_conn(wc);
    reply_ok(w, j);
    return 0;
}

static int h_execute(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    limits lim = get_limits(&j->msg);
    if (lim.has_tl) {
        pthread_mutex_lock(&g_mu);
        j->backstop_ns = mono_ns() + lim.tl * 1000000LL + 5000000LL;
        pthread_cond_signal(&g_wcond);
        pthread_mutex_unlock(&g_mu);
    }
    reply_begin(w, j);
    w_cstr(w, "result");
    int rc = run_execute(j, wc, field(j, "sql"), field(j, "params"), lim, w, e, 1);
    pthread_mutex_lock(&g_mu);
    j->backstop_ns = 0;
    pthread_mutex_unlock(&g_mu);
    if (rc < 0)
        return -1;
    reply_in_tx(w, wc);
    return 0;
}

static int h_batch(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    limits lim = get_limits(&j->msg);
    lim.has_mb = 0;
    const val *stmts = field(j, "statements");
    if (stmts == NULL || stmts->t != V_ARR) {
        err_set(e, "KeyError", NULL, "'statements'");
        e->builtin = 1;
        return -1;
    }
    reply_begin(w, j);
    w_cstr(w, "result");
    w_array(w, (uint32_t)stmts->u.a.n);
    for (size_t i = 0; i < stmts->u.a.n; i++) {
        const val *pair = &stmts->u.a.v[i];
        if (pair->t != V_ARR || pair->u.a.n != 2) {
            err_set(e, "ValueError", NULL, "a batch statement is [sql, params]");
            e->builtin = 1;
            return -1;
        }
        if (run_execute(j, wc, &pair->u.a.v[0], &pair->u.a.v[1], lim, w, e, 0) < 0)
            return -1;
    }
    w_end(w);
    reply_in_tx(w, wc);
    return 0;
}

static int h_execute_many(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    const val *sqlv = field(j, "sql");
    const val *seq = field(j, "params_seq");
    const char *sql = sqlv && sqlv->t == V_STR ? sqlv->u.s.p : "";
    size_t n = sqlv && sqlv->t == V_STR ? sqlv->u.s.n : 0;
    int dml = cpython_is_dml(sql, n);
    progress_ctx pc;
    sqlite3_stmt *st = NULL;
    sqlite3_int64 rowcount = dml ? 0 : -1;
    limit_install(wc, &pc, j, 0, 0);
    if (stmt_enter(j, e) < 0) {
        limit_remove(wc);
        return -1;
    }
    if (prepare(wc, sqlv, &st, e) < 0)
        goto fail;
    if (st && sqlite3_stmt_readonly(st)) {
        err_prog(e, "executemany() can only execute DML statements.");
        goto fail;
    }
    size_t count = seq && seq->t == V_ARR ? seq->u.a.n : 0;
    for (size_t i = 0; i < count && st; i++) {
        sqlite3_reset(st);
        if (bind_params(wc, st, &seq->u.a.v[i], 0, e) < 0)
            goto fail;
        int rc = sqlite3_step(st);
        if (!step_ok(rc)) {
            err_sqlite(e, wc->db, rc);
            goto fail;
        }
        if (rc == SQLITE_DONE && dml)
            rowcount += sqlite3_changes64(wc->db);
    }
    sqlite3_finalize(st);
    limit_remove(wc);
    stmt_leave(j);
    reply_begin(w, j);
    w_cstr(w, "result");
    w_map(w, 1);
    w_cstr(w, "rowcount");
    w_int(w, rowcount);
    w_end(w);
    reply_in_tx(w, wc);
    return 0;
fail:
    sqlite3_finalize(st);
    limit_remove(wc);
    stmt_leave(j);
    return -1;
}

static int h_begin(job *j, conn *wc, wbuf *w, err_t *e) {
    const val *mode = field(j, "mode");
    const char *sql;
    pthread_mutex_lock(&g_mu);
    int open = wc->tx != 0;
    pthread_mutex_unlock(&g_mu);
    if (open) {
        err_set(e, "QueryError", "SQLITE_ERROR", "cannot start a transaction within a transaction");
        return -1;
    }
    if (mode == NULL || mode->t == V_NIL || val_is_str(mode, "write")) {
        sql = "BEGIN IMMEDIATE";
    } else if (val_is_str(mode, "read")) {
        sql = "BEGIN";
    } else {
        char t[80];
        tx_text(mode, t, sizeof t);
        err_set(e, "ValueError", NULL, "unknown transaction mode '%s'", t);
        e->builtin = 1;
        return -1;
    }
    if (exec_simple(j, wc, sql, e) < 0)
        return -1;
    int present;
    int64_t deadline_ms = get_int(&j->msg, "deadline_ms", 0, &present);
    pthread_mutex_lock(&g_mu);
    int tx = wc->tx = ++wc->tx_seq;
    if (present) {
        wc->deadline_ns = mono_ns() + deadline_ms * 1000000LL;
        if (wc->deadline_ns == 0)
            wc->deadline_ns = 1;
        pthread_cond_signal(&g_wcond);
    }
    pthread_mutex_unlock(&g_mu);
    reply_ok(w, j);
    char t[32];
    snprintf(t, sizeof t, "t%d", tx);
    w_cstr(w, "tx");
    w_cstr(w, t);
    return 0;
}

static void clear_tx(conn *wc) {
    pthread_mutex_lock(&g_mu);
    wc->tx = 0;
    wc->deadline_ns = 0;
    pthread_mutex_unlock(&g_mu);
}

static int h_commit(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    pthread_mutex_lock(&g_mu);
    wc->deadline_ns = 0; /* a commit that has started is not cut short */
    pthread_mutex_unlock(&g_mu);
    if (conn_in_tx(wc) && exec_simple(j, wc, "COMMIT", e) < 0)
        return -1;
    clear_tx(wc);
    reply_ok(w, j);
    return 0;
}

static int h_rollback(job *j, conn *wc, wbuf *w, err_t *e) {
    const val *v = field(j, "tx");
    int n = tx_number(v);
    pthread_mutex_lock(&g_mu);
    int was_expired = 0;
    if (n > 0 && wc->tx != n) {
        for (int i = 0; i < wc->nexpired; i++)
            if (wc->expired[i] == n) {
                memmove(&wc->expired[i], &wc->expired[i + 1], (size_t)(wc->nexpired - i - 1) * sizeof(int));
                wc->nexpired--;
                was_expired = 1;
                break;
            }
    }
    pthread_mutex_unlock(&g_mu);
    if (was_expired) {
        reply_ok(w, j);
        w_cstr(w, "expired");
        w_bool(w, 1);
        return 0;
    }
    if (check_tx(j, wc, e) < 0)
        return -1;
    if (conn_in_tx(wc) && exec_simple(j, wc, "ROLLBACK", e) < 0)
        return -1;
    clear_tx(wc);
    reply_ok(w, j);
    return 0;
}

static int h_op(job *j, conn *wc, wbuf *w, err_t *e) {
    (void)w;
    if (check_tx(j, wc, e) < 0)
        return -1;
    const val *name = field(j, "name");
    err_set(e, "NotSupported", "NOT_IMPLEMENTED",
            "named operation %.*s is not implemented by the C worker (the parent runs it)",
            name && name->t == V_STR ? (int)name->u.s.n : 4, name && name->t == V_STR ? name->u.s.p : "None");
    return -1;
}

static int h_schema_version(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    sqlite3_stmt *st = NULL;
    int rc = sqlite3_prepare_v2(wc->db, "pragma schema_version", -1, &st, NULL);
    if (rc == SQLITE_OK)
        rc = sqlite3_step(st);
    if (rc != SQLITE_ROW) {
        err_sqlite(e, wc->db, rc);
        sqlite3_finalize(st);
        return -1;
    }
    int64_t v = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    reply_begin(w, j);
    w_cstr(w, "result");
    w_int(w, v);
    reply_in_tx(w, wc);
    return 0;
}

/* schema.c: the whole schema as schema_to_wire() shapes it; returns a
 * SQLite result code (SQLITE_OK on success). */
int schema_write(sqlite3 *db, wbuf *w);

static int h_schema(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    int own_tx = sqlite3_get_autocommit(wc->db);
    if (own_tx && exec_simple(j, wc, "BEGIN", e) < 0)
        return -1;
    reply_begin(w, j);
    w_cstr(w, "result");
    int rc = schema_write(wc->db, w);
    if (rc != SQLITE_OK)
        err_sqlite(e, wc->db, rc);
    if (own_tx && conn_in_tx(wc))
        sqlite3_exec(wc->db, "ROLLBACK", NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return -1;
    reply_in_tx(w, wc);
    return 0;
}

/* -- streams -------------------------------------------------------------------- */

/* _SqliteStream._next_chunk: 1 rows written, 0 end, -1 error. */
static int next_chunk(job *j, stream *s, wbuf *w, err_t *e) {
    conn *wc = s->wc;
    int64_t start = mono_ns();
    int64_t ms = s->has_limit ? (s->remaining_ms > 0 ? (int64_t)s->remaining_ms : 0) : 0;
    progress_ctx pc;
    int result = -1;
    int64_t count = 0;
    limit_install(wc, &pc, j, s->has_limit, ms);
    if (stmt_enter(j, e) < 0)
        goto out_noleave;
    if (s->has_limit && s->remaining_ms <= 0) {
        err_interrupted(e);
        goto out;
    }
    if (!s->started) {
        s->started = 1;
        if (prepare(wc, field(j, "sql"), &s->st, e) < 0 || bind_params(wc, s->st, field(j, "params"), 1, e) < 0)
            goto out;
        s->ncols = s->st ? sqlite3_column_count(s->st) : 0;
        if (s->st) {
            int rc = sqlite3_step(s->st);
            if (!step_ok(rc)) {
                err_sqlite(e, wc->db, rc);
                goto out;
            }
            s->has_row = rc == SQLITE_ROW;
        }
        if (s->ncols == 0) {
            result = 0;
            goto out;
        }
    }
    int64_t size = 0;
    while (count < s->chunk_rows && s->has_row) {
        if (count == 0) {
            w_cstr(w, "rows");
            w_array_open(w);
        }
        if (emit_row(s->st, s->ncols, w, 1, &size, e) < 0)
            goto out;
        count++;
        int rc = sqlite3_step(s->st);
        if (!step_ok(rc)) {
            err_sqlite(e, wc->db, rc);
            goto out;
        }
        s->has_row = rc == SQLITE_ROW;
        if (w->failed)
            goto out;
    }
    if (count > 0)
        w_end(w);
    if (count > 0 && s->has_max) {
        s->bytes += size;
        if (s->bytes > s->max_bytes) {
            err_set(e, "ResourceLimitExceeded", NULL, "result exceeded max_result_bytes=%lld",
                    (long long)s->max_bytes);
            goto out;
        }
    }
    result = count > 0 ? 1 : 0;
out:
    stmt_leave(j);
out_noleave:
    limit_remove(wc);
    if (s->has_limit)
        s->remaining_ms -= (double)(mono_ns() - start) / 1e6;
    return result;
}

static void write_stream_columns(wbuf *w, stream *s) {
    w_cstr(w, "columns");
    w_array(w, (uint32_t)s->ncols);
    for (int i = 0; i < s->ncols; i++) {
        const char *name = sqlite3_column_name(s->st, i);
        w_str_lossy(w, name ? name : "", name ? strlen(name) : 0);
    }
    w_end(w);
}

/* Server._pump: send up to `credit` frames, then leave the cursor open. */
static int pump(job *j, stream *s, wbuf *w, err_t *e) {
    while (s->credit > 0) {
        if (atomic_load(&j->cancelled)) {
            err_set(e, "QueryInterrupted", NULL, "query cancelled");
            return -1;
        }
        w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
        w_map_open(w);
        w_cstr(w, "id");
        w_int(w, s->id);
        int r = next_chunk(j, s, w, e);
        if (r < 0)
            return -1;
        if (s->first) {
            write_stream_columns(w, s);
            s->first = 0;
        }
        if (r == 0) {
            stream_drop(s);
            w_cstr(w, "end");
            w_bool(w, 1);
            w_end(w);
            if (w->failed)
                return -1;
            send_frame(w);
            return 1;
        }
        s->credit--;
        w_end(w);
        if (w->failed)
            return -1;
        send_frame(w);
    }
    return 1;
}

static int h_stream(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    int present;
    int64_t chunk = get_int(&j->msg, "chunk_rows", 1000, NULL);
    if (chunk < 1) {
        err_set(e, "ValueError", NULL, "chunk_rows must be at least 1");
        e->builtin = 1;
        return -1;
    }
    stream *s = xmalloc(sizeof *s);
    memset(s, 0, sizeof *s);
    s->id = j->id;
    s->wc = wc;
    s->first = 1;
    s->chunk_rows = chunk;
    s->credit = get_int(&j->msg, "credit", 1, NULL);
    int64_t tl = get_int(&j->msg, "time_limit_ms", 0, &present);
    s->has_limit = present;
    s->remaining_ms = (double)tl;
    s->max_bytes = get_int(&j->msg, "max_result_bytes", 0, &s->has_max);
    pthread_mutex_lock(&g_mu);
    stream *old = stream_find(s->id);
    s->next = g_streams;
    g_streams = s;
    pthread_mutex_unlock(&g_mu);
    if (old)
        stream_drop(old);
    return pump(j, s, w, e);
}

static int h_more(job *j, wbuf *w, err_t *e) {
    pthread_mutex_lock(&g_mu);
    stream *s = stream_find(j->id);
    if (s)
        j->wc = s->wc;
    pthread_mutex_unlock(&g_mu);
    if (s == NULL)
        return 1; /* finished or closed meanwhile */
    s->credit += j->credit;
    return pump(j, s, w, e);
}

static int h_close_stream(job *j, wbuf *w, err_t *e) {
    (void)e;
    pthread_mutex_lock(&g_mu);
    stream *s = stream_find(j->id);
    if (s)
        j->wc = s->wc;
    pthread_mutex_unlock(&g_mu);
    if (s == NULL)
        return 1; /* its last frame was already sent */
    stream_drop(s);
    reply_begin(w, j);
    w_cstr(w, "end");
    w_bool(w, 1);
    w_cstr(w, "cancelled");
    w_bool(w, 1);
    return 0;
}

static void close_all(void) {
    for (;;) {
        pthread_mutex_lock(&g_mu);
        conn *c = g_conns;
        pthread_mutex_unlock(&g_mu);
        if (c == NULL)
            break;
        close_conn(c);
    }
    pthread_mutex_lock(&g_mu);
    stream *s = g_streams;
    g_streams = NULL;
    pthread_mutex_unlock(&g_mu);
    while (s) {
        stream *next = s->next;
        sqlite3_finalize(s->st);
        free(s);
        s = next;
    }
}

static int h_sleep(job *j, wbuf *w, err_t *e) {
    (void)e;
    close_all();
    reply_ok(w, j);
    g_exit_requested = 1;
    return 0;
}

static int h_debug(job *j, wbuf *w, err_t *e) {
    const val *action = field(j, "action");
    if (val_is_str(action, "sleep")) {
        const val *sv = field(j, "seconds");
        double secs = sv && sv->t == V_FLOAT ? sv->u.d : sv && sv->t == V_INT ? (double)sv->u.i : 1.0;
        struct timespec ts = {(time_t)secs, (long)((secs - (double)(time_t)secs) * 1e9)};
        while (nanosleep(&ts, &ts) < 0 && errno == EINTR) {
        }
        reply_ok(w, j);
        return 0;
    }
    if (val_is_str(action, "oversized_frame")) {
        static const uint8_t hdr[4] = {0x80, 0, 0, 0};
        write_all(hdr, 4);
        write_all((const uint8_t *)"xxxxxxxxxxxxxxxx", 16);
        return 1;
    }
    if (val_is_str(action, "truncated_frame")) {
        static const uint8_t hdr[4] = {0, 0, 0, 100};
        write_all(hdr, 4);
        write_all((const uint8_t *)"{\"id\":", 6);
        _exit(3);
    }
    if (val_is_str(action, "garbage")) {
        static const char junk[] = "\xff\xfe not a valid payload";
        uint8_t hdr[4] = {0, 0, 0, (uint8_t)(sizeof junk - 1)};
        write_all(hdr, 4);
        write_all((const uint8_t *)junk, sizeof junk - 1);
        return 1;
    }
    if (val_is_str(action, "alloc")) {
        int64_t mb = get_int(&j->msg, "mb", 0, NULL);
        char **blocks = calloc((size_t)(mb > 0 ? mb : 1), sizeof(char *));
        int64_t got = 0;
        for (; blocks && got < mb; got++) {
            blocks[got] = malloc(1024 * 1024);
            if (blocks[got] == NULL)
                break;
            memset(blocks[got], 1, 1024 * 1024);
        }
        for (int64_t i = 0; blocks && i < got; i++)
            free(blocks[i]);
        free(blocks);
        if (got < mb) {
            err_set(e, "ResourceLimitExceeded", NULL, "worker ran out of memory (memory limit)");
            e->fatal = 1;
            return -1;
        }
        reply_ok(w, j);
        w_cstr(w, "allocated_mb");
        w_int(w, got);
        return 0;
    }
    if (val_is_str(action, "exit"))
        _exit((int)get_int(&j->msg, "code", 1, NULL));
    if (val_is_str(action, "print")) {
        const val *t = field(j, "text");
        fprintf(stderr, "%.*s\n", t && t->t == V_STR ? (int)t->u.s.n : 0, t && t->t == V_STR ? t->u.s.p : "");
        fflush(stderr);
        reply_ok(w, j);
        return 0;
    }
    char t[80];
    tx_text(action, t, sizeof t);
    err_set(e, "QueryError", "PROTOCOL", "unknown debug action '%s'", t);
    return -1;
}

/* Server._expire: the deadline passed; roll back and tell the parent. */
static void run_expire(job *j) {
    pthread_mutex_lock(&g_mu);
    conn *wc = NULL;
    for (conn *c = g_conns; c; c = c->next)
        if (c->id == j->exp_conn)
            wc = c;
    int ok = wc && wc->tx == j->exp_tx;
    if (ok)
        wc->tx = 0;
    pthread_mutex_unlock(&g_mu);
    if (!ok)
        return;
    if (conn_in_tx(wc)) {
        err_t e = {0};
        if (exec_simple(j, wc, "ROLLBACK", &e) < 0)
            fprintf(stderr, "dblayer.worker: rollback at deadline failed: %s\n", e.msg);
        err_clear(&e);
    }
    wbuf *w = &g_w;
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map(w, 3);
    w_cstr(w, "type");
    w_cstr(w, "expired");
    w_cstr(w, "conn");
    w_int(w, wc->id);
    char t[32];
    snprintf(t, sizeof t, "t%d", j->exp_tx);
    w_cstr(w, "tx");
    w_cstr(w, t);
    w_end(w);
    send_frame(w);
}

/* Server.translate */
static void translate(job *j, err_t *e) {
    if (!atomic_load(&j->cancelled) || e->type == NULL || strcmp(e->type, "QueryInterrupted") != 0)
        return;
    pthread_mutex_lock(&g_mu);
    int reason = j->reason;
    pthread_mutex_unlock(&g_mu);
    if (reason == R_DEADLINE)
        err_set(e, "TransactionExpired", NULL, "transaction passed its deadline and was rolled back by the worker");
    else if (reason == R_TIME_LIMIT)
        err_set(e, "QueryInterrupted", NULL, "query interrupted (time limit exceeded)");
    else
        err_set(e, "QueryInterrupted", NULL, "query cancelled");
    free(e->cause_msg);
    e->cause_msg = NULL;
    e->cause_type = NULL;
}

static void send_error(job *j, err_t *e, conn *wc) {
    wbuf *w = &g_w;
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map_open(w);
    w_cstr(w, "id");
    if (j->has_id)
        w_int(w, j->id);
    else
        w_nil(w);
    w_cstr(w, "error");
    w_map_open(w);
    w_cstr(w, "type");
    w_cstr(w, e->type);
    w_cstr(w, "message");
    w_str_lossy(w, e->msg ? e->msg : "", e->msg ? strlen(e->msg) : 0);
    if (e->builtin) {
        w_cstr(w, "builtin");
        w_bool(w, 1);
    } else {
        if (e->code) {
            w_cstr(w, "code");
            w_cstr(w, e->code);
        }
        if (e->cause_type) {
            w_cstr(w, "cause");
            w_map(w, 2);
            w_cstr(w, "type");
            w_cstr(w, e->cause_type);
            w_cstr(w, "message");
            w_str_lossy(w, e->cause_msg ? e->cause_msg : "", e->cause_msg ? strlen(e->cause_msg) : 0);
            w_end(w);
        }
    }
    w_end(w);
    if (wc && conn_in_tx(wc)) {
        w_cstr(w, "in_tx");
        w_bool(w, 1);
    }
    if (e->fatal) {
        w_cstr(w, "fatal");
        w_bool(w, 1);
    }
    w_end(w);
    send_frame(w);
}

static void wbuf_error(wbuf *w, err_t *e) {
    if (w->failed == WB_TOO_LARGE)
        err_set(e, "ResourceLimitExceeded", NULL,
                "reply too large: frame of more than %zu bytes exceeds the %zu-byte frame size cap", w->limit,
                w->limit);
    else
        err_set(e, "ResourceLimitExceeded", NULL, "worker ran out of memory (memory limit)");
}

static void run_job(job *j) {
    wbuf *w = &g_w;
    err_t e = {0};
    int rc = -1;
    int streaming = 0;
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    if (j->kind == J_EXPIRE) {
        run_expire(j);
        return;
    }
    if (atomic_load(&j->cancelled)) {
        err_set(&e, "QueryInterrupted", NULL, "query cancelled");
        goto done;
    }
    if (j->kind == J_MORE) {
        streaming = 1;
        rc = h_more(j, w, &e);
        goto done;
    }
    if (j->kind == J_CLOSE_STREAM) {
        rc = h_close_stream(j, w, &e);
        goto done;
    }
    const val *type = field(j, "type");
    if (val_is_str(type, "open")) { rc = h_open(j, w, &e); goto done; }
    if (val_is_str(type, "sleep")) { rc = h_sleep(j, w, &e); goto done; }
    if (val_is_str(type, "_debug") && g_debug) { rc = h_debug(j, w, &e); goto done; }
    static const char *const CONN_TYPES[] = {"close", "execute", "execute_many", "batch", "stream", "begin",
                                             "commit", "rollback", "op", "schema", "schema_version"};
    int known = 0;
    for (size_t i = 0; i < sizeof CONN_TYPES / sizeof CONN_TYPES[0]; i++)
        known |= val_is_str(type, CONN_TYPES[i]);
    if (!known) {
        char t[80];
        tx_text(type, t, sizeof t);
        if (type && type->t == V_STR)
            err_set(&e, "QueryError", "PROTOCOL", "unknown message type '%s'", t);
        else
            err_set(&e, "QueryError", "PROTOCOL", "unknown message type %s", t);
        goto done;
    }
    conn *wc = conn_for(j, &e);
    if (wc == NULL)
        goto done;
    if (val_is_str(type, "close")) rc = h_close(j, wc, w, &e);
    else if (val_is_str(type, "execute")) rc = h_execute(j, wc, w, &e);
    else if (val_is_str(type, "execute_many")) rc = h_execute_many(j, wc, w, &e);
    else if (val_is_str(type, "batch")) rc = h_batch(j, wc, w, &e);
    else if (val_is_str(type, "stream")) { streaming = 1; rc = h_stream(j, wc, w, &e); }
    else if (val_is_str(type, "begin")) rc = h_begin(j, wc, w, &e);
    else if (val_is_str(type, "commit")) rc = h_commit(j, wc, w, &e);
    else if (val_is_str(type, "rollback")) rc = h_rollback(j, wc, w, &e);
    else if (val_is_str(type, "op")) rc = h_op(j, wc, w, &e);
    else if (val_is_str(type, "schema")) rc = h_schema(j, wc, w, &e);
    else rc = h_schema_version(j, wc, w, &e);
done:
    if (rc == 0 && !streaming && atomic_load(&j->cancelled)) {
        /* cancelled while it ran: the caller gets the cancellation */
        err_set(&e, "QueryInterrupted", NULL, "query cancelled");
        rc = -1;
    }
    if (rc == 0) {
        w_end(w);
        if (w->failed) {
            wbuf_error(w, &e);
            rc = -1;
        } else {
            send_frame(w);
        }
    }
    if (rc < 0) {
        if (e.type == NULL)
            wbuf_error(w, &e);
        translate(j, &e);
        pthread_mutex_lock(&g_mu);
        conn *wc2 = j->wc;
        pthread_mutex_unlock(&g_mu);
        send_error(j, &e, wc2);
        if (streaming || (j->kind == J_REQ && val_is_str(field(j, "type"), "stream")))
            stream_drop_id(j->id);
        if (e.fatal)
            g_exit_requested = 1;
    }
    err_clear(&e);
    if (w->cap > (1u << 20))
        w_free(w); /* give back the memory of a big reply */
}

/* -- watchdog ----------------------------------------------------------------- */

/* [g_mu] Server._on_deadline */
static void on_deadline(conn *c) {
    int tx = c->tx;
    c->deadline_ns = 0;
    if (c->nexpired == MAX_EXPIRED) {
        memmove(c->expired, c->expired + 1, (MAX_EXPIRED - 1) * sizeof(int));
        c->nexpired--;
    }
    c->expired[c->nexpired++] = tx;
    if (g_current && g_current->wc == c)
        cancel_job(g_current, R_DEADLINE);
    job *x = job_new(J_EXPIRE);
    x->exp_conn = c->id;
    x->exp_tx = tx;
    enqueue(x, 1);
}

static void *watchdog_main(void *arg) {
    (void)arg;
    pthread_mutex_lock(&g_mu);
    for (;;) {
        int64_t now = mono_ns();
        int64_t next = now + 3600LL * 1000000000LL;
        for (conn *c = g_conns; c; c = c->next) {
            if (c->tx && c->deadline_ns) {
                if (now >= c->deadline_ns)
                    on_deadline(c);
                else if (c->deadline_ns < next)
                    next = c->deadline_ns;
            }
        }
        job *j = g_current;
        if (j && j->backstop_ns) {
            if (now >= j->backstop_ns) {
                j->backstop_ns = 0;
                cancel_job(j, R_TIME_LIMIT);
            } else if (j->backstop_ns < next) {
                next = j->backstop_ns;
            }
        }
        if (j && atomic_load(&j->cancelled) && j->active && j->wc && j->wc->db) {
            if (now >= j->next_interrupt_ns) {
                sqlite3_interrupt(j->wc->db);
                j->next_interrupt_ns = now + REINTERRUPT_NS;
            }
            if (j->next_interrupt_ns < next)
                next = j->next_interrupt_ns;
        }
        cond_wait_until(&g_wcond, next);
    }
    return NULL;
}

/* -- reader -------------------------------------------------------------------- */

static int read_full(int fd, uint8_t *p, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t k = read(fd, p + got, n - got);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (k == 0)
            return got == 0 ? 0 : -1;
        got += (size_t)k;
    }
    return 1;
}

static void hello(const val *msg) {
    const val *codecs = map_get(msg, "codecs");
    int codec = CODEC_JSON;
    if (codecs && codecs->t == V_ARR) {
        for (size_t i = 0; i < codecs->u.a.n; i++) {
            if (val_is_str(&codecs->u.a.v[i], "msgpack")) { codec = CODEC_MSGPACK; break; }
            if (val_is_str(&codecs->u.a.v[i], "json")) { codec = CODEC_JSON; break; }
        }
    }
    int present;
    int64_t mfb = get_int(msg, "max_frame_bytes", 0, &present);
    if (present && mfb > 0)
        atomic_store(&g_max_frame, (size_t)mfb);
    wbuf w;
    w_init(&w, CODEC_JSON, atomic_load(&g_max_frame));
    w_map_open(&w);
    w_cstr(&w, "id");
    const val *id = map_get(msg, "id");
    if (id) w_val(&w, id); else w_nil(&w);
    w_cstr(&w, "type"); w_cstr(&w, "hello");
    w_cstr(&w, "protocol"); w_int(&w, PROTOCOL_VERSION);
    w_cstr(&w, "codec"); w_cstr(&w, codec == CODEC_MSGPACK ? "msgpack" : "json");
    w_cstr(&w, "sqlite_version"); w_cstr(&w, sqlite3_libversion());
    w_cstr(&w, "implementation"); w_cstr(&w, "c");
    w_cstr(&w, "pid"); w_int(&w, (int64_t)getpid());
    w_cstr(&w, "rlimit_as"); w_bool(&w, g_rlimit_as);
    w_cstr(&w, "ops"); w_bool(&w, 0);
    w_end(&w);
    send_frame(&w);
    w_free(&w);
    atomic_store(&g_codec, codec);
}

static void on_eof(void) {
    pthread_mutex_lock(&g_mu);
    if (!g_eof) {
        g_eof = 1;
        for (job *j = q_head; j; j = j->next)
            atomic_store(&j->cancelled, 1);
        if (g_current)
            cancel_job(g_current, R_CANCELLED);
        enqueue(job_new(J_SHUTDOWN), 0);
    }
    pthread_mutex_unlock(&g_mu);
}

static void dispatch(job *j) {
    const val *type = map_get(&j->msg, "type");
    const val *id = map_get(&j->msg, "id");
    if (id && id->t == V_INT) {
        j->has_id = 1;
        j->id = id->u.i;
    }
    if (val_is_str(type, "hello")) {
        hello(&j->msg);
        job_free(j);
        return;
    }
    if (val_is_str(type, "cancel")) {
        const val *t = map_get(&j->msg, "target");
        int64_t target = t && t->t == V_INT ? t->u.i : INT64_MIN;
        pthread_mutex_lock(&g_mu);
        job *pending = NULL;
        for (job *p = q_head; p; p = p->next)
            if (p->has_id && p->id == target && p->kind != J_EXPIRE)
                pending = p;
        if (g_current && g_current->has_id && g_current->id == target && g_current->kind != J_EXPIRE) {
            cancel_job(g_current, R_CANCELLED);
        } else if (pending) {
            atomic_store(&pending->cancelled, 1);
        } else if (stream_find(target)) {
            job *c = job_new(J_CLOSE_STREAM);
            c->has_id = 1;
            c->id = target;
            enqueue(c, 0);
        }
        pthread_mutex_unlock(&g_mu);
        job_free(j);
        return;
    }
    if (val_is_str(type, "more")) {
        job *m = job_new(J_MORE);
        m->has_id = 1;
        m->id = get_int(&j->msg, "stream", 0, NULL);
        m->credit = get_int(&j->msg, "credit", 1, NULL);
        job_free(j);
        pthread_mutex_lock(&g_mu);
        enqueue(m, 0);
        pthread_mutex_unlock(&g_mu);
        return;
    }
    pthread_mutex_lock(&g_mu);
    enqueue(j, 0);
    pthread_mutex_unlock(&g_mu);
}

static void *reader_main(void *arg) {
    (void)arg;
    pthread_mutex_lock(&g_mu);
    while (!g_started)
        pthread_cond_wait(&g_startcond, &g_mu);
    pthread_mutex_unlock(&g_mu);
    for (;;) {
        uint8_t hdr[4];
        int r = read_full(0, hdr, 4);
        if (r <= 0)
            break;
        size_t n = (size_t)hdr[0] << 24 | (size_t)hdr[1] << 16 | (size_t)hdr[2] << 8 | hdr[3];
        size_t cap = atomic_load(&g_max_frame);
        if (n > cap) {
            fprintf(stderr, "dblayer.worker: frame of %zu bytes exceeds the %zu-byte frame size cap; "
                            "dropping the connection\n", n, cap);
            break;
        }
        job *j = job_new(J_REQ);
        j->payload = malloc(n ? n : 1);
        if (j->payload == NULL || read_full(0, j->payload, n) != 1) {
            if (j->payload == NULL)
                fprintf(stderr, "dblayer.worker: out of memory reading a frame; dropping the connection\n");
            job_free(j);
            break;
        }
        char err[200];
        if (decode_msg(atomic_load(&g_codec), j->payload, n, &j->ar, &j->msg, err, sizeof err) < 0) {
            fprintf(stderr, "dblayer.worker: bad input (%s); dropping the connection\n", err);
            job_free(j);
            break;
        }
        dispatch(j);
    }
    on_eof();
    /* Exit even if a statement ignores every interrupt. */
    sleep(SHUTDOWN_GRACE_S);
    fflush(stderr);
    _exit(0);
    return NULL;
}

/* -- main ---------------------------------------------------------------------- */

static int set_rlimit_as(long mb) {
#ifdef __linux__
    FILE *f = fopen("/proc/self/status", "r");
    if (f == NULL)
        return 0;
    char line[256];
    long vm_kb = -1;
    while (fgets(line, sizeof line, f))
        if (strncmp(line, "VmSize:", 7) == 0)
            vm_kb = strtol(line + 7, NULL, 10);
    fclose(f);
    if (vm_kb < 0)
        return 0;
    struct rlimit rl;
    if (getrlimit(RLIMIT_AS, &rl) != 0)
        return 0;
    rlim_t limit = (rlim_t)vm_kb * 1024 + (rlim_t)mb * 1024 * 1024;
    if (rl.rlim_max != RLIM_INFINITY && limit > rl.rlim_max)
        limit = rl.rlim_max;
    rl.rlim_cur = limit;
    return setrlimit(RLIMIT_AS, &rl) == 0;
#else
    (void)mb;
    return 0;
#endif
}

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--debug") == 0) {
            g_debug = 1;
        } else if (strcmp(argv[i], "--memory-limit-mb") == 0 && i + 1 < argc) {
            g_memory_limit_mb = strtol(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--version") == 0) {
            printf("dblayer-cworker protocol %d, SQLite %s\n", PROTOCOL_VERSION, sqlite3_libversion());
            return 0;
        } else {
            fprintf(stderr, "usage: dblayer-cworker [--memory-limit-mb N] [--debug]\n");
            return 2;
        }
    }
    /* The parent's Ctrl-C is not ours: we exit when stdin closes. */
    signal(SIGINT, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    /* Anything printed (an extension, say) goes to stderr, never into
     * the protocol stream. */
    g_out_fd = dup(1);
    dup2(2, 1);
    /* Heap limits need memory statistics (off in some system builds). */
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 1);
    sqlite3_initialize();
    cond_init(&g_qcond);
    cond_init(&g_wcond);
    pthread_t reader, watchdog;
    if (pthread_create(&watchdog, NULL, watchdog_main, NULL) != 0 ||
        pthread_create(&reader, NULL, reader_main, NULL) != 0) {
        fprintf(stderr, "dblayer-cworker: cannot start threads\n");
        return 1;
    }
    /* After the threads exist, so their stacks are already mapped. */
    if (g_memory_limit_mb > 0)
        g_rlimit_as = set_rlimit_as(g_memory_limit_mb);
    pthread_mutex_lock(&g_mu);
    g_started = 1;
    pthread_cond_broadcast(&g_startcond);
    pthread_mutex_unlock(&g_mu);
    for (;;) {
        job *j = dequeue();
        if (j->kind == J_SHUTDOWN) {
            job_free(j);
            break;
        }
        run_job(j);
        pthread_mutex_lock(&g_mu);
        g_current = NULL;
        pthread_mutex_unlock(&g_mu);
        job_free(j);
        if (g_exit_requested || atomic_load(&g_parent_gone))
            break;
    }
    close_all();
    fflush(stderr);
    _exit(0);
}
