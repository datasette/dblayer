/* The `export` request: a result serialised to CSV, TSV or JSON in the
 * worker, straight from sqlite3_column_*, as docs/export-formats.md
 * defines the row bytes. Included by worker.c (it uses that file's
 * statics: prepare, bind_params, the time-limit and stream machinery), so
 * the hooks there stay a few lines.
 *
 * An export is a stream (struct stream) with `ex` set, so credit, `more`,
 * `cancel` of a paused export and closing a connection all reuse the
 * stream code. Three transports for the bytes:
 *
 *   "frames": codec frames {"id", "data": <bin>, "rows": n} (MessagePack
 *             bin; base64 under JSON), credit-based like `stream`;
 *   "raw":    frames whose payload is 0x00, the request id (8 bytes, big
 *             endian), then the bytes: no codec at all; same credit rule;
 *   "fifo":   the bytes go to a named pipe the parent created and holds open
 *             for reading (`data_path`); the pipe is the backpressure.
 *             The data starts with a 4-byte length and a JSON array of the
 *             column names. The request's only reply comes at the end.
 *   "socket": socket handoff. After the columns frame the worker connects to
 *             the Unix socket `handoff_path` and receives a file descriptor
 *             (SCM_RIGHTS): the HTTP client's own connection, with the
 *             status line, headers and the first chunk already sent by the
 *             parent. The worker writes the rows to it as HTTP/1.1 chunks
 *             (never the terminating 0-chunk: the parent writes the tail),
 *             closes its copy, then sends {"id", "end", ...}.
 *
 * For "frames" and "raw" the first frame is {"id", "columns": [...]}; the
 * last is {"id", "end": true, "row_count": n, "truncated": bool}. */

#include <float.h>
#include <math.h>
#ifndef CWORKER_WASM
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

enum { ENC_CSV, ENC_TSV, ENC_JSON_OBJECTS, ENC_JSON_ARRAYS, ENC_NDJSON };
enum { T_FRAMES, T_RAW, T_FIFO, T_SOCKET };

#define SOCK_HEAD 10 /* room for an HTTP chunk-size line: 8 hex digits + CRLF */

#define RAW_HEAD 13 /* frame length (4) + 0x00 + request id (8) */

typedef struct {
    uint8_t *p;
    size_t len, cap;
    int failed;
} xbuf;

static void xb_grow(xbuf *b, size_t need) {
    if (b->len + need <= b->cap)
        return;
    size_t cap = b->cap ? b->cap : 65536;
    while (cap < b->len + need)
        cap *= 2;
    uint8_t *q = realloc(b->p, cap);
    if (q == NULL) {
        b->failed = 1;
        return;
    }
    b->p = q;
    b->cap = cap;
}

static inline void xb_put(xbuf *b, const void *p, size_t n) {
    if (b->len + n > b->cap) {
        xb_grow(b, n);
        if (b->failed)
            return;
    }
    memcpy(b->p + b->len, p, n);
    b->len += n;
}

static inline void xb_putc(xbuf *b, char c) {
    if (b->len + 1 > b->cap) {
        xb_grow(b, 1);
        if (b->failed)
            return;
    }
    b->p[b->len++] = (uint8_t)c;
}

typedef struct export_state {
    int enc, transport;
    int64_t chunk_bytes, max_rows, rows;
    int has_max_rows, truncated, first_row, sent_columns;
    int fd;              /* fifo */
    xbuf out;            /* the bytes of the chunk being built */
    char **keys;         /* JSON objects: `"name":` per column kept, NULL if a repeat */
    size_t *key_len;
} export_state;

static void stream_drop_unlisted(stream *s);

static void export_free(void *p) {
    export_state *x = p;
    if (x == NULL)
        return;
    if (x->keys) {
        for (int i = 0; x->keys[i] != (char *)-1; i++)
            free(x->keys[i]);
        free(x->keys);
    }
    free(x->key_len);
    free(x->out.p);
#ifndef CWORKER_WASM
    if (x->fd >= 0)
        close(x->fd);
#endif
    free(x);
}

/* -- values ---------------------------------------------------------------- */

/* Python's float.__repr__: the shortest digits that read back as `d`
 * (15, 16 or 17 significant digits; see codec.c's w_double), laid out as
 * repr() does: positional for exponents -4..15 with at least one digit
 * after the point, else d.ddde+XX. `d` must be finite. */
static size_t py_float_repr(double d, char *out) {
    char b[40];
    /* 15 significant digits always suffice to find the shortest form of a
     * normal double (trailing zeros are stripped below); subnormals have
     * fewer significant bits, so search from 1 digit up for those. */
    int lo = fabs(d) < DBL_MIN && d != 0 ? 1 : 15;
    for (int prec = lo; prec <= 17; prec++) {
        snprintf(b, sizeof b, "%.*e", prec - 1, d);
        if (prec == 17 || strtod(b, NULL) == d)
            break;
    }
    const char *p = b;
    char *o = out;
    if (*p == '-') {
        *o++ = '-';
        p++;
    }
    char dig[24];
    int nd = 0;
    dig[nd++] = *p++;
    if (*p == '.') {
        p++;
        while (*p && *p != 'e' && nd < 20)
            dig[nd++] = *p++;
    }
    while (*p && *p != 'e')
        p++;
    int exp = *p == 'e' ? atoi(p + 1) : 0;
    while (nd > 1 && dig[nd - 1] == '0')
        nd--;
    if (exp >= -4 && exp < 16) {
        if (exp >= 0) {
            for (int i = 0; i <= exp; i++)
                *o++ = i < nd ? dig[i] : '0';
            *o++ = '.';
            if (nd > exp + 1)
                for (int i = exp + 1; i < nd; i++)
                    *o++ = dig[i];
            else
                *o++ = '0';
        } else {
            *o++ = '0';
            *o++ = '.';
            for (int i = 0; i < -exp - 1; i++)
                *o++ = '0';
            for (int i = 0; i < nd; i++)
                *o++ = dig[i];
        }
    } else {
        *o++ = dig[0];
        if (nd > 1) {
            *o++ = '.';
            for (int i = 1; i < nd; i++)
                *o++ = dig[i];
        }
        *o++ = 'e';
        *o++ = exp < 0 ? '-' : '+';
        int a = exp < 0 ? -exp : exp;
        if (a < 10)
            *o++ = '0';
        o += snprintf(o, 8, "%d", a);
    }
    return (size_t)(o - out);
}

static size_t i64_text(int64_t v, char *out) {
    char t[24];
    int n = 0;
    uint64_t u = v < 0 ? (uint64_t)0 - (uint64_t)v : (uint64_t)v;
    do {
        t[n++] = (char)('0' + u % 10);
        u /= 10;
    } while (u);
    size_t k = 0;
    if (v < 0)
        out[k++] = '-';
    while (n)
        out[k++] = t[--n];
    return k;
}

static const char XB64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void xb_base64(xbuf *b, const uint8_t *s, size_t n) {
    xb_grow(b, (n + 2) / 3 * 4);
    if (b->failed)
        return;
    uint8_t *q = b->p + b->len;
    size_t i = 0;
    for (; i + 3 <= n; i += 3) {
        uint32_t v = (uint32_t)s[i] << 16 | (uint32_t)s[i + 1] << 8 | s[i + 2];
        *q++ = (uint8_t)XB64[v >> 18];
        *q++ = (uint8_t)XB64[(v >> 12) & 63];
        *q++ = (uint8_t)XB64[(v >> 6) & 63];
        *q++ = (uint8_t)XB64[v & 63];
    }
    if (n - i == 1) {
        uint32_t v = (uint32_t)s[i] << 16;
        *q++ = (uint8_t)XB64[v >> 18];
        *q++ = (uint8_t)XB64[(v >> 12) & 63];
        *q++ = '=';
        *q++ = '=';
    } else if (n - i == 2) {
        uint32_t v = (uint32_t)s[i] << 16 | (uint32_t)s[i + 1] << 8;
        *q++ = (uint8_t)XB64[v >> 18];
        *q++ = (uint8_t)XB64[(v >> 12) & 63];
        *q++ = (uint8_t)XB64[(v >> 6) & 63];
        *q++ = '=';
    }
    b->len = (size_t)(q - b->p);
}

/* A CSV/TSV field: quoted when empty or holding the delimiter, `"`, CR
 * or LF; quotes doubled. */
static void xb_csv_field(xbuf *b, const char *p, size_t n, char delim) {
    int quote = n == 0;
    for (size_t i = 0; i < n && !quote; i++) {
        char c = p[i];
        quote = c == delim || c == '"' || c == '\n' || c == '\r';
    }
    if (!quote) {
        xb_put(b, p, n);
        return;
    }
    xb_putc(b, '"');
    size_t start = 0;
    for (size_t i = 0; i < n; i++)
        if (p[i] == '"') {
            xb_put(b, p + start, i + 1 - start);
            xb_putc(b, '"');
            start = i + 1;
        }
    xb_put(b, p + start, n - start);
    xb_putc(b, '"');
}

/* A JSON string as Python's json.dumps(s, ensure_ascii=False) writes it. */
static void xb_json_str(xbuf *b, const char *p, size_t n) {
    static const char HEX[] = "0123456789abcdef";
    xb_putc(b, '"');
    size_t start = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)p[i];
        if (c >= 0x20 && c != '"' && c != '\\')
            continue;
        xb_put(b, p + start, i - start);
        start = i + 1;
        char e[6] = {'\\', 0};
        size_t k = 2;
        switch (c) {
        case '"': e[1] = '"'; break;
        case '\\': e[1] = '\\'; break;
        case '\n': e[1] = 'n'; break;
        case '\r': e[1] = 'r'; break;
        case '\t': e[1] = 't'; break;
        case '\b': e[1] = 'b'; break;
        case '\f': e[1] = 'f'; break;
        default:
            e[1] = 'u'; e[2] = '0'; e[3] = '0'; e[4] = HEX[c >> 4]; e[5] = HEX[c & 15];
            k = 6;
        }
        xb_put(b, e, k);
    }
    xb_put(b, p + start, n - start);
    xb_putc(b, '"');
}

static int export_text(sqlite3_stmt *st, int i, const unsigned char **p, size_t *n, err_t *e) {
    *p = sqlite3_column_text(st, i);
    *n = (size_t)sqlite3_column_bytes(st, i);
    if (*p == NULL)
        *p = (const unsigned char *)"";
    size_t chars;
    if (!utf8_check(*p, *n, &chars)) {
        const char *cname = sqlite3_column_name(st, i);
        char *txt = utf8_lossy((const char *)*p, *n);
        char *cn = utf8_lossy(cname ? cname : "", cname ? strlen(cname) : 0);
        err_set(e, "QueryError", NULL, "Could not decode to UTF-8 column '%s' with text '%s'", cn ? cn : "",
                txt ? txt : "");
        err_cause(e, "sqlite3.OperationalError", e->msg);
        free(txt);
        free(cn);
        return -1;
    }
    return 0;
}

/* One value of the current row, as JSON. */
static int export_json_value(xbuf *b, sqlite3_stmt *st, int i, err_t *e) {
    char num[40];
    switch (sqlite3_column_type(st, i)) {
    case SQLITE_INTEGER:
        xb_put(b, num, i64_text(sqlite3_column_int64(st, i), num));
        return 0;
    case SQLITE_FLOAT: {
        double d = sqlite3_column_double(st, i);
        if (isfinite(d))
            xb_put(b, num, py_float_repr(d, num));
        else
            xb_put(b, "null", 4);
        return 0;
    }
    case SQLITE_TEXT: {
        const unsigned char *p;
        size_t n;
        if (export_text(st, i, &p, &n, e) < 0)
            return -1;
        xb_json_str(b, (const char *)p, n);
        return 0;
    }
    case SQLITE_BLOB: {
        const void *p = sqlite3_column_blob(st, i);
        size_t n = (size_t)sqlite3_column_bytes(st, i);
        xb_put(b, "{\"$base64\":true,\"encoded\":\"", 27);
        xb_base64(b, p ? p : (const void *)"", n);
        xb_put(b, "\"}", 2);
        return 0;
    }
    default:
        xb_put(b, "null", 4);
        return 0;
    }
}

/* One value of the current row, as a CSV/TSV field. */
static int export_csv_value(xbuf *b, sqlite3_stmt *st, int i, char delim, err_t *e) {
    char num[40];
    switch (sqlite3_column_type(st, i)) {
    case SQLITE_INTEGER:
        xb_put(b, num, i64_text(sqlite3_column_int64(st, i), num));
        return 0;
    case SQLITE_FLOAT: {
        double d = sqlite3_column_double(st, i);
        if (isnan(d))
            xb_put(b, "NaN", 3);
        else if (isinf(d))
            d > 0 ? xb_put(b, "Infinity", 8) : xb_put(b, "-Infinity", 9);
        else
            xb_put(b, num, py_float_repr(d, num));
        return 0;
    }
    case SQLITE_TEXT: {
        const unsigned char *p;
        size_t n;
        if (export_text(st, i, &p, &n, e) < 0)
            return -1;
        xb_csv_field(b, (const char *)p, n, delim);
        return 0;
    }
    case SQLITE_BLOB: {
        const uint8_t *p = sqlite3_column_blob(st, i);
        size_t n = (size_t)sqlite3_column_bytes(st, i);
        if (n == 0) {
            xb_put(b, "\"\"", 2); /* the empty string's rule */
            return 0;
        }
        xb_base64(b, p, n); /* base64 never needs quoting */
        return 0;
    }
    default:
        return 0; /* NULL: an empty, unquoted field */
    }
}

static int export_row(export_state *x, sqlite3_stmt *st, int ncols, err_t *e) {
    xbuf *b = &x->out;
    switch (x->enc) {
    case ENC_CSV:
    case ENC_TSV: {
        char delim = x->enc == ENC_CSV ? ',' : '\t';
        for (int i = 0; i < ncols; i++) {
            if (i)
                xb_putc(b, delim);
            if (export_csv_value(b, st, i, delim, e) < 0)
                return -1;
        }
        xb_put(b, "\r\n", 2);
        break;
    }
    case ENC_JSON_ARRAYS:
        if (!x->first_row)
            xb_putc(b, ',');
        xb_putc(b, '[');
        for (int i = 0; i < ncols; i++) {
            if (i)
                xb_putc(b, ',');
            if (export_json_value(b, st, i, e) < 0)
                return -1;
        }
        xb_put(b, "]\n", 2);
        break;
    default: { /* objects, NDJSON */
        if (!x->first_row && x->enc == ENC_JSON_OBJECTS)
            xb_putc(b, ',');
        xb_putc(b, '{');
        int k = 0;
        for (int i = 0; i < ncols; i++) {
            if (x->keys[i] == NULL)
                continue; /* a repeated name: the first occurrence wins */
            if (k++)
                xb_putc(b, ',');
            xb_put(b, x->keys[i], x->key_len[i]);
            if (export_json_value(b, st, i, e) < 0)
                return -1;
        }
        xb_put(b, "}\n", 2);
    }
    }
    x->first_row = 0;
    if (b->failed) {
        err_set(e, "ResourceLimitExceeded", NULL, "worker ran out of memory (memory limit)");
        return -1;
    }
    return 0;
}

static void export_keys(export_state *x, sqlite3_stmt *st, int ncols) {
    x->keys = calloc((size_t)ncols + 1, sizeof *x->keys);
    x->key_len = calloc((size_t)ncols + 1, sizeof *x->key_len);
    if (x->keys == NULL || x->key_len == NULL)
        abort();
    x->keys[ncols] = (char *)-1; /* end marker for export_free */
    for (int i = 0; i < ncols; i++) {
        const char *name = sqlite3_column_name(st, i);
        char *lossy = utf8_lossy(name ? name : "", name ? strlen(name) : 0);
        int repeat = 0;
        for (int k = 0; k < i && !repeat; k++)
            repeat = strcmp(sqlite3_column_name(st, k) ? sqlite3_column_name(st, k) : "", name ? name : "") == 0;
        if (!repeat) {
            xbuf kb = {0};
            xb_json_str(&kb, lossy ? lossy : "", lossy ? strlen(lossy) : 0);
            xb_putc(&kb, ':');
            xb_putc(&kb, 0);
            x->keys[i] = (char *)kb.p;
            x->key_len[i] = kb.len - 1;
        }
        free(lossy);
    }
}

/* -- producing chunks ---------------------------------------------------------- */

/* Fill x->out with rows until it reaches chunk_bytes, the rows run out or
 * max_rows is reached. 1: bytes or columns ready; 0: the export is over;
 * -1: error. The first call prepares and takes the first step. */
static int export_chunk(job *j, stream *s, err_t *e) {
    export_state *x = s->ex;
    conn *wc = s->wc;
    int64_t start = mono_ns();
    int64_t ms = s->has_limit ? (s->remaining_ms > 0 ? (int64_t)s->remaining_ms : 0) : 0;
    progress_ctx pc;
    int result = -1;
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
            if (sqlite3_stmt_readonly(s->st) == 0) {
                /* Exports run on read connections, which refuse writes
                 * anyway; this says so before anything runs. */
                err_set(e, "QueryError", NULL, "export runs reads only");
                goto out;
            }
            int rc = sqlite3_step(s->st);
            if (!step_ok(rc)) {
                err_sqlite(e, wc->db, rc);
                goto out;
            }
            s->has_row = rc == SQLITE_ROW;
            if (x->enc == ENC_JSON_OBJECTS || x->enc == ENC_NDJSON)
                export_keys(x, s->st, s->ncols);
        }
        result = 1; /* the columns are known */
        goto out;
    }
    while (s->has_row && (int64_t)x->out.len < x->chunk_bytes) {
        if (s->ncols == 0)
            break;
        if (x->has_max_rows && x->rows >= x->max_rows) {
            x->truncated = 1;
            s->has_row = 0;
            break;
        }
        if (atomic_load_explicit(&j->cancelled, memory_order_relaxed)) {
            err_interrupted(e);
            goto out;
        }
        if (export_row(x, s->st, s->ncols, e) < 0)
            goto out;
        x->rows++;
        int rc = sqlite3_step(s->st);
        if (!step_ok(rc)) {
            err_sqlite(e, wc->db, rc);
            goto out;
        }
        s->has_row = rc == SQLITE_ROW;
    }
    if (s->has_row && x->has_max_rows && x->rows >= x->max_rows) {
        x->truncated = 1;
        s->has_row = 0;
    }
    result = x->out.len > 0 ? 1 : 0;
out:
    stmt_leave(j);
out_noleave:
    limit_remove(wc);
    if (s->has_limit)
        s->remaining_ms -= (double)(mono_ns() - start) / 1e6;
    return result;
}

static void export_columns_value(wbuf *w, stream *s) {
    w_array(w, (uint32_t)s->ncols);
    for (int i = 0; i < s->ncols; i++) {
        const char *name = sqlite3_column_name(s->st, i);
        w_str_lossy(w, name ? name : "", name ? strlen(name) : 0);
    }
    w_end(w);
}

static int export_send(wbuf *w, err_t *e) {
    w_end(w);
    if (w->failed) {
        wbuf_error(w, e);
        return -1;
    }
    send_frame(w);
    return 0;
}

static int export_send_end(job *j, stream *s, wbuf *w, err_t *e) {
    export_state *x = s->ex;
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map_open(w);
    w_cstr(w, "id"); w_int(w, s->id);
    w_cstr(w, "end"); w_bool(w, 1);
    w_cstr(w, "row_count"); w_int(w, x->rows);
    w_cstr(w, "truncated"); w_bool(w, x->truncated);
    reply_in_tx(w, s->wc);
    (void)j;
    return export_send(w, e);
}

/* Send the chunk in x->out as one data frame. */
static int export_send_data(stream *s, wbuf *w, err_t *e) {
    export_state *x = s->ex;
    size_t cap = atomic_load(&g_max_frame);
    if (x->transport == T_RAW) {
        size_t n = x->out.len - RAW_HEAD + 9; /* payload: 0x00, id, bytes */
        if (n > cap) {
            err_set(e, "ResourceLimitExceeded", NULL,
                    "reply too large: frame of more than %zu bytes exceeds the %zu-byte frame size cap", cap, cap);
            return -1;
        }
        uint8_t *h = x->out.p;
        h[0] = (uint8_t)(n >> 24); h[1] = (uint8_t)(n >> 16); h[2] = (uint8_t)(n >> 8); h[3] = (uint8_t)n;
        h[4] = 0;
        uint64_t id = (uint64_t)s->id;
        for (int i = 0; i < 8; i++)
            h[5 + i] = (uint8_t)(id >> (56 - 8 * i));
        write_all(x->out.p, x->out.len);
        x->out.len = RAW_HEAD;
        return 0;
    }
    w_start(w, atomic_load(&g_codec), cap);
    w_map_open(w);
    w_cstr(w, "id"); w_int(w, s->id);
    w_cstr(w, "data"); w_bin(w, x->out.p, x->out.len);
    w_cstr(w, "rows"); w_int(w, 1);
    x->out.len = 0;
    return export_send(w, e);
}

static size_t export_base(export_state *x) { return x->transport == T_RAW ? RAW_HEAD : 0; }

/* Like pump(): send up to `credit` data frames, then leave the cursor
 * open; the end frame when the rows run out. 1 on success (everything
 * already sent), -1 on error. */
static int export_pump(job *j, stream *s, wbuf *w, err_t *e) {
    export_state *x = s->ex;
    if (!s->started) {
        if (export_chunk(j, s, e) < 0)
            return -1;
        w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
        w_map_open(w);
        w_cstr(w, "id"); w_int(w, s->id);
        w_cstr(w, "columns");
        export_columns_value(w, s);
        if (export_send(w, e) < 0)
            return -1;
    }
    while (s->credit > 0) {
        if (atomic_load(&j->cancelled)) {
            err_set(e, "QueryInterrupted", NULL, "query cancelled");
            return -1;
        }
        int r = export_chunk(j, s, e);
        if (r < 0)
            return -1;
        if (x->out.len > export_base(x)) {
            if (export_send_data(s, w, e) < 0)
                return -1;
            s->credit--;
            if (s->credit == 0)
                break; /* the end frame waits for the next `more`, as pump() does */
        }
        if (!s->has_row) {
            int rc = export_send_end(j, s, w, e);
            stream_drop(s);
            return rc < 0 ? -1 : 1;
        }
    }
    return 1;
}

#ifndef CWORKER_WASM
static int fd_write_all(int fd, const uint8_t *p, size_t n) {
    while (n > 0) {
        /* At most 8 KB per write: on macOS (Darwin 27) a blocking write of
         * 16 KB or more to a FIFO stalls for good once the reader has
         * drained it, with the reader's kqueue never signalled again
         * (reproduced in pure Python: scripts/macos_fifo_stall.py). 8 KB
         * writes do not. */
        ssize_t k = write(fd, p, n > 8192 ? 8192 : n);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

/* The "fifo" transport: the whole export runs in this request. */
static int export_fifo(job *j, stream *s, wbuf *w, err_t *e) {
    export_state *x = s->ex;
    const val *path = field(j, "data_path");
    if (path == NULL || path->t != V_STR) {
        err_set(e, "ValueError", NULL, "export with transport 'fifo' needs data_path");
        e->builtin = 1;
        return -1;
    }
    char *p = xstrndup(path->u.s.p, path->u.s.n);
    /* Non-blocking open fails at once (ENXIO) if the parent is not
     * holding the read end; then blocking writes, the pipe's backpressure. */
    x->fd = open(p, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    free(p);
    if (x->fd < 0) {
        err_set(e, "QueryError", NULL, "cannot open the export pipe: %s", strerror(errno));
        return -1;
    }
    fcntl(x->fd, F_SETFL, fcntl(x->fd, F_GETFL) & ~O_NONBLOCK);
    if (export_chunk(j, s, e) < 0)
        return -1;
    /* Preamble: the columns as a JSON array, length-prefixed. */
    wbuf cw;
    w_init(&cw, CODEC_JSON, atomic_load(&g_max_frame));
    export_columns_value(&cw, s);
    w_finish_frame(&cw);
    int bad = cw.failed || fd_write_all(x->fd, cw.buf, cw.len) < 0;
    w_free(&cw);
    while (!bad) {
        if (atomic_load(&j->cancelled)) {
            err_set(e, "QueryInterrupted", NULL, "query cancelled");
            return -1;
        }
        int r = export_chunk(j, s, e);
        if (r < 0)
            return -1;
        if (x->out.len) {
            bad = fd_write_all(x->fd, x->out.p, x->out.len) < 0;
            x->out.len = 0;
        }
        if (!s->has_row)
            break;
    }
    if (bad) {
        err_set(e, "QueryInterrupted", NULL, "export pipe closed by the reader");
        return -1;
    }
    close(x->fd);
    x->fd = -1;
    reply_begin(w, j);
    w_cstr(w, "result");
    w_map_open(w);
    w_cstr(w, "row_count"); w_int(w, x->rows);
    w_cstr(w, "truncated"); w_bool(w, x->truncated);
    w_end(w);
    reply_in_tx(w, s->wc);
    return 0;
}
#endif

#ifndef CWORKER_WASM
/* -- the "socket" transport ------------------------------------------------------ */

/* Receive one descriptor over the Unix socket at `path`. */
static int recv_handoff_fd(const char *path, err_t *e) {
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof sa);
    sa.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof sa.sun_path) {
        err_set(e, "ValueError", NULL, "handoff_path too long");
        e->builtin = 1;
        return -1;
    }
    memcpy(sa.sun_path, path, strlen(path) + 1);
    int us = socket(AF_UNIX, SOCK_STREAM, 0);
    if (us < 0 || connect(us, (struct sockaddr *)&sa, sizeof sa) < 0) {
        err_set(e, "QueryError", NULL, "cannot connect to the handoff socket: %s", strerror(errno));
        if (us >= 0)
            close(us);
        return -1;
    }
    char byte;
    struct iovec iov = {&byte, 1};
    union {
        struct cmsghdr h;
        char buf[CMSG_SPACE(sizeof(int))];
    } ctl;
    struct msghdr m;
    memset(&m, 0, sizeof m);
    m.msg_iov = &iov;
    m.msg_iovlen = 1;
    m.msg_control = ctl.buf;
    m.msg_controllen = sizeof ctl.buf;
    ssize_t k;
    do
        k = recvmsg(us, &m, 0);
    while (k < 0 && errno == EINTR);
    close(us);
    struct cmsghdr *c = k > 0 ? CMSG_FIRSTHDR(&m) : NULL;
    if (c == NULL || c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS ||
        c->cmsg_len != CMSG_LEN(sizeof(int))) {
        err_set(e, "QueryError", NULL, "no descriptor received on the handoff socket");
        return -1;
    }
    int fd;
    memcpy(&fd, CMSG_DATA(c), sizeof fd);
    fcntl(fd, F_SETFD, FD_CLOEXEC);
    return fd;
}

/* Write all of p to a socket that may be non-blocking. O_NONBLOCK belongs
 * to the open file description the parent shares with this descriptor, so
 * it is left as it is and EAGAIN is waited out with poll(), checking for
 * cancellation every 100 ms. 0: written; -1: the peer is gone (errno);
 * -2: cancelled. */
static int sock_write_all(job *j, int fd, const uint8_t *p, size_t n) {
    while (n > 0) {
#ifdef MSG_NOSIGNAL
        ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
#else
        ssize_t k = send(fd, p, n, 0); /* SIGPIPE is ignored (main) */
#endif
        if (k >= 0) {
            p += k;
            n -= (size_t)k;
            continue;
        }
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return -1;
        struct pollfd pf = {fd, POLLOUT, 0};
        for (;;) {
            if (atomic_load(&j->cancelled))
                return -2;
            int r = poll(&pf, 1, 100);
            if (r > 0 || (r < 0 && errno != EINTR))
                break;
        }
        if (pf.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            errno = EPIPE;
            return -1;
        }
    }
    return 0;
}

/* x->out holds SOCK_HEAD bytes of room, then data: frame it as one HTTP
 * chunk ("<hex>\r\n" data "\r\n") in place and write it. */
static int sock_send_chunk(job *j, export_state *x, int fd) {
    size_t n = x->out.len - SOCK_HEAD;
    char hex[16];
    int k = snprintf(hex, sizeof hex, "%zx\r\n", n);
    uint8_t *start = x->out.p + SOCK_HEAD - k;
    memcpy(start, hex, (size_t)k);
    xb_put(&x->out, "\r\n", 2);
    if (x->out.failed)
        return -1;
    int rc = sock_write_all(j, fd, start, x->out.len - (SOCK_HEAD - (size_t)k));
    x->out.len = SOCK_HEAD;
    return rc;
}

static int export_socket(job *j, stream *s, wbuf *w, err_t *e) {
    export_state *x = s->ex;
    const val *path = field(j, "handoff_path");
    if (path == NULL || path->t != V_STR) {
        err_set(e, "ValueError", NULL, "export with transport 'socket' needs handoff_path");
        e->builtin = 1;
        return -1;
    }
    if (export_chunk(j, s, e) < 0)
        return -1;
    /* The columns first, so the parent can send the head before it hands
     * the connection over. "rows": 0 makes it count as a frame. */
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map_open(w);
    w_cstr(w, "id"); w_int(w, s->id);
    w_cstr(w, "columns");
    export_columns_value(w, s);
    w_cstr(w, "rows"); w_int(w, 0);
    if (export_send(w, e) < 0)
        return -1;
    char *p = xstrndup(path->u.s.p, path->u.s.n);
    int fd = recv_handoff_fd(p, e);
    free(p);
    if (fd < 0)
        return -1;
    x->fd = fd; /* closed by export_free on every error path */
    int64_t bytes = 0;
    for (;;) {
        if (atomic_load(&j->cancelled)) {
            err_set(e, "QueryInterrupted", NULL, "query cancelled");
            return -1;
        }
        if (export_chunk(j, s, e) < 0)
            return -1;
        if (x->out.len > SOCK_HEAD) {
            bytes += (int64_t)(x->out.len - SOCK_HEAD);
            int rc = sock_send_chunk(j, x, fd);
            if (rc == -2) {
                err_set(e, "QueryInterrupted", NULL, "query cancelled");
                return -1;
            }
            if (rc < 0) {
                err_set(e, "QueryInterrupted", NULL, "client connection closed: %s", strerror(errno));
                return -1;
            }
        }
        if (!s->has_row)
            break;
    }
    /* The copy is closed before the reply: once the parent hears back, this
     * process can no longer touch the client's connection. */
    close(x->fd);
    x->fd = -1;
    w_start(w, atomic_load(&g_codec), atomic_load(&g_max_frame));
    w_map_open(w);
    w_cstr(w, "id"); w_int(w, s->id);
    w_cstr(w, "end"); w_bool(w, 1);
    w_cstr(w, "row_count"); w_int(w, x->rows);
    w_cstr(w, "truncated"); w_bool(w, x->truncated);
    w_cstr(w, "bytes"); w_int(w, bytes);
    if (export_send(w, e) < 0)
        return -1;
    return 1;
}

#endif

static int h_export(job *j, conn *wc, wbuf *w, err_t *e) {
    if (check_tx(j, wc, e) < 0)
        return -1;
    const val *enc = field(j, "encoding");
    int encv;
    if (val_is_str(enc, "csv")) encv = ENC_CSV;
    else if (val_is_str(enc, "tsv")) encv = ENC_TSV;
    else if (val_is_str(enc, "json-objects")) encv = ENC_JSON_OBJECTS;
    else if (val_is_str(enc, "json-arrays")) encv = ENC_JSON_ARRAYS;
    else if (val_is_str(enc, "ndjson")) encv = ENC_NDJSON;
    else {
        err_set(e, "ValueError", NULL, "unknown export encoding");
        e->builtin = 1;
        return -1;
    }
    const val *tv = field(j, "transport");
    int transport = T_FRAMES;
    if (tv == NULL || tv->t == V_NIL || val_is_str(tv, "frames")) transport = T_FRAMES;
    else if (val_is_str(tv, "raw")) transport = T_RAW;
    else if (val_is_str(tv, "fifo")) transport = T_FIFO;
    else if (val_is_str(tv, "socket")) transport = T_SOCKET;
    else {
        err_set(e, "ValueError", NULL, "unknown export transport");
        e->builtin = 1;
        return -1;
    }
#ifdef CWORKER_WASM
    if (transport == T_FIFO || transport == T_SOCKET) {
        err_set(e, "NotSupported", NULL, "no fifo or socket transport in the WebAssembly build");
        return -1;
    }
#endif
    int64_t chunk = get_int(&j->msg, "chunk_bytes", 65536, NULL);
    if (chunk < 1 || chunk > (1 << 26)) {
        err_set(e, "ValueError", NULL, "chunk_bytes must be between 1 and 67108864");
        e->builtin = 1;
        return -1;
    }
    export_state *x = xmalloc(sizeof *x);
    memset(x, 0, sizeof *x);
    x->enc = encv;
    x->transport = transport;
    x->chunk_bytes = chunk + (int64_t)(transport == T_RAW ? RAW_HEAD : transport == T_SOCKET ? SOCK_HEAD : 0);
    x->max_rows = get_int(&j->msg, "max_rows", 0, &x->has_max_rows);
    if (x->has_max_rows && x->max_rows < 0)
        x->max_rows = 0;
    x->first_row = 1;
    x->fd = -1;
    if (transport == T_RAW || transport == T_SOCKET) {
        size_t head = transport == T_RAW ? RAW_HEAD : SOCK_HEAD;
        xb_grow(&x->out, head + (size_t)chunk + 2);
        x->out.len = head;
    }
    stream *s = xmalloc(sizeof *s);
    memset(s, 0, sizeof *s);
    s->id = j->id;
    s->wc = wc;
    s->first = 1;
    s->ex = x;
    s->credit = get_int(&j->msg, "credit", 1, NULL);
    int present;
    int64_t tl = get_int(&j->msg, "time_limit_ms", 0, &present);
    s->has_limit = present;
    s->remaining_ms = (double)tl;
#ifndef CWORKER_WASM
    if (transport == T_FIFO) {
        /* Not registered as a stream: it never pauses. */
        int rc = export_fifo(j, s, w, e);
        stream_drop_unlisted(s);
        return rc;
    }
    if (transport == T_SOCKET) {
        int rc = export_socket(j, s, w, e);
        stream_drop_unlisted(s);
        return rc;
    }
#endif
    pthread_mutex_lock(&g_mu);
    stream *old = stream_find(s->id);
    s->next = g_streams;
    g_streams = s;
    pthread_mutex_unlock(&g_mu);
    if (old)
        stream_drop(old);
    return export_pump(j, s, w, e);
}

static void stream_drop_unlisted(stream *s) {
    if (s->st)
        sqlite3_finalize(s->st);
    export_free(s->ex);
    free(s);
}

/* hello's "export": what this worker can serialise and how. */
static void export_hello(wbuf *w) {
    w_map_open(w);
    w_cstr(w, "formats");
    w_array(w, 3); w_cstr(w, "csv"); w_cstr(w, "tsv"); w_cstr(w, "json"); w_end(w);
    w_cstr(w, "transports");
#ifdef CWORKER_WASM
    w_array(w, 2); w_cstr(w, "frames"); w_cstr(w, "raw"); w_end(w);
#else
    w_array(w, 4); w_cstr(w, "frames"); w_cstr(w, "raw"); w_cstr(w, "fifo"); w_cstr(w, "socket"); w_end(w);
#endif
    w_end(w);
}
