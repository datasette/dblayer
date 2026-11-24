/* Codecs: see codec.h and docs/worker-protocol.md ("Codecs"). */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "codec.h"

#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- arena ------------------------------------------------------------------ */

struct ablock {
    ablock *next;
    size_t used, cap;
    _Alignas(16) unsigned char data[];
};

void *arena_alloc(arena *a, size_t n) {
    n = (n + 15) & ~(size_t)15;
    ablock *b = a->head;
    if (b == NULL || b->cap - b->used < n) {
        size_t cap = n > 65536 ? n : 65536;
        b = malloc(sizeof(ablock) + cap);
        if (b == NULL)
            return NULL;
        b->used = 0;
        b->cap = cap;
        b->next = a->head;
        a->head = b;
    }
    void *p = b->data + b->used;
    b->used += n;
    return p;
}

void arena_free(arena *a) {
    ablock *b = a->head;
    while (b) {
        ablock *next = b->next;
        free(b);
        b = next;
    }
    a->head = NULL;
}

/* -- helpers ------------------------------------------------------------------ */

static void seterr(char *err, size_t errlen, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, errlen, fmt, ap);
    va_end(ap);
}

const val *map_get(const val *m, const char *key) {
    if (m == NULL || m->t != V_MAP)
        return NULL;
    size_t kl = strlen(key);
    for (size_t i = 0; i < m->u.a.n; i++) {
        const val *k = &m->u.a.v[2 * i];
        if (k->t == V_STR && k->u.s.n == kl && memcmp(k->u.s.p, key, kl) == 0)
            return &m->u.a.v[2 * i + 1];
    }
    return NULL;
}

int val_is_str(const val *v, const char *s) {
    size_t n = strlen(s);
    return v && v->t == V_STR && v->u.s.n == n && memcmp(v->u.s.p, s, n) == 0;
}

int64_t get_int(const val *m, const char *key, int64_t dflt, int *present) {
    const val *v = map_get(m, key);
    if (present)
        *present = 0;
    if (v == NULL || v->t == V_NIL)
        return dflt;
    if (present)
        *present = 1;
    if (v->t == V_INT)
        return v->u.i;
    if (v->t == V_BOOL)
        return v->u.b;
    if (v->t == V_FLOAT)
        return (int64_t)v->u.d;
    return dflt;
}

/* -- UTF-8 ------------------------------------------------------------------ */

int utf8_check(const unsigned char *p, size_t n, size_t *chars) {
    size_t i = 0, c = 0;
    while (i < n) {
        unsigned char b = p[i];
        if (b < 0x80) {
            i++;
        } else if (b >= 0xC2 && b <= 0xDF) {
            if (i + 1 >= n || (p[i + 1] & 0xC0) != 0x80)
                return 0;
            i += 2;
        } else if (b >= 0xE0 && b <= 0xEF) {
            if (i + 2 >= n || (p[i + 1] & 0xC0) != 0x80 || (p[i + 2] & 0xC0) != 0x80)
                return 0;
            if (b == 0xE0 && p[i + 1] < 0xA0)
                return 0; /* overlong */
            if (b == 0xED && p[i + 1] >= 0xA0)
                return 0; /* surrogate */
            i += 3;
        } else if (b >= 0xF0 && b <= 0xF4) {
            if (i + 3 >= n || (p[i + 1] & 0xC0) != 0x80 || (p[i + 2] & 0xC0) != 0x80 ||
                (p[i + 3] & 0xC0) != 0x80)
                return 0;
            if (b == 0xF0 && p[i + 1] < 0x90)
                return 0;
            if (b == 0xF4 && p[i + 1] >= 0x90)
                return 0;
            i += 4;
        } else {
            return 0;
        }
        c++;
    }
    if (chars)
        *chars = c;
    return 1;
}

char *utf8_lossy(const char *s, size_t n) {
    const unsigned char *p = (const unsigned char *)s;
    char *out = malloc(n * 3 + 1);
    if (out == NULL)
        return NULL;
    size_t o = 0, i = 0;
    while (i < n) {
        size_t len = 0;
        unsigned char b = p[i];
        if (b < 0x80)
            len = 1;
        else if (b >= 0xC2 && b <= 0xDF)
            len = 2;
        else if (b >= 0xE0 && b <= 0xEF)
            len = 3;
        else if (b >= 0xF0 && b <= 0xF4)
            len = 4;
        if (len && i + len <= n && utf8_check(p + i, len, NULL)) {
            memcpy(out + o, p + i, len);
            o += len;
            i += len;
        } else {
            memcpy(out + o, "\xEF\xBF\xBD", 3);
            o += 3;
            i++;
        }
    }
    out[o] = 0;
    return out;
}

/* -- base64 ------------------------------------------------------------------ */

static const char B64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int b64val(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

/* Lenient like Python's b64decode(validate=False): skips non-alphabet bytes. */
static int b64decode(const char *s, size_t n, arena *a, const char **out, size_t *outlen) {
    unsigned char *o = arena_alloc(a, n / 4 * 3 + 3);
    if (o == NULL)
        return -1;
    size_t k = 0;
    uint32_t acc = 0;
    int bits = 0;
    for (size_t i = 0; i < n; i++) {
        int v = b64val((unsigned char)s[i]);
        if (v < 0) {
            if (s[i] == '=')
                break;
            continue;
        }
        acc = (acc << 6) | (uint32_t)v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            o[k++] = (unsigned char)(acc >> bits);
        }
    }
    *out = (const char *)o;
    *outlen = k;
    return 0;
}

/* -- MessagePack decoder ------------------------------------------------------- */

typedef struct {
    const uint8_t *p, *end;
    arena *a;
    char *err;
    size_t errlen;
    int depth;
} dctx;

#define MAX_DEPTH 200

static uint64_t be(const uint8_t *p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; i++)
        v = (v << 8) | p[i];
    return v;
}

static int need(dctx *d, size_t n) {
    if ((size_t)(d->end - d->p) < n) {
        seterr(d->err, d->errlen, "truncated MessagePack data");
        return -1;
    }
    return 0;
}

static int mp_value(dctx *d, val *out);

static int mp_container(dctx *d, val *out, size_t n, int is_map) {
    if (++d->depth > MAX_DEPTH) {
        seterr(d->err, d->errlen, "nesting too deep");
        return -1;
    }
    size_t items = is_map ? 2 * n : n;
    /* Every item takes at least one byte: refuse absurd counts early. */
    if (items > (size_t)(d->end - d->p)) {
        seterr(d->err, d->errlen, "truncated MessagePack data");
        return -1;
    }
    out->t = is_map ? V_MAP : V_ARR;
    out->u.a.n = n;
    out->u.a.v = items ? arena_alloc(d->a, items * sizeof(val)) : NULL;
    if (items && out->u.a.v == NULL) {
        seterr(d->err, d->errlen, "out of memory");
        return -1;
    }
    for (size_t i = 0; i < items; i++)
        if (mp_value(d, &out->u.a.v[i]) < 0)
            return -1;
    d->depth--;
    return 0;
}

static int mp_bytes(dctx *d, val *out, vtype t, size_t n) {
    if (need(d, n) < 0)
        return -1;
    out->t = t;
    out->u.s.p = (const char *)d->p;
    out->u.s.n = n;
    d->p += n;
    return 0;
}

static int mp_ext(dctx *d, val *out, size_t n) {
    if (need(d, n + 1) < 0)
        return -1;
    int8_t type = (int8_t)d->p[0];
    d->p++;
    if (type != 1) {
        seterr(d->err, d->errlen, "unsupported MessagePack ext type %d", type);
        return -1;
    }
    /* ext 1: an integer outside 64 bits, as decimal text */
    out->t = V_BIGINT;
    out->u.s.p = (const char *)d->p;
    out->u.s.n = n;
    d->p += n;
    return 0;
}

static int mp_value(dctx *d, val *out) {
    if (need(d, 1) < 0)
        return -1;
    uint8_t b = *d->p++;
    if (b <= 0x7f) { out->t = V_INT; out->u.i = b; return 0; }
    if (b >= 0xe0) { out->t = V_INT; out->u.i = (int8_t)b; return 0; }
    if ((b & 0xf0) == 0x80) return mp_container(d, out, b & 0x0f, 1);
    if ((b & 0xf0) == 0x90) return mp_container(d, out, b & 0x0f, 0);
    if ((b & 0xe0) == 0xa0) return mp_bytes(d, out, V_STR, b & 0x1f);
    size_t n;
    switch (b) {
    case 0xc0: out->t = V_NIL; return 0;
    case 0xc2: out->t = V_BOOL; out->u.b = 0; return 0;
    case 0xc3: out->t = V_BOOL; out->u.b = 1; return 0;
    case 0xc4: case 0xc5: case 0xc6: case 0xd9: case 0xda: case 0xdb: {
        int w = (b == 0xc4 || b == 0xd9) ? 1 : (b == 0xc5 || b == 0xda) ? 2 : 4;
        if (need(d, (size_t)w) < 0) return -1;
        n = (size_t)be(d->p, w);
        d->p += w;
        return mp_bytes(d, out, b >= 0xd9 ? V_STR : V_BIN, n);
    }
    case 0xc7: case 0xc8: case 0xc9: {
        int w = b == 0xc7 ? 1 : b == 0xc8 ? 2 : 4;
        if (need(d, (size_t)w) < 0) return -1;
        n = (size_t)be(d->p, w);
        d->p += w;
        return mp_ext(d, out, n);
    }
    case 0xd4: return mp_ext(d, out, 1);
    case 0xd5: return mp_ext(d, out, 2);
    case 0xd6: return mp_ext(d, out, 4);
    case 0xd7: return mp_ext(d, out, 8);
    case 0xd8: return mp_ext(d, out, 16);
    case 0xca: {
        if (need(d, 4) < 0) return -1;
        uint32_t u = (uint32_t)be(d->p, 4);
        float f;
        memcpy(&f, &u, 4);
        d->p += 4;
        out->t = V_FLOAT;
        out->u.d = f;
        return 0;
    }
    case 0xcb: {
        if (need(d, 8) < 0) return -1;
        uint64_t u = be(d->p, 8);
        double f;
        memcpy(&f, &u, 8);
        d->p += 8;
        out->t = V_FLOAT;
        out->u.d = f;
        return 0;
    }
    case 0xcc: case 0xcd: case 0xce: case 0xcf: {
        int w = 1 << (b - 0xcc);
        if (need(d, (size_t)w) < 0) return -1;
        uint64_t u = be(d->p, w);
        d->p += w;
        if (u > (uint64_t)INT64_MAX) {
            /* fits msgpack's uint64 but not SQLite's integer */
            char *t = arena_alloc(d->a, 24);
            if (t == NULL) { seterr(d->err, d->errlen, "out of memory"); return -1; }
            int len = snprintf(t, 24, "%llu", (unsigned long long)u);
            out->t = V_BIGINT;
            out->u.s.p = t;
            out->u.s.n = (size_t)len;
            return 0;
        }
        out->t = V_INT;
        out->u.i = (int64_t)u;
        return 0;
    }
    case 0xd0: case 0xd1: case 0xd2: case 0xd3: {
        int w = 1 << (b - 0xd0);
        if (need(d, (size_t)w) < 0) return -1;
        uint64_t u = be(d->p, w);
        d->p += w;
        int64_t v;
        if (w == 1) v = (int8_t)u;
        else if (w == 2) v = (int16_t)u;
        else if (w == 4) v = (int32_t)u;
        else memcpy(&v, &u, 8);
        out->t = V_INT;
        out->u.i = v;
        return 0;
    }
    case 0xdc: case 0xdd: case 0xde: case 0xdf: {
        int w = (b == 0xdc || b == 0xde) ? 2 : 4;
        if (need(d, (size_t)w) < 0) return -1;
        n = (size_t)be(d->p, w);
        d->p += w;
        return mp_container(d, out, n, b >= 0xde);
    }
    default:
        seterr(d->err, d->errlen, "invalid MessagePack byte 0x%02x", b);
        return -1;
    }
}

/* -- JSON decoder ------------------------------------------------------------- */

static void j_ws(dctx *d) {
    while (d->p < d->end && (*d->p == ' ' || *d->p == '\t' || *d->p == '\n' || *d->p == '\r'))
        d->p++;
}

static int j_lit(dctx *d, const char *lit) {
    size_t n = strlen(lit);
    if ((size_t)(d->end - d->p) >= n && memcmp(d->p, lit, n) == 0) {
        d->p += n;
        return 1;
    }
    return 0;
}

static int hex4(const uint8_t *p, unsigned *out) {
    unsigned v = 0;
    for (int i = 0; i < 4; i++) {
        uint8_t c = p[i];
        v <<= 4;
        if (c >= '0' && c <= '9') v |= c - '0';
        else if (c >= 'a' && c <= 'f') v |= c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') v |= c - 'A' + 10;
        else return -1;
    }
    *out = v;
    return 0;
}

static size_t put_utf8(char *o, unsigned cp) {
    if (cp < 0x80) { o[0] = (char)cp; return 1; }
    if (cp < 0x800) { o[0] = (char)(0xC0 | (cp >> 6)); o[1] = (char)(0x80 | (cp & 0x3F)); return 2; }
    if (cp < 0x10000) {
        o[0] = (char)(0xE0 | (cp >> 12));
        o[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        o[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    o[0] = (char)(0xF0 | (cp >> 18));
    o[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    o[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    o[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

static int j_string(dctx *d, val *out) {
    /* d->p is just past the opening quote */
    const uint8_t *s = d->p;
    int escaped = 0;
    while (d->p < d->end && *d->p != '"') {
        if (*d->p == '\\') {
            escaped = 1;
            d->p++;
        }
        d->p++;
    }
    if (d->p >= d->end) {
        seterr(d->err, d->errlen, "unterminated JSON string");
        return -1;
    }
    size_t raw = (size_t)(d->p - s);
    d->p++; /* closing quote */
    out->t = V_STR;
    if (!escaped) {
        out->u.s.p = (const char *)s;
        out->u.s.n = raw;
        return 0;
    }
    char *o = arena_alloc(d->a, raw + 1);
    if (o == NULL) {
        seterr(d->err, d->errlen, "out of memory");
        return -1;
    }
    size_t k = 0;
    for (size_t i = 0; i < raw; i++) {
        uint8_t c = s[i];
        if (c != '\\') {
            o[k++] = (char)c;
            continue;
        }
        c = s[++i];
        switch (c) {
        case '"': o[k++] = '"'; break;
        case '\\': o[k++] = '\\'; break;
        case '/': o[k++] = '/'; break;
        case 'b': o[k++] = '\b'; break;
        case 'f': o[k++] = '\f'; break;
        case 'n': o[k++] = '\n'; break;
        case 'r': o[k++] = '\r'; break;
        case 't': o[k++] = '\t'; break;
        case 'u': {
            unsigned cp, lo;
            if (i + 4 >= raw || hex4(s + i + 1, &cp) < 0) {
                seterr(d->err, d->errlen, "invalid \\u escape");
                return -1;
            }
            i += 4;
            if (cp >= 0xD800 && cp <= 0xDBFF && i + 6 < raw && s[i + 1] == '\\' && s[i + 2] == 'u' && hex4(s + i + 3, &lo) == 0 &&
                lo >= 0xDC00 && lo <= 0xDFFF) {
                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                i += 6;
            }
            /* A lone surrogate is kept as its 3-byte form, like Python's
             * "surrogatepass". */
            k += put_utf8(o + k, cp);
            break;
        }
        default:
            seterr(d->err, d->errlen, "invalid JSON escape");
            return -1;
        }
    }
    out->u.s.p = o;
    out->u.s.n = k;
    return 0;
}

static int j_number(dctx *d, val *out) {
    const uint8_t *s = d->p;
    int is_float = 0;
    if (d->p < d->end && *d->p == '-')
        d->p++;
    if (j_lit(d, "Infinity")) {
        out->t = V_FLOAT;
        out->u.d = *s == '-' ? -INFINITY : INFINITY;
        return 0;
    }
    while (d->p < d->end) {
        uint8_t c = *d->p;
        if (c >= '0' && c <= '9') {
        } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
            is_float = 1;
        } else {
            break;
        }
        d->p++;
    }
    size_t n = (size_t)(d->p - s);
    if (n == 0 || (n == 1 && *s == '-')) {
        seterr(d->err, d->errlen, "invalid JSON number");
        return -1;
    }
    if (!is_float) {
        int neg = *s == '-';
        uint64_t v = 0, lim = neg ? (uint64_t)INT64_MAX + 1 : (uint64_t)INT64_MAX;
        int over = 0;
        for (size_t i = neg; i < n; i++) {
            unsigned dg = s[i] - '0';
            if (v > (lim - dg) / 10) {
                over = 1;
                break;
            }
            v = v * 10 + dg;
        }
        if (over) {
            out->t = V_BIGINT;
            out->u.s.p = (const char *)s;
            out->u.s.n = n;
            return 0;
        }
        out->t = V_INT;
        out->u.i = neg ? (int64_t)(0 - v) : (int64_t)v;
        return 0;
    }
    char buf[64];
    char *tmp = n < sizeof buf ? buf : arena_alloc(d->a, n + 1);
    if (tmp == NULL) {
        seterr(d->err, d->errlen, "out of memory");
        return -1;
    }
    memcpy(tmp, s, n);
    tmp[n] = 0;
    char *endp;
    out->t = V_FLOAT;
    out->u.d = strtod(tmp, &endp);
    if ((size_t)(endp - tmp) != n) {
        seterr(d->err, d->errlen, "invalid JSON number");
        return -1;
    }
    return 0;
}

typedef struct { val *v; size_t n, cap; } vlist;

static int vl_push(dctx *d, vlist *l, val *x) {
    if (l->n == l->cap) {
        size_t cap = l->cap ? l->cap * 2 : 8;
        val *nv = arena_alloc(d->a, cap * sizeof(val));
        if (nv == NULL) {
            seterr(d->err, d->errlen, "out of memory");
            return -1;
        }
        if (l->n)
            memcpy(nv, l->v, l->n * sizeof(val));
        l->v = nv;
        l->cap = cap;
    }
    l->v[l->n++] = *x;
    return 0;
}

static int j_value(dctx *d, val *out);

static int j_object(dctx *d, val *out) {
    vlist l = {0};
    j_ws(d);
    if (d->p < d->end && *d->p == '}') {
        d->p++;
    } else {
        for (;;) {
            val k, v;
            j_ws(d);
            if (d->p >= d->end || *d->p != '"') {
                seterr(d->err, d->errlen, "expected a JSON object key");
                return -1;
            }
            d->p++;
            if (j_string(d, &k) < 0) return -1;
            j_ws(d);
            if (d->p >= d->end || *d->p != ':') {
                seterr(d->err, d->errlen, "expected ':'");
                return -1;
            }
            d->p++;
            if (j_value(d, &v) < 0) return -1;
            if (vl_push(d, &l, &k) < 0 || vl_push(d, &l, &v) < 0) return -1;
            j_ws(d);
            if (d->p < d->end && *d->p == ',') { d->p++; continue; }
            if (d->p < d->end && *d->p == '}') { d->p++; break; }
            seterr(d->err, d->errlen, "expected ',' or '}'");
            return -1;
        }
    }
    out->t = V_MAP;
    out->u.a.v = l.v;
    out->u.a.n = l.n / 2;
    if (l.n == 2 && l.v[0].t == V_STR) {
        val *k = &l.v[0], *v = &l.v[1];
        if (val_is_str(k, "$b")) {
            if (v->t != V_STR) { seterr(d->err, d->errlen, "invalid $b value"); return -1; }
            out->t = V_BIN;
            if (b64decode(v->u.s.p, v->u.s.n, d->a, &out->u.s.p, &out->u.s.n) < 0) {
                seterr(d->err, d->errlen, "out of memory");
                return -1;
            }
        } else if (val_is_str(k, "$d")) {
            if (v->t != V_ARR || v->u.a.n != 2) { seterr(d->err, d->errlen, "invalid $d value"); return -1; }
            out->t = V_MAP;
            out->u.a.v = v->u.a.v;
            out->u.a.n = 1;
        } else if (val_is_str(k, "$x")) {
            seterr(d->err, d->errlen, "extension value types ($x) are not supported by this worker");
            return -1;
        }
    }
    return 0;
}

static int j_array(dctx *d, val *out) {
    vlist l = {0};
    j_ws(d);
    if (d->p < d->end && *d->p == ']') {
        d->p++;
    } else {
        for (;;) {
            val v;
            if (j_value(d, &v) < 0) return -1;
            if (vl_push(d, &l, &v) < 0) return -1;
            j_ws(d);
            if (d->p < d->end && *d->p == ',') { d->p++; continue; }
            if (d->p < d->end && *d->p == ']') { d->p++; break; }
            seterr(d->err, d->errlen, "expected ',' or ']'");
            return -1;
        }
    }
    out->t = V_ARR;
    out->u.a.v = l.v;
    out->u.a.n = l.n;
    return 0;
}

static int j_value(dctx *d, val *out) {
    if (++d->depth > MAX_DEPTH) {
        seterr(d->err, d->errlen, "nesting too deep");
        return -1;
    }
    j_ws(d);
    if (d->p >= d->end) {
        seterr(d->err, d->errlen, "unexpected end of JSON");
        return -1;
    }
    int rc;
    uint8_t c = *d->p;
    if (c == '{') { d->p++; rc = j_object(d, out); }
    else if (c == '[') { d->p++; rc = j_array(d, out); }
    else if (c == '"') { d->p++; rc = j_string(d, out); }
    else if (j_lit(d, "true")) { out->t = V_BOOL; out->u.b = 1; rc = 0; }
    else if (j_lit(d, "false")) { out->t = V_BOOL; out->u.b = 0; rc = 0; }
    else if (j_lit(d, "null")) { out->t = V_NIL; rc = 0; }
    else if (j_lit(d, "NaN")) { out->t = V_FLOAT; out->u.d = NAN; rc = 0; }
    else if (c == '-' || (c >= '0' && c <= '9') || c == 'I') rc = j_number(d, out);
    else { seterr(d->err, d->errlen, "invalid JSON"); rc = -1; }
    d->depth--;
    return rc;
}

int decode_msg(int codec, const uint8_t *p, size_t n, arena *a, val *out, char *err, size_t errlen) {
    dctx d = {p, p + n, a, err, errlen, 0};
    if (codec == CODEC_MSGPACK) {
        if (mp_value(&d, out) < 0)
            return -1;
    } else {
        if (j_value(&d, out) < 0)
            return -1;
        j_ws(&d);
    }
    if (d.p != d.end) {
        seterr(err, errlen, "extra data after the message");
        return -1;
    }
    if (out->t != V_MAP) {
        seterr(err, errlen, "frame is not a message");
        return -1;
    }
    return 0;
}

/* -- writer ------------------------------------------------------------------ */

void w_init(wbuf *w, int codec, size_t limit) {
    memset(w, 0, sizeof *w);
    w->codec = codec;
    w->limit = limit;
    w->len = 4;
}

void w_reset(wbuf *w) {
    w->len = 4;
    w->failed = WB_OK;
    w->depth = 0;
}

void w_start(wbuf *w, int codec, size_t limit) {
    w->codec = codec;
    w->limit = limit;
    w_reset(w);
}

void w_free(wbuf *w) {
    free(w->buf);
    w->buf = NULL;
    w->cap = 0;
}

size_t w_payload_len(const wbuf *w) { return w->len - 4; }

wmark w_mark(const wbuf *w) {
    wmark m = {w->len, w->depth, {0}};
    if (w->depth > 0)
        m.top = w->st[w->depth - 1];
    return m;
}

void w_rewind(wbuf *w, wmark m) {
    w->len = m.len;
    w->depth = m.depth;
    if (m.depth > 0)
        w->st[m.depth - 1] = m.top;
    w->failed = WB_OK;
}

static void put(wbuf *w, const void *p, size_t n) {
    if (w->failed)
        return;
    if (w->len - 4 + n > w->limit) {
        w->failed = WB_TOO_LARGE;
        return;
    }
    if (w->len + n > w->cap) {
        size_t cap = w->cap ? w->cap : 4096;
        while (cap < w->len + n)
            cap *= 2;
        uint8_t *nb = realloc(w->buf, cap);
        if (nb == NULL) {
            w->failed = WB_NOMEM;
            return;
        }
        w->buf = nb;
        w->cap = cap;
    }
    memcpy(w->buf + w->len, p, n);
    w->len += n;
}

static void put1(wbuf *w, uint8_t b) {
    if (!w->failed && w->len < w->cap && w->len - 4 < w->limit)
        w->buf[w->len++] = b;
    else
        put(w, &b, 1);
}

static void put_be(wbuf *w, uint8_t tag, uint64_t v, int nbytes) {
    uint8_t b[9];
    b[0] = tag;
    for (int i = 0; i < nbytes; i++)
        b[1 + i] = (uint8_t)(v >> (8 * (nbytes - 1 - i)));
    put(w, b, (size_t)nbytes + 1);
}

void w_finish_frame(wbuf *w) {
    uint32_t n = (uint32_t)(w->len - 4);
    if (w->cap < 4) {
        uint8_t *nb = realloc(w->buf, 4096);
        if (nb == NULL) {
            w->failed = WB_NOMEM;
            return;
        }
        w->buf = nb;
        w->cap = 4096;
    }
    w->buf[0] = (uint8_t)(n >> 24);
    w->buf[1] = (uint8_t)(n >> 16);
    w->buf[2] = (uint8_t)(n >> 8);
    w->buf[3] = (uint8_t)n;
}

/* Separators (JSON) and item counts (both codecs). */
static void pre(wbuf *w) {
    if (w->depth == 0)
        return;
    wlevel *l = &w->st[w->depth - 1];
    if (w->codec == CODEC_JSON) {
        if (l->kind == 'a') {
            if (l->n > 0) put1(w, ',');
        } else if (l->n % 2 == 0) {
            if (l->n > 0) put1(w, ',');
        } else {
            put1(w, ':');
        }
    }
    l->n++;
}

static void push(wbuf *w, char kind, int open, size_t patch) {
    if (w->depth >= W_MAX_DEPTH) {
        w->failed = WB_NOMEM;
        return;
    }
    wlevel *l = &w->st[w->depth++];
    l->kind = kind;
    l->open = (char)open;
    l->n = 0;
    l->patch = patch;
}

void w_nil(wbuf *w) {
    pre(w);
    if (w->codec == CODEC_MSGPACK) put1(w, 0xc0);
    else put(w, "null", 4);
}

void w_bool(wbuf *w, int b) {
    pre(w);
    if (w->codec == CODEC_MSGPACK) put1(w, b ? 0xc3 : 0xc2);
    else if (b) put(w, "true", 4);
    else put(w, "false", 5);
}

void w_int(wbuf *w, int64_t i) {
    pre(w);
    if (w->codec == CODEC_JSON) {
        char b[24];
        int n = snprintf(b, sizeof b, "%lld", (long long)i);
        put(w, b, (size_t)n);
        return;
    }
    if (i >= 0) {
        uint64_t u = (uint64_t)i;
        if (u < 128) put1(w, (uint8_t)u);
        else if (u <= 0xff) put_be(w, 0xcc, u, 1);
        else if (u <= 0xffff) put_be(w, 0xcd, u, 2);
        else if (u <= 0xffffffffu) put_be(w, 0xce, u, 4);
        else put_be(w, 0xcf, u, 8);
    } else {
        if (i >= -32) put1(w, (uint8_t)(int8_t)i);
        else if (i >= INT8_MIN) put_be(w, 0xd0, (uint64_t)i & 0xff, 1);
        else if (i >= INT16_MIN) put_be(w, 0xd1, (uint64_t)i & 0xffff, 2);
        else if (i >= INT32_MIN) put_be(w, 0xd2, (uint64_t)i & 0xffffffffu, 4);
        else put_be(w, 0xd3, (uint64_t)i, 8);
    }
}

void w_double(wbuf *w, double d) {
    pre(w);
    if (w->codec == CODEC_MSGPACK) {
        uint64_t u;
        memcpy(&u, &d, 8);
        put_be(w, 0xcb, u, 8);
        return;
    }
    if (isnan(d)) { put(w, "NaN", 3); return; }
    if (isinf(d)) {
        if (d > 0) put(w, "Infinity", 8);
        else put(w, "-Infinity", 9);
        return;
    }
    /* Shortest text that reads back as the same double, and always with a
     * '.' or exponent so that Python decodes a float, not an int. */
    char b[40];
    int n = 0;
    for (int prec = 15; prec <= 17; prec++) {
        n = snprintf(b, sizeof b - 3, "%.*g", prec, d);
        if (strtod(b, NULL) == d)
            break;
    }
    if (strpbrk(b, ".eE") == NULL) {
        b[n++] = '.';
        b[n++] = '0';
        b[n] = 0;
    }
    put(w, b, (size_t)n);
}

static void json_str(wbuf *w, const char *p, size_t n) {
    put1(w, '"');
    size_t start = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)p[i];
        if (c >= 0x20 && c != '"' && c != '\\')
            continue;
        put(w, p + start, i - start);
        start = i + 1;
        char esc[8];
        switch (c) {
        case '"': put(w, "\\\"", 2); break;
        case '\\': put(w, "\\\\", 2); break;
        case '\n': put(w, "\\n", 2); break;
        case '\r': put(w, "\\r", 2); break;
        case '\t': put(w, "\\t", 2); break;
        case '\b': put(w, "\\b", 2); break;
        case '\f': put(w, "\\f", 2); break;
        default:
            snprintf(esc, sizeof esc, "\\u%04x", c);
            put(w, esc, 6);
        }
    }
    put(w, p + start, n - start);
    put1(w, '"');
}

void w_str(wbuf *w, const char *p, size_t n) {
    pre(w);
    if (w->codec == CODEC_JSON) {
        json_str(w, p, n);
        return;
    }
    if (n < 32) put1(w, (uint8_t)(0xa0 | n));
    else if (n <= 0xff) put_be(w, 0xd9, n, 1);
    else if (n <= 0xffff) put_be(w, 0xda, n, 2);
    else put_be(w, 0xdb, n, 4);
    put(w, p, n);
}

void w_cstr(wbuf *w, const char *s) { w_str(w, s ? s : "", s ? strlen(s) : 0); }

void w_str_lossy(wbuf *w, const char *p, size_t n) {
    if (utf8_check((const unsigned char *)p, n, NULL)) {
        w_str(w, p, n);
        return;
    }
    char *s = utf8_lossy(p, n);
    if (s == NULL) {
        w->failed = WB_NOMEM;
        return;
    }
    w_str(w, s, strlen(s));
    free(s);
}

void w_bin(wbuf *w, const void *p, size_t n) {
    pre(w);
    if (w->codec == CODEC_MSGPACK) {
        if (n <= 0xff) put_be(w, 0xc4, n, 1);
        else if (n <= 0xffff) put_be(w, 0xc5, n, 2);
        else put_be(w, 0xc6, n, 4);
        put(w, p, n);
        return;
    }
    /* {"$b": "<base64>"} */
    put(w, "{\"$b\":\"", 7);
    const unsigned char *s = p;
    char q[4];
    size_t i = 0;
    for (; i + 3 <= n; i += 3) {
        if (w->failed) return;
        uint32_t v = (uint32_t)s[i] << 16 | (uint32_t)s[i + 1] << 8 | s[i + 2];
        q[0] = B64[v >> 18]; q[1] = B64[(v >> 12) & 63]; q[2] = B64[(v >> 6) & 63]; q[3] = B64[v & 63];
        put(w, q, 4);
    }
    if (n - i == 1) {
        uint32_t v = (uint32_t)s[i] << 16;
        q[0] = B64[v >> 18]; q[1] = B64[(v >> 12) & 63]; q[2] = '='; q[3] = '=';
        put(w, q, 4);
    } else if (n - i == 2) {
        uint32_t v = (uint32_t)s[i] << 16 | (uint32_t)s[i + 1] << 8;
        q[0] = B64[v >> 18]; q[1] = B64[(v >> 12) & 63]; q[2] = B64[(v >> 6) & 63]; q[3] = '=';
        put(w, q, 4);
    }
    put(w, "\"}", 2);
}

void w_array(wbuf *w, uint32_t n) {
    pre(w);
    if (w->codec == CODEC_JSON) put1(w, '[');
    else if (n < 16) put1(w, (uint8_t)(0x90 | n));
    else if (n <= 0xffff) put_be(w, 0xdc, n, 2);
    else put_be(w, 0xdd, n, 4);
    push(w, 'a', 0, 0);
}

void w_map(wbuf *w, uint32_t pairs) {
    pre(w);
    if (w->codec == CODEC_JSON) put1(w, '{');
    else if (pairs < 16) put1(w, (uint8_t)(0x80 | pairs));
    else if (pairs <= 0xffff) put_be(w, 0xde, pairs, 2);
    else put_be(w, 0xdf, pairs, 4);
    push(w, 'm', 0, 0);
}

void w_array_open(wbuf *w) {
    pre(w);
    if (w->codec == CODEC_JSON) {
        put1(w, '[');
        push(w, 'a', 0, 0);
    } else {
        put_be(w, 0xdd, 0, 4);
        push(w, 'a', 1, w->len - 4);
    }
}

void w_map_open(wbuf *w) {
    pre(w);
    if (w->codec == CODEC_JSON) {
        put1(w, '{');
        push(w, 'm', 0, 0);
    } else {
        put_be(w, 0xdf, 0, 4);
        push(w, 'm', 1, w->len - 4);
    }
}

void w_end(wbuf *w) {
    if (w->depth == 0)
        return;
    wlevel *l = &w->st[--w->depth];
    if (w->codec == CODEC_JSON) {
        put1(w, l->kind == 'a' ? ']' : '}');
    } else if (l->open && !w->failed) {
        uint32_t n = l->kind == 'm' ? l->n / 2 : l->n;
        uint8_t *p = w->buf + l->patch;
        p[0] = (uint8_t)(n >> 24);
        p[1] = (uint8_t)(n >> 16);
        p[2] = (uint8_t)(n >> 8);
        p[3] = (uint8_t)n;
    }
}

void w_raw(wbuf *w, const void *p, size_t n) {
    pre(w);
    put(w, p, n);
}

void w_val(wbuf *w, const val *v) {
    switch (v->t) {
    case V_NIL: w_nil(w); break;
    case V_BOOL: w_bool(w, v->u.b); break;
    case V_INT: w_int(w, v->u.i); break;
    case V_BIGINT:
        if (w->codec == CODEC_JSON) w_raw(w, v->u.s.p, v->u.s.n);
        else w_str(w, v->u.s.p, v->u.s.n);
        break;
    case V_FLOAT: w_double(w, v->u.d); break;
    case V_STR: w_str_lossy(w, v->u.s.p, v->u.s.n); break;
    case V_BIN: w_bin(w, v->u.s.p, v->u.s.n); break;
    case V_ARR:
        w_array(w, (uint32_t)v->u.a.n);
        for (size_t i = 0; i < v->u.a.n; i++) w_val(w, &v->u.a.v[i]);
        w_end(w);
        break;
    case V_MAP:
        w_map(w, (uint32_t)v->u.a.n);
        for (size_t i = 0; i < 2 * v->u.a.n; i++) w_val(w, &v->u.a.v[i]);
        w_end(w);
        break;
    }
}
