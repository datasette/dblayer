/* Wire codecs for the dblayer worker protocol (docs/worker-protocol.md):
 * a decoded value tree, a MessagePack and a JSON decoder, and a streaming
 * writer that emits either codec. */
#ifndef DBLAYER_CODEC_H
#define DBLAYER_CODEC_H

#include <stddef.h>
#include <stdint.h>

enum { CODEC_JSON = 0, CODEC_MSGPACK = 1 };

typedef enum {
    V_NIL, V_BOOL, V_INT, V_BIGINT, V_FLOAT, V_STR, V_BIN, V_ARR, V_MAP,
    V_EXT /* a registered value type (protocol.STANDARD_VALUE_TYPES): ext code + payload */
} vtype;

typedef struct val val;
struct val {
    vtype t;
    union {
        int b;
        int64_t i;
        double d;
        struct { const char *p; size_t n; } s;   /* V_STR, V_BIN, V_BIGINT (decimal text) */
        struct { val *v; size_t n; } a;          /* V_ARR: n items; V_MAP: n pairs, v has 2n */
        struct { int code; val *inner; } x;      /* V_EXT: code 16-127, inner = to_wire(value) */
    } u;
};

/* A bump allocator: every node of one decoded message lives in one arena,
 * freed in one go when the request is done. */
typedef struct ablock ablock;
typedef struct { ablock *head; } arena;
void *arena_alloc(arena *a, size_t n);
void arena_free(arena *a);

/* Decode one payload. Strings may point into `p`, which must outlive the
 * tree. Returns 0, or -1 with a message in err. */
int decode_msg(int codec, const uint8_t *p, size_t n, arena *a, val *out, char *err, size_t errlen);

const val *map_get(const val *m, const char *key);
int val_is_str(const val *v, const char *s);
/* Integer or bool value of an optional field; `dflt` when absent or nil. */
int64_t get_int(const val *m, const char *key, int64_t dflt, int *present);
/* The ext code of a value type tag ("datetime" -> 16 ...), -1 if unknown. */
int value_type_code(const char *tag, size_t n);

/* -- writer --------------------------------------------------------------- */

enum { WB_OK = 0, WB_NOMEM = 1, WB_TOO_LARGE = 2 };

typedef struct {
    char kind;      /* 'a' or 'm' */
    char open;      /* count patched at the end (msgpack) */
    uint32_t n;     /* items written (keys and values both count in a map) */
    size_t patch;   /* offset of the 4-byte count to patch */
} wlevel;

#define W_MAX_DEPTH 32

typedef struct {
    uint8_t *buf;
    size_t len, cap;
    size_t limit;   /* frame payload cap: max_frame_bytes */
    int codec;
    int failed;     /* WB_NOMEM or WB_TOO_LARGE */
    int depth;
    wlevel st[W_MAX_DEPTH];
} wbuf;

void w_init(wbuf *w, int codec, size_t limit);  /* reserves the 4-byte frame header */
void w_reset(wbuf *w);                           /* empty again, header reserved */
void w_start(wbuf *w, int codec, size_t limit);  /* w_reset, keeping the buffer */
void w_free(wbuf *w);
size_t w_payload_len(const wbuf *w);
void w_finish_frame(wbuf *w);                    /* writes the length header */

typedef struct { size_t len; int depth; wlevel top; } wmark;
wmark w_mark(const wbuf *w);
void w_rewind(wbuf *w, wmark m);

void w_nil(wbuf *w);
void w_bool(wbuf *w, int b);
void w_int(wbuf *w, int64_t i);
void w_double(wbuf *w, double d);
void w_str(wbuf *w, const char *p, size_t n);       /* p must be valid UTF-8 */
void w_cstr(wbuf *w, const char *s);
void w_str_lossy(wbuf *w, const char *p, size_t n); /* invalid UTF-8 -> U+FFFD */
void w_bin(wbuf *w, const void *p, size_t n);
void w_array(wbuf *w, uint32_t n);
void w_map(wbuf *w, uint32_t pairs);
void w_array_open(wbuf *w);
void w_map_open(wbuf *w);
void w_end(wbuf *w);
void w_raw(wbuf *w, const void *p, size_t n);       /* splice pre-encoded bytes */
void w_val(wbuf *w, const val *v);                  /* re-encode a decoded value */

/* UTF-8: 1 if valid; *chars gets the number of code points. */
int utf8_check(const unsigned char *p, size_t n, size_t *chars);
/* Copy with invalid sequences replaced by U+FFFD (malloc'd, NUL-terminated). */
char *utf8_lossy(const char *p, size_t n);

#endif
