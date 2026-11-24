#include "wire.hpp"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>

namespace dbw {

WireConfig g_wire;

namespace {
std::mutex g_out_mu;
int g_out_fd = 1;
const size_t PAD = 6;  // room for the largest msgpack ext header (ext32)
}  // namespace

void set_output_fd(int fd) { g_out_fd = fd; }

const char *ext_tag(int code) {
    switch (code) {
    case EXT_DATETIME: return "datetime";
    case EXT_DATE: return "date";
    case EXT_TIME: return "time";
    case EXT_DECIMAL: return "decimal";
    case EXT_UUID: return "uuid";
    case EXT_TIMEDELTA: return "timedelta";
    case EXT_MAP: return "map";
    case EXT_TUPLE: return "tuple";
    default: return "?";
    }
}

bool write_frame(const uint8_t *p, size_t n) {
    std::lock_guard<std::mutex> lock(g_out_mu);
    while (n > 0) {
        ssize_t k = ::write(g_out_fd, p, n);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        p += k;
        n -= (size_t)k;
    }
    return true;
}

Writer::Writer() { w_init(&main_, CODEC_JSON, g_wire.max_frame_bytes); }

Writer::~Writer() {
    w_free(&main_);
    for (wbuf *s : scratch_) {
        w_free(s);
        delete s;
    }
}

void Writer::start(int codec) {
    stack_.clear();
    w_start(&main_, codec < 0 ? g_wire.codec : codec, g_wire.max_frame_bytes);
}

bool Writer::send(int64_t id, bool has_id) {
    if (main_.failed) {
        if (!has_id)
            return true;  // a notice that cannot be sent: drop it
        int failed = main_.failed;
        size_t len = w_payload_len(&main_);
        start(main_.codec);
        wbuf *w = &main_;
        w_map(w, 2);
        w_cstr(w, "id");
        w_int(w, id);
        w_cstr(w, "error");
        w_map(w, 2);
        w_cstr(w, "type");
        w_cstr(w, "ResourceLimitExceeded");
        w_cstr(w, "message");
        char msg[160];
        if (failed == WB_TOO_LARGE)
            snprintf(msg, sizeof msg, "reply too large: frame of more than %zu bytes exceeds the %zu-byte frame size cap",
                     len, g_wire.max_frame_bytes);
        else
            snprintf(msg, sizeof msg, "worker ran out of memory encoding the reply");
        w_cstr(w, msg);
        w_end(w);
        w_end(w);
    }
    w_finish_frame(&main_);
    return write_frame(main_.buf, main_.len);
}

void Writer::uint64(uint64_t v) {
    wbuf *w = cur();
    if (v <= (uint64_t)INT64_MAX) {
        w_int(w, (int64_t)v);
        return;
    }
    if (w->codec == CODEC_MSGPACK) {
        uint8_t b[9];
        b[0] = 0xcf;
        for (int i = 0; i < 8; i++)
            b[1 + i] = (uint8_t)(v >> (8 * (7 - i)));
        w_raw(w, b, 9);
    } else {
        char t[24];
        int n = snprintf(t, sizeof t, "%llu", (unsigned long long)v);
        w_raw(w, t, (size_t)n);
    }
}

static size_t ext_header(uint8_t *h, int code, size_t n) {
    if (n < 256) {
        h[0] = 0xc7;
        h[1] = (uint8_t)n;
        h[2] = (uint8_t)code;
        return 3;
    }
    if (n < 65536) {
        h[0] = 0xc8;
        h[1] = (uint8_t)(n >> 8);
        h[2] = (uint8_t)n;
        h[3] = (uint8_t)code;
        return 4;
    }
    h[0] = 0xc9;
    h[1] = (uint8_t)(n >> 24);
    h[2] = (uint8_t)(n >> 16);
    h[3] = (uint8_t)(n >> 8);
    h[4] = (uint8_t)n;
    h[5] = (uint8_t)code;
    return 6;
}

void Writer::big_decimal_text(const std::string &digits) {
    wbuf *w = cur();
    if (w->codec == CODEC_JSON) {
        w_raw(w, digits.data(), digits.size());
        return;
    }
    std::string b(PAD, '\0');
    uint8_t h[PAD];
    size_t hn = ext_header(h, EXT_BIGINT, digits.size());
    b.assign((const char *)h, hn);
    b += digits;
    w_raw(w, b.data(), b.size());
}

void Writer::ext_string(int code, const char *s, size_t n) {
    wbuf *w = cur();
    if (w->codec == CODEC_JSON) {
        w_map(w, 1);
        w_cstr(w, "$x");
        w_array(w, 2);
        w_cstr(w, ext_tag(code));
        w_str(w, s, n);
        w_end(w);
        w_end(w);
        return;
    }
    // ext header, then the payload: a msgpack str holding `s`.
    uint8_t b[96];
    size_t sh;
    uint8_t strh[5];
    if (n < 32) {
        strh[0] = (uint8_t)(0xa0 | n);
        sh = 1;
    } else if (n < 256) {
        strh[0] = 0xd9;
        strh[1] = (uint8_t)n;
        sh = 2;
    } else if (n < 65536) {
        strh[0] = 0xda;
        strh[1] = (uint8_t)(n >> 8);
        strh[2] = (uint8_t)n;
        sh = 3;
    } else {
        strh[0] = 0xdb;
        strh[1] = (uint8_t)(n >> 24);
        strh[2] = (uint8_t)(n >> 16);
        strh[3] = (uint8_t)(n >> 8);
        strh[4] = (uint8_t)n;
        sh = 5;
    }
    size_t hn = ext_header(b, code, sh + n);
    if (hn + sh + n <= sizeof b) {
        memcpy(b + hn, strh, sh);
        memcpy(b + hn + sh, s, n);
        w_raw(w, b, hn + sh + n);
        return;
    }
    std::string big((const char *)b, hn);
    big.append((const char *)strh, sh);
    big.append(s, n);
    w_raw(w, big.data(), big.size());
}

wbuf *Writer::ext_begin(int code) {
    wbuf *parent = cur();
    if (parent->codec == CODEC_JSON) {
        w_map(parent, 1);
        w_cstr(parent, "$x");
        w_array(parent, 2);
        w_cstr(parent, ext_tag(code));
        stack_.push_back(parent);
        return parent;
    }
    size_t depth = stack_.size();
    while (scratch_.size() <= depth) {
        wbuf *s = new wbuf;
        w_init(s, CODEC_MSGPACK, g_wire.max_frame_bytes);
        scratch_.push_back(s);
    }
    wbuf *s = scratch_[depth];
    w_start(s, CODEC_MSGPACK, g_wire.max_frame_bytes);
    static const uint8_t zeros[PAD] = {0};
    w_raw(s, zeros, PAD);  // depth 0: no separator, no count
    stack_.push_back(s);
    return s;
}

void Writer::ext_end(int code) {
    wbuf *s = stack_.back();
    stack_.pop_back();
    wbuf *parent = cur();
    if (parent == s) {  // JSON: the payload went straight into the frame
        w_end(parent);
        w_end(parent);
        return;
    }
    if (s->failed) {
        if (!parent->failed)
            parent->failed = s->failed;
        return;
    }
    size_t n = s->len - 4 - PAD;
    uint8_t h[PAD];
    size_t hn = ext_header(h, code, n);
    uint8_t *start = s->buf + 4 + PAD - hn;
    memcpy(start, h, hn);
    w_raw(parent, start, hn + n);
}

}  // namespace dbw
