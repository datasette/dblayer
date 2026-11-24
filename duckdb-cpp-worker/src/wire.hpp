// Framing and value writing on top of the C worker's codec
// (cworker/src/codec.c): length-prefixed frames on stdout, shared by every
// connection thread, and the extension value types of
// dblayer/worker/protocol.py (STANDARD_VALUE_TYPES).
#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

extern "C" {
#include "codec.h"
}

namespace dbw {

// Ext codes of protocol.STANDARD_VALUE_TYPES.
enum ExtCode {
    EXT_BIGINT = 1,  // integers outside 64 bits (msgpack only), decimal text
    EXT_DATETIME = 16,
    EXT_DATE = 17,
    EXT_TIME = 18,
    EXT_DECIMAL = 19,
    EXT_UUID = 20,
    EXT_TIMEDELTA = 21,
    EXT_MAP = 22,
    EXT_TUPLE = 23,
};

const char *ext_tag(int code);

// The codec agreed in hello (JSON until then) and the frame size cap.
struct WireConfig {
    int codec = CODEC_JSON;
    size_t max_frame_bytes = 256u * 1024 * 1024;
};
extern WireConfig g_wire;

// One writer per thread: a wbuf plus scratch buffers for msgpack ext
// payloads (which need their length up front), one per nesting depth.
class Writer {
  public:
    Writer();
    ~Writer();
    Writer(const Writer &) = delete;
    Writer &operator=(const Writer &) = delete;

    // Where the next value goes: the frame, or the payload of the
    // innermost open ext.
    wbuf *w() { return cur(); }
    // Start a new frame in the agreed codec (or JSON for hello).
    void start(int codec = -1);
    // Finish the frame and write it to stdout (serialised with every
    // other thread). A frame over the cap becomes a ResourceLimitExceeded
    // error for `id` (if the reply has one). Returns false if stdout is gone.
    bool send(int64_t id, bool has_id);

    // Values beyond the plain msgpack/JSON set.
    void uint64(uint64_t v);
    void big_decimal_text(const std::string &digits);  // an int outside 64 bits
    void ext_string(int code, const char *s, size_t n);  // ext whose to_wire is a str
    // An ext whose payload is a structured value: write the payload with
    // the normal calls on the returned wbuf between ext_begin and ext_end.
    wbuf *ext_begin(int code);
    void ext_end(int code);

  private:
    wbuf main_;
    std::vector<wbuf *> scratch_;  // msgpack ext payloads, by depth
    std::vector<wbuf *> stack_;    // current target per ext depth
    wbuf *cur() { return stack_.empty() ? &main_ : stack_.back(); }
};

// Writes a whole frame (header included) to stdout under the output lock.
bool write_frame(const uint8_t *p, size_t n);

}  // namespace dbw
