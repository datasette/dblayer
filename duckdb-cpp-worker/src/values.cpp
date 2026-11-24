// Values across the wire, as the Python DuckDB worker sends them: DuckDB
// result vectors become exactly what the duckdb Python package returns
// (then dblayer.worker.protocol's value types and markers), and request
// parameters are bound as the package binds Python values.
#include "worker.hpp"

#include <cmath>
#include <cstring>

namespace dbw {

typedef __int128 i128;
typedef unsigned __int128 u128;

namespace {

const int64_t MICROS_PER_DAY = 86400LL * 1000000LL;

// -- dates (Howard Hinnant's civil calendar algorithms) -----------------------------

int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468;
}

void civil_from_days(int64_t z, int64_t &y, unsigned &m, unsigned &d) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = (unsigned)(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = (int64_t)yoe + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += m <= 2;
}

int64_t floordiv(int64_t a, int64_t b) {
    int64_t q = a / b;
    if ((a % b != 0) && ((a < 0) != (b < 0))) q--;
    return q;
}

Error value_error(const std::string &msg) {
    Error e("ValueError", msg);
    e.builtin = true;
    return e;
}

// "YYYY-MM-DD"
size_t fmt_date(char *b, int64_t days) {
    int64_t y;
    unsigned m, d;
    civil_from_days(days, y, m, d);
    if (y < 1 || y > 9999) throw value_error("year " + std::to_string(y) + " is out of range");
    return (size_t)snprintf(b, 16, "%04d-%02u-%02u", (int)y, m, d);
}

// "HH:MM:SS[.ffffff]" (Python isoformat leaves out a zero fraction)
size_t fmt_time(char *b, int64_t micros) {
    int64_t s = micros / 1000000, us = micros % 1000000;
    int h = (int)(s / 3600), mi = (int)(s / 60 % 60), se = (int)(s % 60);
    if (h > 23) throw value_error("hour must be in 0..23");
    if (us) return (size_t)snprintf(b, 24, "%02d:%02d:%02d.%06d", h, mi, se, (int)us);
    return (size_t)snprintf(b, 24, "%02d:%02d:%02d", h, mi, se);
}

size_t fmt_offset(char *b, int32_t offset_s) {
    char sign = offset_s < 0 ? '-' : '+';
    int a = offset_s < 0 ? -offset_s : offset_s;
    if (a % 60) return (size_t)snprintf(b, 16, "%c%02d:%02d:%02d", sign, a / 3600, a / 60 % 60, a % 60);
    return (size_t)snprintf(b, 16, "%c%02d:%02d", sign, a / 3600, a / 60 % 60);
}

// "YYYY-MM-DDTHH:MM:SS[.ffffff]"
size_t fmt_timestamp(char *b, int64_t micros) {
    int64_t days = floordiv(micros, MICROS_PER_DAY);
    int64_t rest = micros - days * MICROS_PER_DAY;
    size_t n = fmt_date(b, days);
    b[n++] = 'T';
    n += fmt_time(b + n, rest);
    return n;
}

const int64_t TS_INFINITY = INT64_MAX, TS_NINFINITY = -INT64_MAX;
const char *TS_MAX = "9999-12-31T23:59:59.999999";
const char *TS_MIN = "0001-01-01T00:00:00";

// -- big integers ---------------------------------------------------------------------

std::string u128_text(u128 v) {
    if (v == 0) return "0";
    char b[48];
    int i = 47;
    b[i] = 0;
    while (v > 0) {
        b[--i] = (char)('0' + (int)(v % 10));
        v /= 10;
    }
    return std::string(b + i);
}

std::string i128_text(i128 v) {
    if (v < 0) return "-" + u128_text((u128)0 - (u128)v);
    return u128_text((u128)v);
}

void write_i128(Writer &wr, i128 v) {
    if (v >= INT64_MIN && v <= INT64_MAX) w_int(wr.w(), (int64_t)v);
    else if (v > 0 && (u128)v <= UINT64_MAX) wr.uint64((uint64_t)v);
    else wr.big_decimal_text(i128_text(v));
}

void write_u128(Writer &wr, u128 v) {
    if (v <= UINT64_MAX) wr.uint64((uint64_t)v);
    else wr.big_decimal_text(u128_text(v));
}

std::string decimal_text(i128 v, int scale) {
    bool neg = v < 0;
    std::string digits = u128_text(neg ? (u128)0 - (u128)v : (u128)v);
    if (scale > 0) {
        if ((int)digits.size() <= scale) digits.insert(0, (size_t)(scale - (int)digits.size() + 1), '0');
        digits.insert(digits.size() - (size_t)scale, 1, '.');
    }
    return neg ? "-" + digits : digits;
}

// BIGNUM storage: a 3-byte header (sign bit set for positive numbers),
// then the big-endian magnitude; negative numbers are stored inverted.
std::string bignum_text(const uint8_t *p, size_t n) {
    if (n < 3) return "0";
    bool neg = (p[0] & 0x80) == 0;
    std::vector<uint8_t> mag(p + 3, p + n);
    if (neg)
        for (auto &b : mag) b = (uint8_t)~b;
    std::string out;
    // Repeated division by 10 of the big-endian byte string.
    size_t start = 0;
    while (start < mag.size() && mag[start] == 0) start++;
    if (start == mag.size()) return "0";
    while (start < mag.size()) {
        unsigned rem = 0;
        for (size_t i = start; i < mag.size(); i++) {
            unsigned cur = (rem << 8) | mag[i];
            mag[i] = (uint8_t)(cur / 10);
            rem = cur % 10;
        }
        out += (char)('0' + rem);
        while (start < mag.size() && mag[start] == 0) start++;
    }
    if (neg) out += '-';
    return std::string(out.rbegin(), out.rend());
}

std::string bit_text(const uint8_t *p, size_t n) {
    if (n < 1) return "";
    unsigned padding = p[0];
    std::string out;
    out.reserve((n - 1) * 8);
    for (size_t i = 1; i < n; i++)
        for (int bit = 7; bit >= 0; bit--) out += (p[i] >> bit) & 1 ? '1' : '0';
    return padding <= out.size() ? out.substr(padding) : std::string();
}

size_t utf8_chars(const char *p, size_t n) {
    size_t c = 0;
    for (size_t i = 0; i < n; i++)
        if (((unsigned char)p[i] & 0xC0) != 0x80) c++;
    return c;
}

struct TypeHandle {
    duckdb_logical_type t;
    explicit TypeHandle(duckdb_logical_type x) : t(x) {}
    ~TypeHandle() { duckdb_destroy_logical_type(&t); }
};

std::string take(char *s) {
    std::string out = s ? s : "";
    if (s) duckdb_free(s);
    return out;
}

inline bool valid_row(uint64_t *validity, idx_t row) {
    return validity == nullptr || (validity[row / 64] >> (row % 64)) & 1;
}

}  // namespace

// -- result columns ------------------------------------------------------------------------

struct ColumnCodec {
    duckdb_type id = DUCKDB_TYPE_INVALID;
    int scale = 0;
    duckdb_type internal = DUCKDB_TYPE_INVALID;  // DECIMAL, ENUM storage
    std::vector<std::string> dict;               // ENUM
    std::vector<std::string> names;              // STRUCT fields, UNION members
    std::vector<std::unique_ptr<ColumnCodec>> children;  // LIST/ARRAY: [0]; STRUCT/UNION; MAP: key, value
    idx_t array_size = 0;
    bool map_by_columns = false;  // MAP with nested keys: Python's {"key": [...], "value": [...]}
    bool key_is_str = false;      // MAP keys are Python str
    bool str_output = false;      // Python returns a str
    const std::string *zone = nullptr;

    explicit ColumnCodec(duckdb_logical_type t, const std::string *z) : zone(z) {
        id = duckdb_get_type_id(t);
        switch (id) {
        case DUCKDB_TYPE_DECIMAL:
            scale = duckdb_decimal_scale(t);
            internal = duckdb_decimal_internal_type(t);
            break;
        case DUCKDB_TYPE_ENUM: {
            internal = duckdb_enum_internal_type(t);
            uint32_t n = duckdb_enum_dictionary_size(t);
            for (uint32_t i = 0; i < n; i++) dict.push_back(take(duckdb_enum_dictionary_value(t, i)));
            str_output = true;
            break;
        }
        case DUCKDB_TYPE_LIST: {
            TypeHandle c(duckdb_list_type_child_type(t));
            children.emplace_back(new ColumnCodec(c.t, z));
            break;
        }
        case DUCKDB_TYPE_ARRAY: {
            TypeHandle c(duckdb_array_type_child_type(t));
            array_size = duckdb_array_type_array_size(t);
            children.emplace_back(new ColumnCodec(c.t, z));
            break;
        }
        case DUCKDB_TYPE_STRUCT:
            for (idx_t i = 0, n = duckdb_struct_type_child_count(t); i < n; i++) {
                TypeHandle c(duckdb_struct_type_child_type(t, i));
                names.push_back(take(duckdb_struct_type_child_name(t, i)));
                children.emplace_back(new ColumnCodec(c.t, z));
            }
            break;
        case DUCKDB_TYPE_UNION:
            for (idx_t i = 0, n = duckdb_union_type_member_count(t); i < n; i++) {
                TypeHandle c(duckdb_union_type_member_type(t, i));
                names.push_back(take(duckdb_union_type_member_name(t, i)));
                children.emplace_back(new ColumnCodec(c.t, z));
            }
            break;
        case DUCKDB_TYPE_MAP: {
            TypeHandle k(duckdb_map_type_key_type(t)), v(duckdb_map_type_value_type(t));
            children.emplace_back(new ColumnCodec(k.t, z));
            children.emplace_back(new ColumnCodec(v.t, z));
            duckdb_type kid = children[0]->id;
            map_by_columns = kid == DUCKDB_TYPE_LIST || kid == DUCKDB_TYPE_ARRAY || kid == DUCKDB_TYPE_STRUCT ||
                             kid == DUCKDB_TYPE_MAP || kid == DUCKDB_TYPE_UNION;
            key_is_str = children[0]->str_output;
            break;
        }
        case DUCKDB_TYPE_VARCHAR:
        case DUCKDB_TYPE_BIT:
        case DUCKDB_TYPE_BIGNUM:
            str_output = true;
            break;
        default:
            break;
        }
    }

    void write(Writer &wr, duckdb_vector v, idx_t row) const {
        write_at(wr, v, duckdb_vector_get_data(v), duckdb_vector_get_validity(v), row);
    }

    void write_at(Writer &wr, duckdb_vector v, void *data, uint64_t *validity, idx_t row) const {
        wbuf *w = wr.w();
        if (!valid_row(validity, row)) {
            w_nil(w);
            return;
        }
        char b[96];
        switch (id) {
        case DUCKDB_TYPE_BOOLEAN: w_bool(w, ((bool *)data)[row]); return;
        case DUCKDB_TYPE_TINYINT: w_int(w, ((int8_t *)data)[row]); return;
        case DUCKDB_TYPE_SMALLINT: w_int(w, ((int16_t *)data)[row]); return;
        case DUCKDB_TYPE_INTEGER: w_int(w, ((int32_t *)data)[row]); return;
        case DUCKDB_TYPE_BIGINT: w_int(w, ((int64_t *)data)[row]); return;
        case DUCKDB_TYPE_UTINYINT: w_int(w, ((uint8_t *)data)[row]); return;
        case DUCKDB_TYPE_USMALLINT: w_int(w, ((uint16_t *)data)[row]); return;
        case DUCKDB_TYPE_UINTEGER: w_int(w, ((uint32_t *)data)[row]); return;
        case DUCKDB_TYPE_UBIGINT: wr.uint64(((uint64_t *)data)[row]); return;
        case DUCKDB_TYPE_HUGEINT: {
            duckdb_hugeint h = ((duckdb_hugeint *)data)[row];
            write_i128(wr, (i128)(((u128)(uint64_t)h.upper << 64) | h.lower));
            return;
        }
        case DUCKDB_TYPE_UHUGEINT: {
            duckdb_uhugeint h = ((duckdb_uhugeint *)data)[row];
            write_u128(wr, ((u128)h.upper << 64) | h.lower);
            return;
        }
        case DUCKDB_TYPE_FLOAT: w_double(w, (double)((float *)data)[row]); return;
        case DUCKDB_TYPE_DOUBLE: w_double(w, ((double *)data)[row]); return;
        case DUCKDB_TYPE_VARCHAR: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            w_str(w, duckdb_string_t_data(s), duckdb_string_t_length(*s));
            return;
        }
        case DUCKDB_TYPE_BLOB: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            w_bin(w, duckdb_string_t_data(s), duckdb_string_t_length(*s));
            return;
        }
        case DUCKDB_TYPE_BIT: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            std::string t = bit_text((const uint8_t *)duckdb_string_t_data(s), duckdb_string_t_length(*s));
            w_str(w, t.data(), t.size());
            return;
        }
        case DUCKDB_TYPE_BIGNUM: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            std::string t = bignum_text((const uint8_t *)duckdb_string_t_data(s), duckdb_string_t_length(*s));
            w_str(w, t.data(), t.size());
            return;
        }
        case DUCKDB_TYPE_ENUM: {
            uint32_t ix;
            if (internal == DUCKDB_TYPE_UTINYINT) ix = ((uint8_t *)data)[row];
            else if (internal == DUCKDB_TYPE_USMALLINT) ix = ((uint16_t *)data)[row];
            else ix = ((uint32_t *)data)[row];
            const std::string &s = ix < dict.size() ? dict[ix] : dict.at(0);
            w_str(w, s.data(), s.size());
            return;
        }
        case DUCKDB_TYPE_UUID: {
            duckdb_hugeint h = ((duckdb_hugeint *)data)[row];
            uint64_t hi = (uint64_t)h.upper ^ (1ULL << 63), lo = h.lower;
            int n = snprintf(b, sizeof b, "%08x-%04x-%04x-%04x-%012llx", (unsigned)(hi >> 32), (unsigned)(hi >> 16) & 0xffff,
                             (unsigned)hi & 0xffff, (unsigned)(lo >> 48), (unsigned long long)(lo & 0xffffffffffffULL));
            wr.ext_string(EXT_UUID, b, (size_t)n);
            return;
        }
        case DUCKDB_TYPE_DATE: {
            int32_t d = ((int32_t *)data)[row];
            if (d == INT32_MAX) wr.ext_string(EXT_DATE, "9999-12-31", 10);
            else if (d == -INT32_MAX) wr.ext_string(EXT_DATE, "0001-01-01", 10);
            else wr.ext_string(EXT_DATE, b, fmt_date(b, d));
            return;
        }
        case DUCKDB_TYPE_TIME: wr.ext_string(EXT_TIME, b, fmt_time(b, ((int64_t *)data)[row])); return;
        case DUCKDB_TYPE_TIME_NS: wr.ext_string(EXT_TIME, b, fmt_time(b, ((int64_t *)data)[row] / 1000)); return;
        case DUCKDB_TYPE_TIME_TZ: {
            duckdb_time_tz_struct t = duckdb_from_time_tz(((duckdb_time_tz *)data)[row]);
            int64_t micros = ((int64_t)t.time.hour * 3600 + t.time.min * 60 + t.time.sec) * 1000000 + t.time.micros;
            size_t n = fmt_time(b, micros);
            n += fmt_offset(b + n, t.offset);
            wr.ext_string(EXT_TIME, b, n);
            return;
        }
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_S:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP_NS: {
            int64_t ts = ((int64_t *)data)[row];
            if (ts == TS_INFINITY) wr.ext_string(EXT_DATETIME, TS_MAX, strlen(TS_MAX));
            else if (ts == TS_NINFINITY) wr.ext_string(EXT_DATETIME, TS_MIN, strlen(TS_MIN));
            else {
                int64_t us = id == DUCKDB_TYPE_TIMESTAMP ? ts
                             : id == DUCKDB_TYPE_TIMESTAMP_S ? ts * 1000000
                             : id == DUCKDB_TYPE_TIMESTAMP_MS ? ts * 1000
                                                              : floordiv(ts, 1000);
                wr.ext_string(EXT_DATETIME, b, fmt_timestamp(b, us));
            }
            return;
        }
        case DUCKDB_TYPE_TIMESTAMP_TZ: {
            int64_t ts = ((int64_t *)data)[row];
            if (ts == TS_INFINITY) wr.ext_string(EXT_DATETIME, TS_MAX, strlen(TS_MAX));
            else if (ts == TS_NINFINITY) wr.ext_string(EXT_DATETIME, TS_MIN, strlen(TS_MIN));
            else {
                // [UTC isoformat with offset, zone]; the parent converts to
                // the zone (ZoneInfo), as the Python package does.
                size_t n = fmt_timestamp(b, ts);
                memcpy(b + n, "+00:00", 6);
                n += 6;
                wbuf *p = wr.ext_begin(EXT_DATETIME);
                w_array(p, 2);
                w_str(p, b, n);
                w_cstr(p, zone && !zone->empty() ? zone->c_str() : "UTC");
                w_end(p);
                wr.ext_end(EXT_DATETIME);
            }
            return;
        }
        case DUCKDB_TYPE_INTERVAL: {
            duckdb_interval iv = ((duckdb_interval *)data)[row];
            i128 total = ((i128)iv.months * 30 + iv.days) * MICROS_PER_DAY + iv.micros;
            i128 days = total / MICROS_PER_DAY;
            if (total % MICROS_PER_DAY < 0) days -= 1;
            i128 rest = total - days * MICROS_PER_DAY;
            wbuf *p = wr.ext_begin(EXT_TIMEDELTA);
            w_array(p, 3);
            w_int(p, (int64_t)days);
            w_int(p, (int64_t)(rest / 1000000));
            w_int(p, (int64_t)(rest % 1000000));
            w_end(p);
            wr.ext_end(EXT_TIMEDELTA);
            return;
        }
        case DUCKDB_TYPE_DECIMAL: {
            i128 x;
            switch (internal) {
            case DUCKDB_TYPE_SMALLINT: x = ((int16_t *)data)[row]; break;
            case DUCKDB_TYPE_INTEGER: x = ((int32_t *)data)[row]; break;
            case DUCKDB_TYPE_BIGINT: x = ((int64_t *)data)[row]; break;
            default: {
                duckdb_hugeint h = ((duckdb_hugeint *)data)[row];
                x = (i128)(((u128)(uint64_t)h.upper << 64) | h.lower);
            }
            }
            std::string t = decimal_text(x, scale);
            wr.ext_string(EXT_DECIMAL, t.data(), t.size());
            return;
        }
        case DUCKDB_TYPE_LIST: {
            duckdb_list_entry e = ((duckdb_list_entry *)data)[row];
            duckdb_vector cv = duckdb_list_vector_get_child(v);
            void *cd = duckdb_vector_get_data(cv);
            uint64_t *cval = duckdb_vector_get_validity(cv);
            w_array(w, (uint32_t)e.length);
            for (idx_t i = 0; i < e.length; i++) children[0]->write_at(wr, cv, cd, cval, e.offset + i);
            w_end(wr.w());
            return;
        }
        case DUCKDB_TYPE_ARRAY: {
            duckdb_vector cv = duckdb_array_vector_get_child(v);
            void *cd = duckdb_vector_get_data(cv);
            uint64_t *cval = duckdb_vector_get_validity(cv);
            wbuf *p = wr.ext_begin(EXT_TUPLE);
            w_array(p, (uint32_t)array_size);
            for (idx_t i = 0; i < array_size; i++) children[0]->write_at(wr, cv, cd, cval, row * array_size + i);
            w_end(wr.w());
            wr.ext_end(EXT_TUPLE);
            return;
        }
        case DUCKDB_TYPE_STRUCT: {
            size_t n = names.size();
            bool marked = n == 1 && (names[0] == "$b" || names[0] == "$d" || names[0] == "$x");
            if (marked) {
                wbuf *p = wr.ext_begin(EXT_MAP);
                w_array(p, 1);
                w_array(p, 2);
                w_str(p, names[0].data(), names[0].size());
                children[0]->write(wr, duckdb_struct_vector_get_child(v, 0), row);
                w_end(wr.w());
                w_end(wr.w());
                wr.ext_end(EXT_MAP);
                return;
            }
            w_map(w, (uint32_t)n);
            for (size_t i = 0; i < n; i++) {
                w_str(wr.w(), names[i].data(), names[i].size());
                children[i]->write(wr, duckdb_struct_vector_get_child(v, i), row);
            }
            w_end(wr.w());
            return;
        }
        case DUCKDB_TYPE_UNION: {
            duckdb_vector tags = duckdb_struct_vector_get_child(v, 0);
            uint8_t tag = ((uint8_t *)duckdb_vector_get_data(tags))[row];
            if (tag >= children.size()) {
                w_nil(w);
                return;
            }
            children[tag]->write(wr, duckdb_struct_vector_get_child(v, tag + 1), row);
            return;
        }
        case DUCKDB_TYPE_MAP: {
            duckdb_list_entry e = ((duckdb_list_entry *)data)[row];
            duckdb_vector entries = duckdb_list_vector_get_child(v);
            duckdb_vector kv = duckdb_struct_vector_get_child(entries, 0);
            duckdb_vector vv = duckdb_struct_vector_get_child(entries, 1);
            const ColumnCodec &kc = *children[0], &vc = *children[1];
            if (map_by_columns) {
                w_map(w, 2);
                w_cstr(w, "key");
                w_array(w, (uint32_t)e.length);
                for (idx_t i = 0; i < e.length; i++) kc.write(wr, kv, e.offset + i);
                w_end(wr.w());
                w_cstr(wr.w(), "value");
                w_array(wr.w(), (uint32_t)e.length);
                for (idx_t i = 0; i < e.length; i++) vc.write(wr, vv, e.offset + i);
                w_end(wr.w());
                w_end(wr.w());
                return;
            }
            bool marked = !key_is_str && e.length > 0;
            if (key_is_str && e.length == 1 && kc.id == DUCKDB_TYPE_VARCHAR) {
                duckdb_string_t *s = &((duckdb_string_t *)duckdb_vector_get_data(kv))[e.offset];
                std::string k(duckdb_string_t_data(s), duckdb_string_t_length(*s));
                marked = k == "$b" || k == "$d" || k == "$x";
            }
            if (marked) {
                wbuf *p = wr.ext_begin(EXT_MAP);
                w_array(p, (uint32_t)e.length);
                for (idx_t i = 0; i < e.length; i++) {
                    w_array(wr.w(), 2);
                    kc.write(wr, kv, e.offset + i);
                    vc.write(wr, vv, e.offset + i);
                    w_end(wr.w());
                }
                w_end(wr.w());
                wr.ext_end(EXT_MAP);
                return;
            }
            w_map(w, (uint32_t)e.length);
            for (idx_t i = 0; i < e.length; i++) {
                kc.write(wr, kv, e.offset + i);
                vc.write(wr, vv, e.offset + i);
            }
            w_end(wr.w());
            return;
        }
        case DUCKDB_TYPE_SQLNULL: w_nil(w); return;
        default:
            throw Error("NotSupported", "the DuckDB C++ worker cannot send values of DuckDB type id " + std::to_string((int)id));
        }
    }

    // _row_bytes(): None 1, str its length, bytes its length, else 8.
    int64_t bytes_at(duckdb_vector v, idx_t row) const {
        uint64_t *validity = duckdb_vector_get_validity(v);
        if (!valid_row(validity, row)) return 1;
        void *data = duckdb_vector_get_data(v);
        switch (id) {
        case DUCKDB_TYPE_VARCHAR: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            return (int64_t)utf8_chars(duckdb_string_t_data(s), duckdb_string_t_length(*s));
        }
        case DUCKDB_TYPE_BLOB: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            return (int64_t)duckdb_string_t_length(*s);
        }
        case DUCKDB_TYPE_BIT: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            return (int64_t)bit_text((const uint8_t *)duckdb_string_t_data(s), duckdb_string_t_length(*s)).size();
        }
        case DUCKDB_TYPE_BIGNUM: {
            duckdb_string_t *s = &((duckdb_string_t *)data)[row];
            return (int64_t)bignum_text((const uint8_t *)duckdb_string_t_data(s), duckdb_string_t_length(*s)).size();
        }
        case DUCKDB_TYPE_ENUM: {
            uint32_t ix = internal == DUCKDB_TYPE_UTINYINT ? ((uint8_t *)data)[row]
                          : internal == DUCKDB_TYPE_USMALLINT ? ((uint16_t *)data)[row]
                                                              : ((uint32_t *)data)[row];
            return ix < dict.size() ? (int64_t)utf8_chars(dict[ix].data(), dict[ix].size()) : 8;
        }
        case DUCKDB_TYPE_UNION: {
            uint8_t tag = ((uint8_t *)duckdb_vector_get_data(duckdb_struct_vector_get_child(v, 0)))[row];
            if (tag >= children.size()) return 1;
            return children[tag]->bytes_at(duckdb_struct_vector_get_child(v, tag + 1), row);
        }
        default: return 8;
        }
    }
};

ResultCodec::ResultCodec() {}
ResultCodec::~ResultCodec() {}

void ResultCodec::init(duckdb_result *res) {
    idx_t n = duckdb_column_count(res);
    for (idx_t i = 0; i < n; i++) {
        const char *name = duckdb_column_name(res, i);
        names.emplace_back(name ? name : "");
        TypeHandle t(duckdb_column_logical_type(res, i));
        types.push_back(type_name(t.t));
        if (type_has_timestamptz(t.t)) needs_zone = true;
        cols.emplace_back(new ColumnCodec(t.t, &zone));
    }
}

void ResultCodec::write_rows(Writer &wr, duckdb_data_chunk chunk, idx_t from, idx_t to) {
    size_t n = cols.size();
    duckdb_vector vecs[64];
    void *datas[64];
    uint64_t *vals[64];
    std::vector<duckdb_vector> vv;
    std::vector<void *> dv;
    std::vector<uint64_t *> valv;
    duckdb_vector *V = vecs;
    void **D = datas;
    uint64_t **L = vals;
    if (n > 64) {
        vv.resize(n), dv.resize(n), valv.resize(n);
        V = vv.data(), D = dv.data(), L = valv.data();
    }
    for (size_t c = 0; c < n; c++) {
        V[c] = duckdb_data_chunk_get_vector(chunk, c);
        D[c] = duckdb_vector_get_data(V[c]);
        L[c] = duckdb_vector_get_validity(V[c]);
    }
    for (idx_t r = from; r < to; r++) {
        w_array(wr.w(), (uint32_t)n);
        for (size_t c = 0; c < n; c++) cols[c]->write_at(wr, V[c], D[c], L[c], r);
        w_end(wr.w());
    }
}

int64_t ResultCodec::row_bytes(duckdb_data_chunk chunk, idx_t row) {
    int64_t size = 0;
    for (size_t c = 0; c < cols.size(); c++) size += cols[c]->bytes_at(duckdb_data_chunk_get_vector(chunk, c), row);
    return size;
}

// -- parameters ---------------------------------------------------------------------------

namespace {

struct Value {
    duckdb_value v = nullptr;
    Value() = default;
    explicit Value(duckdb_value x) : v(x) {}
    Value(Value &&o) noexcept : v(o.v) { o.v = nullptr; }
    Value &operator=(Value &&o) noexcept {
        std::swap(v, o.v);
        return *this;
    }
    ~Value() {
        if (v) duckdb_destroy_value(&v);
    }
};

Error invalid_input(const std::string &msg) {
    Error e = duckdb_error("Invalid Input Error: " + msg);
    return e;
}

bool parse_digits(const char *&p, const char *end, int n, int &out) {
    out = 0;
    for (int i = 0; i < n; i++) {
        if (p >= end || *p < '0' || *p > '9') return false;
        out = out * 10 + (*p++ - '0');
    }
    return true;
}

// "HH:MM:SS[.ffffff]" -> micros
bool parse_time(const char *&p, const char *end, int64_t &micros) {
    int h, m, s = 0, us = 0;
    if (!parse_digits(p, end, 2, h) || p >= end || *p++ != ':' || !parse_digits(p, end, 2, m)) return false;
    if (p < end && *p == ':') {
        p++;
        if (!parse_digits(p, end, 2, s)) return false;
        if (p < end && *p == '.') {
            p++;
            int digits = 0;
            while (p < end && *p >= '0' && *p <= '9' && digits < 6) {
                us = us * 10 + (*p++ - '0');
                digits++;
            }
            while (digits++ < 6) us *= 10;
            while (p < end && *p >= '0' && *p <= '9') p++;
        }
    }
    micros = ((int64_t)h * 3600 + m * 60 + s) * 1000000 + us;
    return true;
}

// "+HH:MM[:SS[.ffffff]]" -> seconds east of UTC
bool parse_offset(const char *&p, const char *end, int32_t &secs, bool &present) {
    present = false;
    if (p >= end) return true;
    if (*p == 'Z') {
        p++;
        present = true;
        secs = 0;
        return true;
    }
    if (*p != '+' && *p != '-') return false;
    int sign = *p++ == '-' ? -1 : 1;
    int64_t micros;
    if (!parse_time(p, end, micros)) return false;
    secs = (int32_t)(sign * (micros / 1000000));
    present = true;
    return true;
}

bool parse_date(const char *&p, const char *end, int64_t &days) {
    int y, m, d;
    if (!parse_digits(p, end, 4, y) || p >= end || *p++ != '-' || !parse_digits(p, end, 2, m) || p >= end ||
        *p++ != '-' || !parse_digits(p, end, 2, d))
        return false;
    days = days_from_civil(y, (unsigned)m, (unsigned)d);
    return true;
}

Value make_value(const val *x);

Value ext_value(const val *x) {
    const val *in = x->u.x.inner;
    int code = x->u.x.code;
    std::string s = in->t == V_STR ? std::string(in->u.s.p, in->u.s.n) : "";
    const char *p = s.data(), *end = s.data() + s.size();
    switch (code) {
    case EXT_DATETIME: {
        if (in->t == V_ARR && in->u.a.n == 2 && in->u.a.v[0].t == V_STR) {
            s.assign(in->u.a.v[0].u.s.p, in->u.a.v[0].u.s.n);
            p = s.data(), end = s.data() + s.size();
        }
        int64_t days, micros = 0;
        int32_t off = 0;
        bool tz = false;
        if (!parse_date(p, end, days)) break;
        if (p < end && (*p == 'T' || *p == ' ')) {
            p++;
            if (!parse_time(p, end, micros)) break;
        }
        if (!parse_offset(p, end, off, tz) || p != end) break;
        int64_t ts = days * MICROS_PER_DAY + micros;
        if (tz) {
            duckdb_timestamp t{ts - (int64_t)off * 1000000};
            return Value(duckdb_create_timestamp_tz(t));
        }
        return Value(duckdb_create_timestamp(duckdb_timestamp{ts}));
    }
    case EXT_DATE: {
        int64_t days;
        if (!parse_date(p, end, days) || p != end) break;
        return Value(duckdb_create_date(duckdb_date{(int32_t)days}));
    }
    case EXT_TIME: {
        int64_t micros;
        int32_t off = 0;
        bool tz = false;
        if (!parse_time(p, end, micros) || !parse_offset(p, end, off, tz) || p != end) break;
        if (tz) return Value(duckdb_create_time_tz_value(duckdb_create_time_tz(micros, off)));
        return Value(duckdb_create_time(duckdb_time{micros}));
    }
    case EXT_DECIMAL: {
        // As the Python package binds a Decimal: DECIMAL(width, scale), DOUBLE
        // past 38 digits, FLOAT for NaN and infinities.
        std::string low;
        for (char ch : s) low += (char)tolower((unsigned char)ch);
        if (low.find("nan") != std::string::npos) return Value(duckdb_create_float(NAN));
        if (low.find("inf") != std::string::npos) return Value(duckdb_create_float(low[0] == '-' ? -INFINITY : INFINITY));
        bool neg = false;
        size_t i = 0;
        if (i < s.size() && (s[i] == '-' || s[i] == '+')) neg = s[i++] == '-';
        std::string digits;
        int exp = 0;
        bool dot = false;
        for (; i < s.size(); i++) {
            char ch = s[i];
            if (ch >= '0' && ch <= '9') {
                digits += ch;
                if (dot) exp--;
            } else if (ch == '.') dot = true;
            else if (ch == 'e' || ch == 'E') {
                exp += atoi(s.c_str() + i + 1);
                break;
            } else break;
        }
        size_t nz = 0;
        while (nz + 1 < digits.size() && digits[nz] == '0') nz++;
        digits = digits.substr(nz);
        if (digits.empty()) digits = "0";
        int scale = exp < 0 ? -exp : 0;
        if (exp > 0) digits.append((size_t)exp, '0');
        int width = std::max((int)digits.size(), scale + 1);
        if (digits == "0") width = std::max(1, scale + 1);
        if (width > 38) return Value(duckdb_create_double(strtod(s.c_str(), nullptr)));
        u128 mag = 0;
        for (char ch : digits) mag = mag * 10 + (unsigned)(ch - '0');
        i128 v = neg ? -(i128)mag : (i128)mag;
        duckdb_decimal d;
        d.width = (uint8_t)width;
        d.scale = (uint8_t)scale;
        d.value.lower = (uint64_t)((u128)v & UINT64_MAX);
        d.value.upper = (int64_t)((u128)v >> 64);
        return Value(duckdb_create_decimal(d));
    }
    case EXT_UUID: {
        u128 v = 0;
        int n = 0;
        for (char ch : s) {
            int d = ch >= '0' && ch <= '9' ? ch - '0' : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10 : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10 : -1;
            if (d < 0) continue;
            v = (v << 4) | (unsigned)d;
            n++;
        }
        if (n != 32) break;
        duckdb_uhugeint u;
        u.lower = (uint64_t)(v & UINT64_MAX);
        u.upper = (uint64_t)(v >> 64);
        return Value(duckdb_create_uuid(u));
    }
    case EXT_TIMEDELTA: {
        if (in->t != V_ARR || in->u.a.n != 3) break;
        int64_t d = in->u.a.v[0].u.i, sec = in->u.a.v[1].u.i, us = in->u.a.v[2].u.i;
        duckdb_interval iv;
        iv.months = 0;
        iv.days = (int32_t)d;
        iv.micros = sec * 1000000 + us;
        return Value(duckdb_create_interval(iv));
    }
    case EXT_TUPLE: return make_value(in);
    default: break;
    }
    throw invalid_input("cannot bind a value of this type (ext " + std::to_string(code) + ")");
}


Value list_value(const val *x) {
    std::vector<Value> items;
    items.reserve(x->u.a.n);
    for (size_t i = 0; i < x->u.a.n; i++) items.push_back(make_value(&x->u.a.v[i]));
    // The child type: the one type of the non-null items, BIGINT/DOUBLE for
    // mixed numbers, VARCHAR otherwise (as the Python package does).
    duckdb_logical_type child = nullptr;
    std::string child_name;
    bool mixed = false;
    duckdb_type best = DUCKDB_TYPE_INVALID;
    auto rank = [](duckdb_type id) {
        switch (id) {
        case DUCKDB_TYPE_INTEGER: return 1;
        case DUCKDB_TYPE_BIGINT: return 2;
        case DUCKDB_TYPE_HUGEINT: return 3;
        case DUCKDB_TYPE_DOUBLE: return 4;
        default: return 0;
        }
    };
    for (auto &it : items) {
        if (duckdb_is_null_value(it.v)) continue;
        duckdb_logical_type t = duckdb_get_value_type(it.v);  // borrowed
        std::string name = type_name(t);
        if (child == nullptr) {
            child = t;
            child_name = name;
            best = duckdb_get_type_id(t);
        } else if (name != child_name) {
            duckdb_type a = duckdb_get_type_id(t);
            if (rank(a) && rank(best)) {
                if (rank(a) > rank(best)) {
                    best = a;
                    child = t;
                    child_name = name;
                }
            } else {
                mixed = true;
            }
        }
    }
    duckdb_logical_type owned = nullptr;
    if (child == nullptr) owned = duckdb_create_logical_type(DUCKDB_TYPE_SQLNULL);
    else if (mixed) owned = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_logical_type use = owned ? owned : child;
    std::vector<duckdb_value> raw;
    std::vector<Value> converted;
    for (auto &it : items) {
        if (mixed && !duckdb_is_null_value(it.v)) {
            char *text = duckdb_get_varchar(it.v);
            converted.emplace_back(duckdb_create_varchar(text ? text : ""));
            if (text) duckdb_free(text);
            raw.push_back(converted.back().v);
        } else if (!mixed && child != nullptr && !duckdb_is_null_value(it.v) && type_name(duckdb_get_value_type(it.v)) != child_name) {
            // widen a number to the list's type
            duckdb_type target = duckdb_get_type_id(child);
            if (target == DUCKDB_TYPE_DOUBLE) converted.emplace_back(duckdb_create_double(duckdb_get_double(it.v)));
            else converted.emplace_back(duckdb_create_int64(duckdb_get_int64(it.v)));
            raw.push_back(converted.back().v);
        } else {
            raw.push_back(it.v);
        }
    }
    duckdb_value out = duckdb_create_list_value(use, raw.data(), raw.size());
    if (owned) duckdb_destroy_logical_type(&owned);
    if (out == nullptr) throw invalid_input("could not bind a list parameter");
    return Value(out);
}

Value struct_value(const val *x) {
    size_t n = x->u.a.n;
    std::vector<Value> vals;
    std::vector<std::string> names;
    std::vector<duckdb_logical_type> types;
    for (size_t i = 0; i < n; i++) {
        const val *k = &x->u.a.v[2 * i];
        if (k->t == V_STR) names.emplace_back(k->u.s.p, k->u.s.n);
        else if (k->t == V_INT) names.push_back(std::to_string(k->u.i));
        else names.push_back("?");
        vals.push_back(make_value(&x->u.a.v[2 * i + 1]));
        types.push_back(duckdb_get_value_type(vals.back().v));
    }
    std::vector<const char *> cnames;
    for (auto &s : names) cnames.push_back(s.c_str());
    duckdb_logical_type st = duckdb_create_struct_type(types.data(), cnames.data(), n);
    std::vector<duckdb_value> raw;
    for (auto &v : vals) raw.push_back(v.v);
    duckdb_value out = duckdb_create_struct_value(st, raw.data());
    duckdb_destroy_logical_type(&st);
    if (out == nullptr) throw invalid_input("could not bind a dict parameter");
    return Value(out);
}

Value make_value(const val *x) {
    switch (x->t) {
    case V_NIL: return Value(duckdb_create_null_value());
    case V_BOOL: return Value(duckdb_create_bool(x->u.b != 0));
    case V_INT:
        if (x->u.i >= INT32_MIN && x->u.i <= INT32_MAX) return Value(duckdb_create_int32((int32_t)x->u.i));
        return Value(duckdb_create_int64(x->u.i));
    case V_BIGINT: {
        std::string t(x->u.s.p, x->u.s.n);
        bool neg = !t.empty() && t[0] == '-';
        u128 mag = 0;
        bool overflow = false;
        for (size_t i = neg ? 1 : 0; i < t.size(); i++) {
            if (t[i] < '0' || t[i] > '9') continue;
            if (mag > ((u128)-1) / 10) overflow = true;
            mag = mag * 10 + (unsigned)(t[i] - '0');
        }
        const u128 i128_max = ((u128)1 << 127) - 1;
        if (!neg && !overflow && mag <= UINT64_MAX) return Value(duckdb_create_uint64((uint64_t)mag));
        if (overflow || (neg ? mag > i128_max + 1 : mag > i128_max))
            throw invalid_input("Python integer too large for 128-bit integer type: " + t);
        i128 v = neg ? (i128)((u128)0 - mag) : (i128)mag;
        duckdb_hugeint h;
        h.lower = (uint64_t)((u128)v & UINT64_MAX);
        h.upper = (int64_t)((u128)v >> 64);
        return Value(duckdb_create_hugeint(h));
    }
    case V_FLOAT: return Value(duckdb_create_double(x->u.d));
    case V_STR: return Value(duckdb_create_varchar_length(x->u.s.p, x->u.s.n));
    case V_BIN: return Value(duckdb_create_blob((const uint8_t *)x->u.s.p, x->u.s.n));
    case V_ARR: return list_value(x);
    case V_MAP: return struct_value(x);
    case V_EXT: return ext_value(x);
    }
    throw invalid_input("cannot bind this value");
}

std::string lower(std::string s) {
    for (auto &ch : s) ch = (char)tolower((unsigned char)ch);
    return s;
}

}  // namespace

void bind_params(duckdb_prepared_statement p, const val *params) {
    idx_t n = duckdb_nparams(p);
    // Identifiers given: "1".."n" for a list, the keys of a map.
    std::vector<std::pair<std::string, const val *>> given;
    if (params != nullptr && params->t == V_ARR) {
        for (size_t i = 0; i < params->u.a.n; i++) given.emplace_back(std::to_string(i + 1), &params->u.a.v[i]);
    } else if (params != nullptr && params->t == V_MAP) {
        for (size_t i = 0; i < params->u.a.n; i++)
            given.emplace_back(val_str(&params->u.a.v[2 * i]), &params->u.a.v[2 * i + 1]);
    }
    if (n == 0) {
        if (!given.empty())
            throw duckdb_error("Binder Error: Parameter/argument count mismatch for prepared statement. Expected 0, got " +
                               std::to_string(given.size()));
        return;
    }
    std::vector<std::string> names;
    for (idx_t i = 1; i <= n; i++) {
        std::string nm = take(const_cast<char *>(duckdb_parameter_name(p, i)));
        names.push_back(nm.empty() ? std::to_string(i) : nm);
    }
    std::string excess, missing;
    for (auto &g : given) {
        bool found = false;
        for (auto &nm : names)
            if (lower(nm) == lower(g.first)) found = true;
        if (!found) excess += (excess.empty() ? "" : ", ") + g.first;
    }
    if (!excess.empty())
        throw invalid_input("Parameter argument/count mismatch, identifiers of the excess parameters: " + excess);
    for (auto &nm : names) {
        bool found = false;
        for (auto &g : given)
            if (lower(nm) == lower(g.first)) found = true;
        if (!found) missing += (missing.empty() ? "" : ", ") + nm;
    }
    if (!missing.empty()) throw invalid_input("Values were not provided for the following parameters: " + missing);
    for (idx_t i = 1; i <= n; i++) {
        const val *x = nullptr;
        for (auto &g : given)
            if (lower(g.first) == lower(names[i - 1])) x = g.second;
        Value v = make_value(x);
        if (duckdb_bind_value(p, i, v.v) != DuckDBSuccess)
            throw invalid_input("could not bind parameter " + names[i - 1]);
    }
}

}  // namespace dbw
