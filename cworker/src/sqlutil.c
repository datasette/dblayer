/* See sqlutil.h. */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include "sqlutil.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* Python's str.isalnum() is true for non-ASCII letters too: treat every
 * byte of a multi-byte UTF-8 character as an identifier character. */
static int ident_char(unsigned char c) {
    return isalnum(c) || c == '_' || c == '$' || c >= 0x80;
}

static int name_start(unsigned char c) { return isalpha(c) || c == '_' || c >= 0x80; }
static int name_char(unsigned char c) { return isalnum(c) || c == '_' || c >= 0x80; }

static int verb(const char *w, size_t n) {
    static const struct { const char *s; int kw; } V[] = {
        {"SELECT", KW_SELECT}, {"INSERT", KW_INSERT}, {"UPDATE", KW_UPDATE},
        {"DELETE", KW_DELETE}, {"REPLACE", KW_REPLACE}, {"VALUES", KW_VALUES},
        {"MERGE", KW_MERGE},
    };
    for (size_t i = 0; i < sizeof V / sizeof V[0]; i++)
        if (strlen(V[i].s) == n && strncasecmp(w, V[i].s, n) == 0)
            return V[i].kw;
    return KW_OTHER;
}

static size_t find(const char *s, size_t n, size_t from, const char *needle) {
    size_t k = strlen(needle);
    for (size_t i = from; i + k <= n; i++)
        if (memcmp(s + i, needle, k) == 0)
            return i;
    return (size_t)-1;
}

int leading_keyword(const char *sql, size_t n) {
    const unsigned char *s = (const unsigned char *)sql;
    int depth = 0, have_first = 0, first_is_with = 0;
    size_t i = 0;
    while (i < n) {
        unsigned char c = s[i];
        if (c == '\'') {
            size_t j = i + 1;
            for (;;) {
                if (j >= n) { i = n; break; }
                if (s[j] == '\'') {
                    if (j + 1 < n && s[j + 1] == '\'') { j += 2; continue; }
                    i = j + 1;
                    break;
                }
                j++;
            }
        } else if (c == '"' || c == '`') {
            size_t j = i + 1;
            for (;;) {
                const unsigned char *e = j < n ? memchr(s + j, c, n - j) : NULL;
                if (e == NULL) { i = n; break; }
                size_t end = (size_t)(e - s);
                if (end + 1 < n && s[end + 1] == c) { j = end + 2; continue; }
                i = end + 1;
                break;
            }
        } else if (c == '[') {
            const unsigned char *e = memchr(s + i + 1, ']', n - i - 1);
            i = e ? (size_t)(e - s) + 1 : n;
        } else if (c == '-') {
            if (i + 1 < n && s[i + 1] == '-') {
                const unsigned char *e = memchr(s + i + 2, '\n', n - i - 2);
                i = e ? (size_t)(e - s) + 1 : n;
            } else {
                i++;
            }
        } else if (c == '/') {
            if (i + 1 < n && s[i + 1] == '*') {
                size_t e = find(sql, n, i + 2, "*/");
                i = e == (size_t)-1 ? n : e + 2;
            } else {
                i++;
            }
        } else if (c == ':') {
            if (i + 1 < n && s[i + 1] == ':') {
                i += 2;
            } else if (i + 1 < n && name_start(s[i + 1]) && (i == 0 || !ident_char(s[i - 1]))) {
                size_t j = i + 2;
                while (j < n && name_char(s[j])) j++;
                i = j;
            } else {
                i++;
            }
        } else if (c == '?') {
            size_t j = i + 1;
            while (j < n && isdigit(s[j])) j++;
            i = j;
        } else if (c == '(') {
            depth++;
            i++;
        } else if (c == ')') {
            depth = depth > 0 ? depth - 1 : 0;
            i++;
        } else if (isalpha(c) || c == '_') {
            size_t j = i + 1;
            while (j < n && ident_char(s[j])) j++;
            if (i > 0 && ident_char(s[i - 1])) {
                i = j;
                continue;
            }
            size_t len = j - i;
            int kw = verb(sql + i, len);
            int is_with = len == 4 && strncasecmp(sql + i, "WITH", 4) == 0;
            if (!have_first) {
                if (depth > 0 && kw == KW_OTHER && !is_with) {
                    i = j;
                    continue;
                }
                have_first = 1;
                first_is_with = is_with;
                if (!is_with)
                    return kw;
            } else if (depth == 0 && kw != KW_OTHER) {
                return kw;
            }
            i = j;
        } else {
            i++;
        }
    }
    (void)first_is_with; /* "WITH" with no verb after it: not a verb we track */
    return have_first ? KW_OTHER : KW_NONE;
}

const char *lstrip_sql(const char *p, const char *end) {
    while (p < end) {
        switch (*p) {
        case ' ': case '\t': case '\f': case '\n': case '\r':
            p++;
            continue;
        case '-':
            if (p + 1 < end && p[1] == '-') {
                p += 2;
                while (p < end && *p != '\n') p++;
                if (p == end) return NULL;
                p++;
                continue;
            }
            return p;
        case '/':
            if (p + 1 < end && p[1] == '*') {
                p += 2;
                while (p + 1 < end && !(p[0] == '*' && p[1] == '/')) p++;
                if (p + 1 >= end) return NULL;
                p += 2;
                continue;
            }
            return p;
        default:
            return p;
        }
    }
    return NULL;
}

int cpython_is_dml(const char *sql, size_t n) {
    const char *end = sql + n;
    const char *p = lstrip_sql(sql, end);
    if (p == NULL)
        return 0;
    size_t left = (size_t)(end - p);
    return (left >= 6 && (strncasecmp(p, "insert", 6) == 0 || strncasecmp(p, "update", 6) == 0 ||
                          strncasecmp(p, "delete", 6) == 0)) ||
           (left >= 7 && strncasecmp(p, "replace", 7) == 0);
}

char *path_as_uri(const char *path) {
    static const char hex[] = "0123456789ABCDEF";
    size_t n = strlen(path);
    char *out = malloc(7 + n * 3 + 1);
    if (out == NULL)
        return NULL;
    memcpy(out, "file://", 7);
    size_t o = 7;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)path[i];
        if (isalnum(c) || c == '/' || c == '_' || c == '.' || c == '-' || c == '~') {
            out[o++] = (char)c;
        } else {
            out[o++] = '%';
            out[o++] = hex[c >> 4];
            out[o++] = hex[c & 15];
        }
    }
    out[o] = 0;
    return out;
}
