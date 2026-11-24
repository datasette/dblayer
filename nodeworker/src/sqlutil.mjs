// SQL text helpers that must agree with the Python side (ported from
// cworker/src/sqlutil.c). They work on JavaScript strings: the only
// non-ASCII characters they meet count as identifier characters, as
// Python's str.isalnum() does for letters.

export const KW = { OTHER: 0, SELECT: 1, INSERT: 2, UPDATE: 3, DELETE: 4, REPLACE: 5, VALUES: 6, MERGE: 7, NONE: 8 };
const VERBS = { SELECT: KW.SELECT, INSERT: KW.INSERT, UPDATE: KW.UPDATE, DELETE: KW.DELETE, REPLACE: KW.REPLACE, VALUES: KW.VALUES, MERGE: KW.MERGE };

const isAlpha = (c) => (c >= 65 && c <= 90) || (c >= 97 && c <= 122);
const isDigit = (c) => c >= 48 && c <= 57;
const identChar = (c) => isAlpha(c) || isDigit(c) || c === 95 || c === 36 || c >= 0x80;
const nameStart = (c) => isAlpha(c) || c === 95 || c >= 0x80;
const nameChar = (c) => isAlpha(c) || isDigit(c) || c === 95 || c >= 0x80;

/** dblayer.core.params.leading_keyword (SQLite flavor). */
export function leadingKeyword(sql) {
  const n = sql.length;
  let depth = 0;
  let haveFirst = false;
  let i = 0;
  while (i < n) {
    const c = sql.charCodeAt(i);
    if (c === 39) { // '
      let j = i + 1;
      for (;;) {
        if (j >= n) { i = n; break; }
        if (sql.charCodeAt(j) === 39) {
          if (j + 1 < n && sql.charCodeAt(j + 1) === 39) { j += 2; continue; }
          i = j + 1;
          break;
        }
        j++;
      }
    } else if (c === 34 || c === 96) { // " `
      let j = i + 1;
      for (;;) {
        const e = sql.indexOf(sql[i], j);
        if (e < 0) { i = n; break; }
        if (e + 1 < n && sql.charCodeAt(e + 1) === c) { j = e + 2; continue; }
        i = e + 1;
        break;
      }
    } else if (c === 91) { // [
      const e = sql.indexOf(']', i + 1);
      i = e < 0 ? n : e + 1;
    } else if (c === 45) { // -
      if (sql.charCodeAt(i + 1) === 45) {
        const e = sql.indexOf('\n', i + 2);
        i = e < 0 ? n : e + 1;
      } else i++;
    } else if (c === 47) { // /
      if (sql.charCodeAt(i + 1) === 42) {
        const e = sql.indexOf('*/', i + 2);
        i = e < 0 ? n : e + 2;
      } else i++;
    } else if (c === 58) { // :
      if (sql.charCodeAt(i + 1) === 58) i += 2;
      else if (i + 1 < n && nameStart(sql.charCodeAt(i + 1)) && (i === 0 || !identChar(sql.charCodeAt(i - 1)))) {
        let j = i + 2;
        while (j < n && nameChar(sql.charCodeAt(j))) j++;
        i = j;
      } else i++;
    } else if (c === 63) { // ?
      let j = i + 1;
      while (j < n && isDigit(sql.charCodeAt(j))) j++;
      i = j;
    } else if (c === 40) {
      depth++;
      i++;
    } else if (c === 41) {
      depth = depth > 0 ? depth - 1 : 0;
      i++;
    } else if (isAlpha(c) || c === 95) {
      let j = i + 1;
      while (j < n && identChar(sql.charCodeAt(j))) j++;
      if (i > 0 && identChar(sql.charCodeAt(i - 1))) { i = j; continue; }
      const word = sql.slice(i, j).toUpperCase();
      const kw = VERBS[word] ?? KW.OTHER;
      const isWith = word === 'WITH';
      if (!haveFirst) {
        if (depth > 0 && kw === KW.OTHER && !isWith) { i = j; continue; }
        haveFirst = true;
        if (!isWith) return kw;
      } else if (depth === 0 && kw !== KW.OTHER) {
        return kw;
      }
      i = j;
    } else {
      i++;
    }
  }
  return haveFirst ? KW.OTHER : KW.NONE;
}

/** CPython's lstrip_sql over bytes [p, end): the offset of the first byte
 * that is not whitespace or a comment, or -1. */
export function lstripSqlBytes(b, p, end) {
  while (p < end) {
    const c = b[p];
    if (c === 32 || c === 9 || c === 12 || c === 10 || c === 13) { p++; continue; }
    if (c === 45 && p + 1 < end && b[p + 1] === 45) {
      p += 2;
      while (p < end && b[p] !== 10) p++;
      if (p === end) return -1;
      p++;
      continue;
    }
    if (c === 47 && p + 1 < end && b[p + 1] === 42) {
      p += 2;
      while (p + 1 < end && !(b[p] === 42 && b[p + 1] === 47)) p++;
      if (p + 1 >= end) return -1;
      p += 2;
      continue;
    }
    return p;
  }
  return -1;
}

/** CPython's is_dml: the first word starts with insert, update, delete or
 * replace (a case-insensitive prefix match). */
export function isDml(sql) {
  let p = 0;
  const n = sql.length;
  // lstrip_sql on the string (ASCII whitespace and comments only)
  while (p < n) {
    const c = sql.charCodeAt(p);
    if (c === 32 || c === 9 || c === 12 || c === 10 || c === 13) { p++; continue; }
    if (c === 45 && sql.charCodeAt(p + 1) === 45) {
      const e = sql.indexOf('\n', p + 2);
      if (e < 0) return false;
      p = e + 1;
      continue;
    }
    if (c === 47 && sql.charCodeAt(p + 1) === 42) {
      const e = sql.indexOf('*/', p + 2);
      if (e < 0) return false;
      p = e + 2;
      continue;
    }
    break;
  }
  const head = sql.slice(p, p + 7).toLowerCase();
  return head.startsWith('insert') || head.startsWith('update') || head.startsWith('delete') || head.startsWith('replace');
}

/** pathlib.Path(path).as_uri() for an absolute POSIX path. */
export function pathAsUri(path) {
  const bytes = Buffer.from(path, 'utf8');
  let out = 'file://';
  for (const c of bytes) {
    if (isAlpha(c) || isDigit(c) || c === 47 || c === 95 || c === 46 || c === 45 || c === 126) out += String.fromCharCode(c);
    else out += '%' + c.toString(16).toUpperCase().padStart(2, '0');
  }
  return out;
}
