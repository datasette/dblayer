// Values between the wire and PostgreSQL's text format, mirroring the
// psycopg backend (dblayer/backends/postgres_psycopg.py) both ways.
//
// Parameters (wire value -> [text, type oid]), as psycopg sends them:
//   null                 NULL, type unknown
//   true / false         int4 1 / 0 (portable columns have no boolean type)
//   integer / bigint     int4, int8 or numeric by size (never int2)
//   float                float8
//   string               type unknown: the server converts it as the context needs
//   bytes                bytea
//   decimal, uuid, date, time(tz), datetime (naive: timestamp, aware:
//   timestamptz), timedelta (interval)
//   list                 an array of the elements' type (nested lists: more dimensions)
//
// Results (text -> wire value), by column type oid, as psycopg loads them:
//   int2/int4/int8/oid -> integer (bigint beyond 2^53), float4/float8 -> float,
//   numeric -> decimal, bool -> boolean, bytea -> bytes, date/time/timetz/
//   timestamp/timestamptz -> the value types, interval -> timedelta (a month is
//   30 days, a year 365, as psycopg), uuid -> uuid, arrays -> lists,
//   json/jsonb -> the JSON text (the parent parses it with json.loads, as
//   psycopg does, so numbers keep Python's int/float distinction), anything
//   else -> text. A value Python could not rebuild (a date outside years
//   1..9999, 'infinity') is sent as its text instead.

import { Ext, EXT_CODES, PyFloat } from './codec.mjs';
import { ARRAY_ELEMENTS } from './pgtypes.mjs';

export const OID = {
  unknown: 0, bool: 16, bytea: 17, int8: 20, int2: 21, int4: 23, text: 25, oid: 26, json: 114,
  float4: 700, float8: 701, int4array: 1007, textarray: 1009, int8array: 1016, float8array: 1022,
  boolarray: 1000, bytea_array: 1001, numericarray: 1231, date: 1082, time: 1083, timestamp: 1114,
  timestamptz: 1184, interval: 1186, timetz: 1266, numeric: 1700, uuid: 2950, jsonb: 3802,
  datearray: 1182, timearray: 1183, timestamparray: 1115, timestamptzarray: 1185, intervalarray: 1187,
  timetzarray: 1270, uuidarray: 2951,
};

export class ParamError extends Error {}

const ARRAY_OF = new Map([
  [OID.int4, OID.int4array], [OID.int8, OID.int8array], [OID.numeric, OID.numericarray],
  [OID.float8, OID.float8array], [OID.text, OID.textarray], [OID.bytea, OID.bytea_array],
  [OID.date, OID.datearray], [OID.time, OID.timearray], [OID.timestamp, OID.timestamparray],
  [OID.timestamptz, OID.timestamptzarray], [OID.interval, OID.intervalarray],
  [OID.timetz, OID.timetzarray], [OID.uuid, OID.uuidarray],
]);

const INT4_MIN = -2147483648;
const INT4_MAX = 2147483647;
const INT8_MIN = -(2n ** 63n);
const INT8_MAX = 2n ** 63n - 1n;

function hex(u8) {
  return Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength).toString('hex');
}

function floatParam(x) {
  if (x !== x) return 'NaN';
  if (x === Infinity) return 'Infinity';
  if (x === -Infinity) return '-Infinity';
  if (x === 0 && 1 / x < 0) return '-0';
  return String(x);
}

function intOid(v) {
  if (typeof v === 'number') return v >= INT4_MIN && v <= INT4_MAX ? OID.int4 : OID.int8;
  return v >= INT8_MIN && v <= INT8_MAX ? OID.int8 : OID.numeric;
}

function intervalText(p) {
  if (!Array.isArray(p) || p.length !== 3) throw new ParamError('bad timedelta payload');
  return `${p[0]} days ${p[1]} seconds ${p[2]} microseconds`;
}

/** [text, oid] for an Ext value. */
function extParam(x) {
  const d = x.data;
  switch (x.code) {
    case EXT_CODES.decimal: return [String(d), OID.numeric];
    case EXT_CODES.uuid: return [String(d), OID.uuid];
    case EXT_CODES.date: return [String(d), OID.date];
    case EXT_CODES.time: {
      const s = String(d);
      return [s, /[+-]\d\d(:?\d\d)?(:?\d\d)?$/.test(s) || s.endsWith('Z') ? OID.timetz : OID.time];
    }
    case EXT_CODES.datetime: {
      if (Array.isArray(d)) return [String(d[0]), OID.timestamptz]; // [iso with offset, zone name]
      const s = String(d);
      return [s, /[+-]\d\d:\d\d(:\d\d(\.\d+)?)?$|Z$/.test(s) ? OID.timestamptz : OID.timestamp];
    }
    case EXT_CODES.timedelta: return [intervalText(d), OID.interval];
    default:
      throw new ParamError(`cannot adapt a value of type ${x.code === EXT_CODES.map ? 'dict' : x.code === EXT_CODES.tuple ? 'tuple' : x.code}`);
  }
}

/** The scalar type a value would be sent as (for array elements). */
function scalar(v) {
  if (v === null || v === undefined) return [null, null];
  switch (typeof v) {
    case 'boolean': return [v ? '1' : '0', OID.int4];
    case 'number':
      if (Number.isInteger(v) && !(v === 0 && 1 / v < 0)) return [String(v), intOid(v)];
      return [floatParam(v), OID.float8];
    case 'bigint': return [v.toString(), intOid(v)];
    case 'string': return [v, OID.text];
    default:
  }
  if (v instanceof PyFloat) return [floatParam(v.v), OID.float8];
  if (v instanceof Uint8Array) return ['\\x' + hex(v), OID.bytea];
  if (v instanceof Ext) return extParam(v);
  throw new ParamError(`cannot adapt type '${Array.isArray(v) ? 'list' : 'dict'}'`);
}

const WIDER = new Map([[OID.int4, 0], [OID.int8, 1], [OID.numeric, 2]]); // int4 < int8 < numeric

/** One array element, quoted only when it must be (as psycopg writes them). */
function arrayItem(text) {
  if (text !== '' && !/[{}",\\\s]/.test(text) && text.toUpperCase() !== 'NULL') return text;
  return '"' + text.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
}

function arrayParam(list) {
  let elem = null;
  const walk = (a) => {
    const parts = [];
    for (const v of a) {
      if (Array.isArray(v)) {
        parts.push(walk(v));
        continue;
      }
      const [text, oid] = scalar(v);
      if (text === null) {
        parts.push('NULL');
        continue;
      }
      if (elem === null) elem = oid;
      else if (elem !== oid) {
        if (WIDER.has(elem) && WIDER.has(oid)) elem = WIDER.get(oid) > WIDER.get(elem) ? oid : elem;
        else if ((elem === OID.timestamp && oid === OID.timestamptz) || (elem === OID.timestamptz && oid === OID.timestamp)) elem = OID.timestamptz;
        else throw new ParamError('cannot dump lists of mixed types'); // as psycopg (float and int included)
      }
      parts.push(arrayItem(text));
    }
    return '{' + parts.join(',') + '}';
  };
  const text = walk(list);
  // Empty, only NULLs, or strings: type unknown, so the server decides (as
  // psycopg sends a list of str), e.g. text[] in "x = any(:list)".
  if (elem === null || elem === OID.text) return [text, OID.unknown];
  return [text, ARRAY_OF.get(elem) ?? OID.unknown];
}

/** [text | null, oid] for one parameter. */
export function param(v) {
  if (v === null || v === undefined) return [null, OID.unknown];
  if (typeof v === 'string') return [v, OID.unknown];
  if (Array.isArray(v)) return arrayParam(v);
  return scalar(v);
}

// -- results ----------------------------------------------------------------------

const DATE_RE = /^\d{4}-\d\d-\d\d$/;
const TS_RE = /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d{1,6})?$/;
const TSTZ_RE = /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d{1,6})?[+-]\d\d(:\d\d(:\d\d)?)?$/;
const TIME_RE = /^([01]\d|2[0-3]):\d\d:\d\d(\.\d{1,6})?$/;
const TIMETZ_RE = /^([01]\d|2[0-3]):\d\d:\d\d(\.\d{1,6})?[+-]\d\d(:\d\d(:\d\d)?)?$/;

function validYear(s) {
  return s.slice(0, 4) !== '0000';
}

let sessionZone = null; // the session TimeZone, when Python's ZoneInfo can take it

export function setSessionZone(name) {
  sessionZone = null;
  if (typeof name !== 'string' || !name || name.length > 64) return;
  try {
    new Intl.DateTimeFormat('en', { timeZone: name });
    if (/^[A-Za-z0-9_+\-/]+$/.test(name)) sessionZone = name;
  } catch {
    // a POSIX zone spec or an abbreviation: send fixed offsets
  }
}

function intervalValue(s) {
  // IntervalStyle 'postgres': [N year[s]] [N mon[s]] [N day[s]] [[-]HH:MM:SS[.ffffff]]
  const m = /^(?:(-?\d+) years? ?)?(?:(-?\d+) mons? ?)?(?:(-?\d+) days? ?)?(?:([+-])?(\d+):(\d\d):(\d\d)(?:\.(\d{1,6}))?)?$/.exec(s);
  if (!m || s === '') return s;
  const years = BigInt(m[1] ?? 0);
  const months = BigInt(m[2] ?? 0);
  const days = BigInt(m[3] ?? 0) + years * 365n + months * 30n;
  let micros = 0n;
  if (m[5] !== undefined) {
    micros = (BigInt(m[5]) * 3600n + BigInt(m[6]) * 60n + BigInt(m[7])) * 1000000n + BigInt((m[8] ?? '').padEnd(6, '0') || 0);
    if (m[4] === '-') micros = -micros;
  }
  const total = days * 86400000000n + micros;
  const DAY = 86400000000n;
  let d = total / DAY;
  let rest = total % DAY;
  if (rest < 0n) { rest += DAY; d -= 1n; }
  if (d > 999999999n || d < -999999999n) return s;
  return new Ext(EXT_CODES.timedelta, [Number(d), Number(rest / 1000000n), Number(rest % 1000000n)]);
}

function bytesValue(s) {
  if (s.startsWith('\\x')) return new Uint8Array(Buffer.from(s.slice(2), 'hex'));
  // bytea_output = 'escape' (only if a user changed it)
  const out = [];
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c === 92) {
      if (s[i + 1] === '\\') { out.push(92); i++; } else { out.push(parseInt(s.slice(i + 1, i + 4), 8)); i += 3; }
    } else out.push(c);
  }
  return new Uint8Array(out);
}

function floatValue(s) {
  const x = s === 'NaN' ? NaN : s === 'Infinity' ? Infinity : s === '-Infinity' ? -Infinity : Number(s);
  return Number.isInteger(x) ? new PyFloat(x) : x;
}

function intValue(s) {
  if (s.length < 16) return Number(s);
  const b = BigInt(s);
  return b >= -9007199254740991n && b <= 9007199254740991n ? Number(b) : b;
}

/** A converter text -> wire value for a column type, or null (keep text). */
export function converter(oid) {
  switch (oid) {
    case OID.int2: case OID.int4: case OID.int8: case OID.oid:
      return intValue;
    case OID.float4: case OID.float8:
      return floatValue;
    case OID.numeric:
      return (s) => new Ext(EXT_CODES.decimal, s);
    case OID.bool:
      return (s) => s === 't';
    case OID.bytea:
      return bytesValue;
    case OID.uuid:
      return (s) => new Ext(EXT_CODES.uuid, s);
    case OID.date:
      return (s) => (DATE_RE.test(s) && validYear(s) ? new Ext(EXT_CODES.date, s) : s);
    case OID.timestamp:
      return (s) => (TS_RE.test(s) && validYear(s) ? new Ext(EXT_CODES.datetime, s) : s);
    case OID.timestamptz:
      return (s) => {
        if (!TSTZ_RE.test(s) || !validYear(s)) return s;
        return new Ext(EXT_CODES.datetime, sessionZone ? [s, sessionZone] : s);
      };
    case OID.time:
      return (s) => (TIME_RE.test(s) ? new Ext(EXT_CODES.time, s) : s);
    case OID.timetz:
      return (s) => (TIMETZ_RE.test(s) ? new Ext(EXT_CODES.time, s) : s);
    case OID.interval:
      return intervalValue;
    default:
  }
  const elem = ARRAY_ELEMENTS.get(oid);
  if (elem !== undefined && elem !== OID.json && elem !== OID.jsonb) {
    const inner = converter(elem);
    const delim = elem === 603 ? ';' : ','; // box[] is the one type with another delimiter
    return (s) => parseArray(s, inner, delim);
  }
  return null;
}

/** PostgreSQL array text ({1,2,NULL}, nested, quoted) to a list. A form
 * this does not read (explicit bounds such as [0:1]={1,2}) stays text. */
export function parseArray(s, conv, delim = ',') {
  if (s[0] !== '{') return s;
  let i = 0;
  const n = s.length;
  const fail = Symbol('fail');
  const read = () => {
    // at '{'
    i++;
    const out = [];
    if (s[i] === '}') { i++; return out; }
    for (;;) {
      let v;
      if (s[i] === '{') {
        v = read();
        if (v === fail) return fail;
      } else if (s[i] === '"') {
        i++;
        let t = '';
        while (i < n && s[i] !== '"') {
          if (s[i] === '\\') i++;
          t += s[i++];
        }
        if (i >= n) return fail;
        i++;
        v = conv ? conv(t) : t;
      } else {
        const start = i;
        while (i < n && s[i] !== delim && s[i] !== '}') i++;
        const t = s.slice(start, i).trim();
        v = t === 'NULL' ? null : conv ? conv(t) : t;
      }
      out.push(v);
      if (s[i] === delim) { i++; continue; }
      if (s[i] === '}') { i++; return out; }
      return fail;
    }
  };
  const v = read();
  return v === fail || i !== n ? s : v;
}

/** The size estimate max_result_bytes is checked against (row_bytes in
 * dblayer/backends/_postgres_common.py), from the text values. */
export function textRowBytes(fields) {
  let size = 0;
  for (const f of fields) size += f === null ? 1 : f.length;
  return size;
}
