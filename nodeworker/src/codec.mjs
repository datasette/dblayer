// The two wire codecs of docs/worker-protocol.md, written for a JavaScript
// peer of a Python parent.
//
// Values in memory:
//   null, true/false, string, Array, dict (a null-prototype object),
//   number      an integer if Number.isInteger(v) (and not -0), else a float
//   PyFloat     a float whose value is integral (1.0, -0.0): Python keeps
//               int and float apart, and SQLite binds them differently
//   bigint      an integer outside +/-2^53 (SQLite INTEGER is 64-bit)
//   Uint8Array  bytes
//   Ext         a registered value type (datetime, date, ...): code + payload
//   Raw         an already-encoded fragment (result rows), spliced in as is
//
// Both decoders are strict, as Python's are: anything Python's json or
// msgpack would refuse (or that would make the worker's dispatch raise)
// throws, and the caller drops the connection.

const td = new TextDecoder('utf-8', { fatal: true });
const te = new TextEncoder();

export class PyFloat {
  constructor(v) { this.v = v; }
}

export class Ext {
  constructor(code, data) { this.code = code; this.data = data; }
}

export class Raw {
  constructor(bytes) { this.bytes = bytes; }
}

export const EXT_CODES = { datetime: 16, date: 17, time: 18, decimal: 19, uuid: 20, timedelta: 21, map: 22, tuple: 23 };
const TAGS = Object.fromEntries(Object.entries(EXT_CODES).map(([k, v]) => [v, k]));
export const BIGINT_EXT = 1;
const MAX_DEPTH = 900; // below Python's default recursion limit

export class DecodeError extends Error {}

function fail(msg) {
  throw new DecodeError(msg);
}

export function dict() {
  return Object.create(null);
}

/** A float value as the wire sees it: integral floats are kept as floats. */
export function float(v) {
  return Number.isInteger(v) ? new PyFloat(v) : v;
}

// -- JSON ----------------------------------------------------------------------

/** Python's json.loads plus dblayer's object hook ($b, $d, $x). */
export function jsonDecode(bytes) {
  const s = td.decode(bytes);
  const p = new JsonParser(s);
  p.ws();
  const v = p.value(0);
  p.ws();
  if (p.i !== s.length) fail('Extra data');
  return v;
}

class JsonParser {
  constructor(s) { this.s = s; this.i = 0; }

  ws() {
    const s = this.s;
    let i = this.i;
    for (;;) {
      const c = s.charCodeAt(i);
      if (c === 32 || c === 10 || c === 13 || c === 9) i++;
      else break;
    }
    this.i = i;
  }

  value(depth) {
    const s = this.s;
    const c = s.charCodeAt(this.i);
    switch (c) {
      case 123: return this.object(depth + 1); // {
      case 91: return this.array(depth + 1); // [
      case 34: return this.string(); // "
      case 116: return this.lit('true', true);
      case 102: return this.lit('false', false);
      case 110: return this.lit('null', null);
      case 78: return this.lit('NaN', NaN);
      case 73: return this.lit('Infinity', Infinity);
      case 45: // -
        if (s.startsWith('-Infinity', this.i)) { this.i += 9; return -Infinity; }
        return this.number();
      default:
        if (c >= 48 && c <= 57) return this.number();
        return fail(`Expecting value at ${this.i}`);
    }
  }

  lit(word, v) {
    if (!this.s.startsWith(word, this.i)) fail(`Expecting value at ${this.i}`);
    this.i += word.length;
    return v;
  }

  number() {
    const s = this.s;
    const start = this.i;
    let i = start;
    if (s.charCodeAt(i) === 45) i++;
    const d0 = s.charCodeAt(i);
    if (d0 === 48) i++;
    else if (d0 >= 49 && d0 <= 57) {
      while (i < s.length && s.charCodeAt(i) >= 48 && s.charCodeAt(i) <= 57) i++;
    } else fail(`Expecting value at ${start}`);
    let isFloat = false;
    if (s.charCodeAt(i) === 46) { // .
      const j = i + 1;
      let k = j;
      while (k < s.length && s.charCodeAt(k) >= 48 && s.charCodeAt(k) <= 57) k++;
      if (k > j) { isFloat = true; i = k; }
    }
    const e = s.charCodeAt(i);
    if (e === 101 || e === 69) { // e E
      let k = i + 1;
      const sign = s.charCodeAt(k);
      if (sign === 43 || sign === 45) k++;
      const j = k;
      while (k < s.length && s.charCodeAt(k) >= 48 && s.charCodeAt(k) <= 57) k++;
      if (k > j) { isFloat = true; i = k; }
    }
    this.i = i;
    const text = s.slice(start, i);
    if (isFloat) return float(Number(text));
    if (i - start < 16) return Number(text);
    const b = BigInt(text);
    return b >= -9007199254740991n && b <= 9007199254740991n ? Number(b) : b;
  }

  string() {
    const s = this.s;
    let i = this.i + 1;
    const start = i;
    // fast path: no escapes, no control characters
    for (;;) {
      if (i >= s.length) fail('Unterminated string');
      const c = s.charCodeAt(i);
      if (c === 34) { this.i = i + 1; return s.slice(start, i); }
      if (c === 92 || c < 32) break;
      i++;
    }
    let out = s.slice(start, i);
    for (;;) {
      if (i >= s.length) fail('Unterminated string');
      const c = s.charCodeAt(i);
      if (c === 34) { this.i = i + 1; return out; }
      if (c < 32) fail('Invalid control character');
      if (c !== 92) {
        const j = i;
        while (i < s.length) {
          const d = s.charCodeAt(i);
          if (d === 34 || d === 92 || d < 32) break;
          i++;
        }
        out += s.slice(j, i);
        continue;
      }
      const esc = s.charCodeAt(i + 1);
      i += 2;
      switch (esc) {
        case 34: out += '"'; break;
        case 92: out += '\\'; break;
        case 47: out += '/'; break;
        case 98: out += '\b'; break;
        case 102: out += '\f'; break;
        case 110: out += '\n'; break;
        case 114: out += '\r'; break;
        case 116: out += '\t'; break;
        case 117: { // \uXXXX
          const hex = s.slice(i, i + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail('Invalid \\uXXXX escape');
          out += String.fromCharCode(parseInt(hex, 16));
          i += 4;
          break;
        }
        default: fail('Invalid \\escape');
      }
    }
  }

  array(depth) {
    if (depth > MAX_DEPTH) fail('maximum recursion depth exceeded');
    this.i++;
    const out = [];
    this.ws();
    if (this.s.charCodeAt(this.i) === 93) { this.i++; return out; }
    for (;;) {
      this.ws();
      out.push(this.value(depth));
      this.ws();
      const c = this.s.charCodeAt(this.i++);
      if (c === 93) return out;
      if (c !== 44) fail("Expecting ',' delimiter");
    }
  }

  object(depth) {
    if (depth > MAX_DEPTH) fail('maximum recursion depth exceeded');
    this.i++;
    const out = dict();
    let n = 0;
    let only = null;
    this.ws();
    if (this.s.charCodeAt(this.i) === 125) { this.i++; return out; }
    for (;;) {
      this.ws();
      if (this.s.charCodeAt(this.i) !== 34) fail('Expecting property name enclosed in double quotes');
      const k = this.string();
      this.ws();
      if (this.s.charCodeAt(this.i++) !== 58) fail("Expecting ':' delimiter");
      this.ws();
      if (!(k in out)) n++;
      out[k] = this.value(depth);
      only = k;
      this.ws();
      const c = this.s.charCodeAt(this.i++);
      if (c === 125) break;
      if (c !== 44) fail("Expecting ',' delimiter");
    }
    if (n === 1 && only.charCodeAt(0) === 36) return jsonHook(only, out[only]);
    return out;
  }
}

function jsonHook(k, v) {
  if (k === '$b') {
    if (typeof v !== 'string') fail('$b needs a string');
    return new Uint8Array(Buffer.from(v, 'base64'));
  }
  if (k === '$d') {
    if (!Array.isArray(v) || v.length !== 2) fail('$d needs [key, value]');
    if (typeof v[0] === 'object' && v[0] !== null) fail('unhashable $d key');
    const d = dict();
    d[String(v[0])] = v[1];
    return d;
  }
  if (k === '$x') {
    if (!Array.isArray(v) || v.length !== 2) fail('$x needs [tag, payload]');
    const code = typeof v[0] === 'string' ? EXT_CODES[v[0]] : undefined;
    if (code === undefined) fail(`unknown value type ${v[0]}`);
    return new Ext(code, v[1]);
  }
  const d = dict();
  d[k] = v;
  return d;
}

/** Python's repr-compatible float text (json.dumps of a float). */
export function floatText(x) {
  if (x !== x) return 'NaN';
  if (x === Infinity) return 'Infinity';
  if (x === -Infinity) return '-Infinity';
  if (x === 0 && 1 / x < 0) return '-0.0';
  const s = String(x);
  if (Number.isInteger(x) && s.indexOf('e') < 0) return s + '.0';
  return s;
}

// -- MessagePack -----------------------------------------------------------------

export function msgpackDecode(bytes) {
  const r = new MpReader(bytes);
  const v = r.value(0);
  if (r.i !== bytes.length) fail('ExtraData');
  return v;
}

class MpReader {
  constructor(b) {
    this.b = b;
    this.dv = new DataView(b.buffer, b.byteOffset, b.byteLength);
    this.i = 0;
  }

  need(n) {
    if (this.i + n > this.b.length) fail('insufficient data');
  }

  u8() { this.need(1); return this.b[this.i++]; }
  u16() { this.need(2); const v = this.dv.getUint16(this.i); this.i += 2; return v; }
  u32() { this.need(4); const v = this.dv.getUint32(this.i); this.i += 4; return v; }

  str(n) {
    this.need(n);
    const b = this.b;
    const start = this.i;
    this.i += n;
    if (n < 64) {
      let ascii = true;
      for (let k = start; k < start + n; k++) if (b[k] >= 0x80) { ascii = false; break; }
      if (ascii) return String.fromCharCode.apply(null, b.subarray(start, start + n));
    }
    try {
      return td.decode(b.subarray(start, start + n));
    } catch {
      return fail('invalid UTF-8 in str');
    }
  }

  bin(n) {
    this.need(n);
    const v = this.b.slice(this.i, this.i + n);
    this.i += n;
    return v;
  }

  int64(signed) {
    this.need(8);
    const v = signed ? this.dv.getBigInt64(this.i) : this.dv.getBigUint64(this.i);
    this.i += 8;
    return v >= -9007199254740991n && v <= 9007199254740991n ? Number(v) : v;
  }

  array(n, depth) {
    if (depth > MAX_DEPTH) fail('maximum recursion depth exceeded');
    if (n > this.b.length - this.i) fail('array longer than the frame');
    const out = new Array(n);
    for (let k = 0; k < n; k++) out[k] = this.value(depth);
    return out;
  }

  map(n, depth) {
    if (depth > MAX_DEPTH) fail('maximum recursion depth exceeded');
    if (n * 2 > this.b.length - this.i) fail('map longer than the frame');
    const out = dict();
    for (let k = 0; k < n; k++) {
      const key = this.value(depth);
      if (typeof key === 'object' && key !== null && !(key instanceof PyFloat)) fail('unhashable map key');
      out[String(key instanceof PyFloat ? key.v : key)] = this.value(depth);
    }
    return out;
  }

  extOf(n) {
    this.need(1 + n);
    const code = this.dv.getInt8(this.i);
    this.i++;
    const data = this.b.subarray(this.i, this.i + n);
    this.i += n;
    if (code === BIGINT_EXT) {
      const text = td.decode(data);
      if (!/^-?[0-9]+$/.test(text)) fail('bad big integer');
      return BigInt(text);
    }
    if (TAGS[code] !== undefined) return new Ext(code, msgpackDecode(data));
    return new Ext(code, data.slice());
  }

  value(depth) {
    const t = this.u8();
    if (t < 0x80) return t;
    if (t >= 0xe0) return t - 0x100;
    if (t >= 0xa0 && t <= 0xbf) return this.str(t & 0x1f);
    if (t >= 0x90 && t <= 0x9f) return this.array(t & 0x0f, depth + 1);
    if (t >= 0x80 && t <= 0x8f) return this.map(t & 0x0f, depth + 1);
    const dv = this.dv;
    switch (t) {
      case 0xc0: return null;
      case 0xc2: return false;
      case 0xc3: return true;
      case 0xc4: return this.bin(this.u8());
      case 0xc5: return this.bin(this.u16());
      case 0xc6: return this.bin(this.u32());
      case 0xc7: return this.extOf(this.u8());
      case 0xc8: return this.extOf(this.u16());
      case 0xc9: return this.extOf(this.u32());
      case 0xca: { this.need(4); const v = dv.getFloat32(this.i); this.i += 4; return float(v); }
      case 0xcb: { this.need(8); const v = dv.getFloat64(this.i); this.i += 8; return float(v); }
      case 0xcc: return this.u8();
      case 0xcd: return this.u16();
      case 0xce: return this.u32();
      case 0xcf: return this.int64(false);
      case 0xd0: { this.need(1); return dv.getInt8(this.i++); }
      case 0xd1: { this.need(2); const v = dv.getInt16(this.i); this.i += 2; return v; }
      case 0xd2: { this.need(4); const v = dv.getInt32(this.i); this.i += 4; return v; }
      case 0xd3: return this.int64(true);
      case 0xd4: return this.extOf(1);
      case 0xd5: return this.extOf(2);
      case 0xd6: return this.extOf(4);
      case 0xd7: return this.extOf(8);
      case 0xd8: return this.extOf(16);
      case 0xd9: return this.str(this.u8());
      case 0xda: return this.str(this.u16());
      case 0xdb: return this.str(this.u32());
      case 0xdc: return this.array(this.u16(), depth + 1);
      case 0xdd: return this.array(this.u32(), depth + 1);
      case 0xde: return this.map(this.u16(), depth + 1);
      case 0xdf: return this.map(this.u32(), depth + 1);
      default: return fail(`invalid msgpack byte 0x${t.toString(16)}`);
    }
  }
}

// -- the writer (both codecs) -------------------------------------------------------

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const UINT64_MAX = 2n ** 64n - 1n;

/** A growable byte buffer that encodes values in one codec. */
export class Writer {
  constructor(codec = 'json') {
    this.codec = codec;
    this.json = codec === 'json';
    this.buf = new Uint8Array(1 << 16);
    this.dv = new DataView(this.buf.buffer);
    this.pos = 0;
  }

  reset() {
    if (this.buf.length > 1 << 22) {
      this.buf = new Uint8Array(1 << 16);
      this.dv = new DataView(this.buf.buffer);
    }
    this.pos = 0;
  }

  ensure(n) {
    const need = this.pos + n;
    if (need <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < need) size *= 2;
    const nb = new Uint8Array(size);
    nb.set(this.buf.subarray(0, this.pos));
    this.buf = nb;
    this.dv = new DataView(nb.buffer);
  }

  byte(b) {
    if (this.pos >= this.buf.length) this.ensure(1);
    this.buf[this.pos++] = b;
  }

  bytes(u8) {
    this.ensure(u8.length);
    this.buf.set(u8, this.pos);
    this.pos += u8.length;
  }

  ascii(s) {
    const n = s.length;
    this.ensure(n);
    const buf = this.buf;
    let p = this.pos;
    for (let k = 0; k < n; k++) buf[p++] = s.charCodeAt(k);
    this.pos = p;
  }

  /** Everything written so far (a copy). */
  take() {
    return this.buf.slice(0, this.pos);
  }

  // -- scalar values

  nil() { if (this.json) this.ascii('null'); else this.byte(0xc0); }

  bool(v) { if (this.json) this.ascii(v ? 'true' : 'false'); else this.byte(v ? 0xc3 : 0xc2); }

  /** An integer: a safe-integer number or a bigint. */
  int(v) {
    if (this.json) { this.ascii(String(v)); return; }
    if (typeof v === 'number') {
      if (v >= 0) {
        if (v < 128) { this.byte(v); return; }
        if (v < 256) { this.ensure(2); this.buf[this.pos++] = 0xcc; this.buf[this.pos++] = v; return; }
        if (v < 65536) { this.ensure(3); this.buf[this.pos++] = 0xcd; this.dv.setUint16(this.pos, v); this.pos += 2; return; }
        if (v < 4294967296) { this.ensure(5); this.buf[this.pos++] = 0xce; this.dv.setUint32(this.pos, v); this.pos += 4; return; }
      } else {
        if (v >= -32) { this.byte(v & 0xff); return; }
        if (v >= -128) { this.ensure(2); this.buf[this.pos++] = 0xd0; this.dv.setInt8(this.pos++, v); return; }
        if (v >= -32768) { this.ensure(3); this.buf[this.pos++] = 0xd1; this.dv.setInt16(this.pos, v); this.pos += 2; return; }
        if (v >= -2147483648) { this.ensure(5); this.buf[this.pos++] = 0xd2; this.dv.setInt32(this.pos, v); this.pos += 4; return; }
      }
      v = BigInt(v);
    }
    this.ensure(9);
    if (v >= 0n && v <= UINT64_MAX) {
      if (v > INT64_MAX) { this.buf[this.pos++] = 0xcf; this.dv.setBigUint64(this.pos, v); }
      else { this.buf[this.pos++] = 0xd3; this.dv.setBigInt64(this.pos, v); }
      this.pos += 8;
    } else if (v < 0n && v >= INT64_MIN) {
      this.buf[this.pos++] = 0xd3;
      this.dv.setBigInt64(this.pos, v);
      this.pos += 8;
    } else {
      // outside 64 bits: ext type 1 with the decimal text
      const text = te.encode(v.toString());
      this.extHeader(BIGINT_EXT, text.length);
      this.bytes(text);
    }
  }

  float(x) {
    if (this.json) { this.ascii(floatText(x)); return; }
    this.ensure(9);
    this.buf[this.pos++] = 0xcb;
    this.dv.setFloat64(this.pos, x);
    this.pos += 8;
  }

  str(s) {
    const n = s.length;
    if (this.json) {
      // JSON.stringify escapes exactly what JSON needs (and lone surrogates)
      let plain = true;
      for (let k = 0; k < n; k++) {
        const c = s.charCodeAt(k);
        if (c < 32 || c === 34 || c === 92 || c >= 0x7f) { plain = false; break; }
      }
      if (plain) { this.ensure(n + 2); this.byte(34); this.ascii(s); this.byte(34); return; }
      this.utf8(JSON.stringify(s));
      return;
    }
    let ascii = true;
    for (let k = 0; k < n; k++) if (s.charCodeAt(k) >= 0x80) { ascii = false; break; }
    if (ascii) {
      this.strHeader(n);
      this.ascii(s);
      return;
    }
    const enc = te.encode(s);
    this.strHeader(enc.length);
    this.bytes(enc);
  }

  utf8(s) {
    this.ensure(s.length * 3);
    const { written } = te.encodeInto(s, this.buf.subarray(this.pos));
    this.pos += written;
  }

  strHeader(n) {
    this.ensure(5);
    if (n < 32) this.buf[this.pos++] = 0xa0 | n;
    else if (n < 256) { this.buf[this.pos++] = 0xd9; this.buf[this.pos++] = n; }
    else if (n < 65536) { this.buf[this.pos++] = 0xda; this.dv.setUint16(this.pos, n); this.pos += 2; }
    else { this.buf[this.pos++] = 0xdb; this.dv.setUint32(this.pos, n); this.pos += 4; }
  }

  binHeader(n) {
    this.ensure(5);
    if (n < 256) { this.buf[this.pos++] = 0xc4; this.buf[this.pos++] = n; }
    else if (n < 65536) { this.buf[this.pos++] = 0xc5; this.dv.setUint16(this.pos, n); this.pos += 2; }
    else { this.buf[this.pos++] = 0xc6; this.dv.setUint32(this.pos, n); this.pos += 4; }
  }

  extHeader(code, n) {
    this.ensure(6);
    const fixed = { 1: 0xd4, 2: 0xd5, 4: 0xd6, 8: 0xd7, 16: 0xd8 }[n];
    if (fixed !== undefined) this.buf[this.pos++] = fixed;
    else if (n < 256) { this.buf[this.pos++] = 0xc7; this.buf[this.pos++] = n; }
    else if (n < 65536) { this.buf[this.pos++] = 0xc8; this.dv.setUint16(this.pos, n); this.pos += 2; }
    else { this.buf[this.pos++] = 0xc9; this.dv.setUint32(this.pos, n); this.pos += 4; }
    this.dv.setInt8(this.pos++, code);
  }

  bin(u8) {
    if (this.json) {
      this.ascii('{"$b":"');
      this.ascii(Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength).toString('base64'));
      this.ascii('"}');
      return;
    }
    this.binHeader(u8.length);
    this.bytes(u8);
  }

  arrayHeader(n) {
    if (this.json) throw new Error('arrayHeader is MessagePack only');
    this.ensure(5);
    if (n < 16) this.buf[this.pos++] = 0x90 | n;
    else if (n < 65536) { this.buf[this.pos++] = 0xdc; this.dv.setUint16(this.pos, n); this.pos += 2; }
    else { this.buf[this.pos++] = 0xdd; this.dv.setUint32(this.pos, n); this.pos += 4; }
  }

  mapHeader(n) {
    this.ensure(5);
    if (n < 16) this.buf[this.pos++] = 0x80 | n;
    else if (n < 65536) { this.buf[this.pos++] = 0xde; this.dv.setUint16(this.pos, n); this.pos += 2; }
    else { this.buf[this.pos++] = 0xdf; this.dv.setUint32(this.pos, n); this.pos += 4; }
  }

  // -- any value

  value(v) {
    switch (typeof v) {
      case 'number':
        if (Number.isInteger(v) && !(v === 0 && 1 / v < 0)) this.int(v);
        else this.float(v);
        return;
      case 'bigint': this.int(v); return;
      case 'string': this.str(v); return;
      case 'boolean': this.bool(v); return;
      case 'undefined': this.nil(); return;
      default:
    }
    if (v === null) { this.nil(); return; }
    if (v instanceof PyFloat) { this.float(v.v); return; }
    if (v instanceof Uint8Array) { this.bin(v); return; }
    if (v instanceof Raw) { this.bytes(v.bytes); return; }
    if (Array.isArray(v)) { this.array(v); return; }
    if (v instanceof Ext) { this.ext(v); return; }
    this.object(v);
  }

  array(a) {
    if (this.json) {
      this.byte(91);
      for (let k = 0; k < a.length; k++) {
        if (k) this.byte(44);
        this.value(a[k]);
      }
      this.byte(93);
      return;
    }
    this.arrayHeader(a.length);
    for (let k = 0; k < a.length; k++) this.value(a[k]);
  }

  object(o) {
    const keys = Object.keys(o).filter((k) => o[k] !== undefined);
    if (this.json) {
      this.byte(123);
      for (let k = 0; k < keys.length; k++) {
        if (k) this.byte(44);
        this.str(keys[k]);
        this.byte(58);
        this.value(o[keys[k]]);
      }
      this.byte(125);
      return;
    }
    this.mapHeader(keys.length);
    for (const k of keys) {
      this.str(k);
      this.value(o[k]);
    }
  }

  ext(x) {
    if (this.json) {
      this.ascii('{"$x":[');
      this.str(TAGS[x.code] ?? String(x.code));
      this.byte(44);
      this.value(x.data);
      this.ascii(']}');
      return;
    }
    const inner = new Writer('msgpack');
    inner.value(x.data);
    const body = inner.take();
    this.extHeader(x.code, body.length);
    this.bytes(body);
  }
}

export function decode(codec, bytes) {
  return codec === 'msgpack' ? msgpackDecode(bytes) : jsonDecode(bytes);
}

/** One message, encoded (no frame header). */
export function encode(codec, msg) {
  const w = new Writer(codec);
  w.value(msg);
  return w.take();
}
