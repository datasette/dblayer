// The one PostgreSQL session a PGlite instance is, driven with the
// extended query protocol through PGlite's execProtocol (no query() and no
// JavaScript type parsers: values travel as PostgreSQL text and are
// converted once, in values.mjs).

import { PGlite, protocol } from '@electric-sql/pglite';
import { TYPE_NAMES } from './pgtypes.mjs';
import { converter, param, ParamError } from './values.mjs';

const S = protocol.serialize;

export class PgError extends Error {
  constructor(fields) {
    super(fields.message);
    this.fields = fields;
  }
}

export class LimitError extends Error {}

function concat(parts) {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
}

function valueBytes(v) {
  if (v === null) return 1;
  if (typeof v === 'string') return v.length;
  if (v instanceof Uint8Array) return v.length;
  return 8;
}

const DML = new Set(['INSERT', 'UPDATE', 'DELETE', 'MERGE']);

/** First keyword of a statement, skipping whitespace, comments and
 * opening parentheses. */
export function leadingKeyword(sql) {
  let i = 0;
  const n = sql.length;
  for (;;) {
    while (i < n && /[\s(]/.test(sql[i])) i++;
    if (sql.startsWith('--', i)) {
      const j = sql.indexOf('\n', i);
      if (j < 0) return '';
      i = j + 1;
      continue;
    }
    if (sql.startsWith('/*', i)) {
      let depth = 0;
      while (i < n) {
        if (sql.startsWith('/*', i)) { depth++; i += 2; } else if (sql.startsWith('*/', i)) { depth--; i += 2; if (!depth) break; } else i++;
      }
      continue;
    }
    break;
  }
  const m = /^[A-Za-z_]+/.exec(sql.slice(i, i + 32));
  return m ? m[0].toUpperCase() : '';
}

const WRITES_IN_WITH = /\b(insert|update|delete|merge)\b/i;

/** Whether an Execute row limit can stop this statement early without
 * changing what it does: plain reads only (a data-modifying statement
 * must run to completion and report its row count). */
export function limitable(sql) {
  const kw = leadingKeyword(sql);
  // SELECT ... INTO creates a table; any "into" (even in a string) is
  // taken to mean that, which only costs fetching every row.
  if (kw === 'SELECT') return !/\binto\b/i.test(sql);
  if (kw === 'VALUES' || kw === 'TABLE') return true;
  if (kw === 'WITH') return !WRITES_IN_WITH.test(sql) && !/\binto\b/i.test(sql);
  return false;
}

export class Session {
  constructor() {
    this.pg = null;
    this.status = 'I'; // ReadyForQuery status: I idle, T in a transaction block, E failed
    this.readOnly = null; // default_transaction_read_only as last set
    this.convCache = new Map();
    this.version = null;
  }

  async open(dir, { startParams, wasmModule } = {}) {
    const opts = {};
    if (startParams) opts.startParams = startParams;
    if (wasmModule) opts.pgliteWasmModule = wasmModule;
    this.pg = new PGlite(dir, opts);
    await this.pg.waitReady;
  }

  async close() {
    if (this.pg && !this.pg.closed) await this.pg.close();
  }

  convOf(oid) {
    let c = this.convCache.get(oid);
    if (c === undefined) {
      c = converter(oid);
      this.convCache.set(oid, c);
    }
    return c;
  }

  /** Run raw protocol bytes; returns the backend messages. Keeps the
   * transaction status. */
  async raw(bytes) {
    const r = await this.pg.execProtocol(bytes, { throwOnError: false, syncToFs: false });
    const msgs = r.messages;
    for (let k = msgs.length - 1; k >= 0; k--) {
      if (msgs[k].name === 'readyForQuery') {
        this.status = msgs[k].status;
        break;
      }
    }
    return msgs;
  }

  /** A statement without parameters or results (BEGIN, SAVEPOINT, SET...),
   * by the simple query protocol. Throws PgError. */
  async simple(sql) {
    const msgs = await this.raw(S.query(sql));
    const err = msgs.find((m) => m.name === 'error');
    if (err) throw new PgError(err);
    return msgs;
  }

  /** Simple-protocol statements whose failure is not interesting (cleanup). */
  async quiet(sql) {
    try {
      await this.simple(sql);
    } catch {
      // nothing to do
    }
  }

  async setReadOnly(on) {
    if (this.readOnly === on || this.status !== 'I') return;
    await this.simple(`SET default_transaction_read_only = ${on ? 'on' : 'off'}`);
    this.readOnly = on;
  }

  static params(values) {
    const texts = [];
    const types = [];
    for (const v of values ?? []) {
      const [t, oid] = param(v);
      texts.push(t);
      types.push(oid);
    }
    return { texts, types };
  }

  /** Parse/Bind/Describe/Execute/Sync for one statement. `maxRows`: fetch
   * at most that many rows (Execute row limit) when the statement allows
   * it, else all and cut. Returns {columns, types, rows, truncated,
   * rowcount}. Throws PgError, ParamError, LimitError. `pre`/`post`:
   * simple-protocol statements sent in the same call (a savepoint). */
  async execute(sql, values, { maxRows = null, maxBytes = null, pre = null, post = null } = {}) {
    const { texts, types } = Session.params(values);
    const limit = maxRows !== null && limitable(sql) ? maxRows + 1 : 0;
    const parts = [];
    if (pre) parts.push(S.query(pre));
    parts.push(
      S.parse({ text: sql, types }),
      S.bind({ values: texts }),
      S.describe({ type: 'P' }),
      S.execute({ rows: limit }),
      S.sync(),
    );
    if (post) parts.push(S.query(post));
    const msgs = await this.raw(concat(parts));
    return this.collect(msgs, sql, maxRows, maxBytes, !!pre, !!post);
  }

  collect(msgs, sql, maxRows, maxBytes, hasPre, hasPost) {
    let fields = null;
    let convs = null;
    const rows = [];
    let truncated = false;
    let rowcount = -1;
    let bytes = 0;
    let section = hasPre ? 0 : 1; // 0: pre statement, 1: the statement, 2: post
    let err = null;
    let postErr = null;
    for (const m of msgs) {
      const name = m.name;
      if (name === 'readyForQuery') {
        section++;
        continue;
      }
      if (section !== 1) {
        if (name === 'error') {
          if (section === 0) {
            err = m;
          } else {
            postErr = m;
          }
        }
        continue;
      }
      switch (name) {
        case 'rowDescription':
          fields = m.fields;
          convs = fields.map((f) => this.convOf(f.dataTypeID));
          break;
        case 'dataRow': {
          if (maxRows !== null && rows.length >= maxRows) {
            truncated = true;
            break;
          }
          const fs = m.fields;
          const row = new Array(fs.length);
          for (let k = 0; k < fs.length; k++) {
            const t = fs[k];
            const c = convs[k];
            const v = t === null ? null : c === null ? t : c(t);
            row[k] = v;
            if (maxBytes !== null) bytes += valueBytes(v);
          }
          if (maxBytes !== null && bytes > maxBytes) throw new LimitError(`result exceeded max_result_bytes=${maxBytes}`);
          rows.push(row);
          break;
        }
        case 'portalSuspended':
          truncated = true;
          break;
        case 'commandComplete': {
          const verb = m.text.split(' ', 1)[0];
          if (DML.has(verb)) {
            const last = m.text.slice(m.text.lastIndexOf(' ') + 1);
            rowcount = /^\d+$/.test(last) ? Number(last) : -1;
          }
          break;
        }
        case 'error':
          if (!err) err = m;
          break;
        default:
      }
    }
    if (err) throw new PgError(err);
    if (postErr && hasPost) throw new PgError(postErr);
    const out = { columns: [], types: [], rows, truncated, rowcount };
    if (fields) {
      out.columns = fields.map((f) => f.name);
      out.types = fields.map((f) => TYPE_NAMES.get(f.dataTypeID) ?? null);
    }
    return out;
  }

  /** One statement for many parameter sets, in one call: one Parse per run
   * of sets with the same parameter types, a Bind/Execute per set, one
   * Sync (outside a transaction block, the whole call is one implicit
   * transaction: all or nothing). Returns the total row count. */
  async executeMany(sql, seq, { pre = null } = {}) {
    const parts = [];
    if (pre) parts.push(S.query(pre));
    let lastTypes = null;
    for (const values of seq) {
      const { texts, types } = Session.params(values);
      const key = types.join(',');
      if (key !== lastTypes) {
        parts.push(S.parse({ text: sql, types }));
        lastTypes = key;
      }
      parts.push(S.bind({ values: texts }), S.execute({ rows: 0 }));
    }
    parts.push(S.sync());
    const msgs = await this.raw(concat(parts));
    let total = 0;
    let section = pre ? 0 : 1;
    let any = false;
    for (const m of msgs) {
      if (m.name === 'readyForQuery') {
        section++;
        continue;
      }
      if (m.name === 'error') throw new PgError(m);
      if (section === 1 && m.name === 'commandComplete') {
        const verb = m.text.split(' ', 1)[0];
        if (DML.has(verb)) {
          any = true;
          const last = m.text.slice(m.text.lastIndexOf(' ') + 1);
          if (/^\d+$/.test(last)) total += Number(last);
        }
      }
    }
    return any ? total : -1;
  }

  /** COPY table (columns) FROM STDIN with rows already in text format. */
  async copyIn(statement, data) {
    const msgs = await this.raw(concat([S.query(statement), S.copyData(data), S.copyDone()]));
    let count = 0;
    for (const m of msgs) {
      if (m.name === 'error') throw new PgError(m);
      if (m.name === 'commandComplete') {
        const last = m.text.slice(m.text.lastIndexOf(' ') + 1);
        if (/^\d+$/.test(last)) count = Number(last);
      }
    }
    return count;
  }

  // -- portals (streams) ------------------------------------------------------------

  /** Bind `sql` to the named portal; the first chunk comes from fetch(). */
  async openPortal(name, sql, values) {
    const { texts, types } = Session.params(values);
    const msgs = await this.raw(concat([
      S.parse({ text: sql, types }),
      S.bind({ portal: name, values: texts }),
      S.describe({ type: 'P', name }),
      S.sync(),
    ]));
    const err = msgs.find((m) => m.name === 'error');
    if (err) throw new PgError(err);
    const desc = msgs.find((m) => m.name === 'rowDescription');
    const fields = desc ? desc.fields : [];
    return {
      name,
      convs: fields.map((f) => this.convOf(f.dataTypeID)),
      columns: fields.map((f) => f.name),
      types: fields.map((f) => TYPE_NAMES.get(f.dataTypeID) ?? null),
      done: !desc, // no rows to fetch (a statement without results)
      noData: !desc,
    };
  }

  /** Up to `n` rows from a portal; sets portal.done at its end. */
  async fetch(portal, n, maxBytes, spent) {
    const msgs = await this.raw(concat([S.execute({ portal: portal.name, rows: n }), S.sync()]));
    const rows = [];
    let bytes = spent;
    for (const m of msgs) {
      if (m.name === 'error') throw new PgError(m);
      if (m.name === 'dataRow') {
        const fs = m.fields;
        const row = new Array(fs.length);
        for (let k = 0; k < fs.length; k++) {
          const t = fs[k];
          const c = portal.convs[k];
          const v = t === null ? null : c === null ? t : c(t);
          row[k] = v;
          if (maxBytes !== null) bytes += valueBytes(v);
        }
        rows.push(row);
      } else if (m.name === 'commandComplete') portal.done = true;
    }
    if (maxBytes !== null && bytes > maxBytes) throw new LimitError(`result exceeded max_result_bytes=${maxBytes}`);
    return { rows, bytes };
  }

  async closePortal(portal) {
    try {
      await this.raw(concat([S.close({ type: 'P', name: portal.name }), S.sync()]));
    } catch {
      // the transaction is gone with it
    }
  }
}

export { ParamError };
