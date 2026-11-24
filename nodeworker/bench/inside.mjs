// SQLite inside Node, with no protocol: the WebAssembly build the worker
// uses against Node's built-in node:sqlite (native SQLite, not WASM), on
// the same in-memory work. Run by benchmarks/nodeworker_bench.py:
//
//   node nodeworker/bench/inside.mjs
//
// Prints one JSON line.

import { DatabaseSync } from 'node:sqlite';

const median = (xs) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const time = (fn, n) => {
  const out = [];
  for (let i = 0; i < n; i++) {
    const t0 = performance.now();
    fn();
    out.push(performance.now() - t0);
  }
  return median(out);
};
const SETUP = [
  'create table r (id integer primary key, title text, score real, body blob)',
  "insert into r (title, score, body) select 'title ' || i, i * 1.5, randomblob(16) from " +
    '(with recursive c(i) as (select 1 union all select i + 1 from c where i < 100000) select i from c)',
];

const out = {};

// node:sqlite (native SQLite inside Node)
{
  const t0 = performance.now();
  const db = new DatabaseSync(':memory:');
  out.native_open_ms = performance.now() - t0;
  for (const s of SETUP) db.exec(s);
  const one = db.prepare('select id, title from r where id = ?');
  out.native_row_us = time(() => one.get(7), 20000) * 1000;
  const all = db.prepare('select id, title, score, body from r');
  out.native_100k_ms = time(() => all.all(), 5);
  out.native_sqlite = db.prepare('select sqlite_version() v').get().v;
}

// the WebAssembly build (its oo1 API)
{
  const t0 = performance.now();
  const { default: init } = await import('@sqlite.org/sqlite-wasm');
  const sqlite3 = await init({ print: () => {}, printErr: () => {} });
  out.wasm_init_ms = performance.now() - t0;
  const db = new sqlite3.oo1.DB(':memory:');
  for (const s of SETUP) db.exec(s);
  const one = db.prepare('select id, title from r where id = ?');
  out.wasm_row_us = time(() => {
    one.bind([7]);
    one.step();
    one.get([]);
    one.reset();
  }, 20000) * 1000;
  out.wasm_oo1_100k_ms = time(() => db.exec({ sql: 'select id, title, score, body from r', rowMode: 'array', returnValue: 'resultRows' }), 5);
  // the raw exports, as the worker's executor reads rows: into JS values
  const X = sqlite3.wasm.exports;
  const td = new TextDecoder();
  const st = db.prepare('select id, title, score, body from r');
  const p = st.pointer;
  out.wasm_100k_ms = time(() => {
    const rows = [];
    while (X.sqlite3_step(p) === 100) {
      const h = sqlite3.wasm.heap8u();
      const t = X.sqlite3_column_text(p, 1);
      const b = X.sqlite3_column_blob(p, 3);
      rows.push([
        Number(X.sqlite3_column_int64(p, 0)),
        td.decode(h.subarray(t, t + X.sqlite3_column_bytes(p, 1))),
        X.sqlite3_column_double(p, 2),
        h.slice(b, b + X.sqlite3_column_bytes(p, 3)),
      ]);
    }
    X.sqlite3_reset(p);
  }, 5);
  out.wasm_sqlite = sqlite3.capi.sqlite3_libversion();
}

console.log(JSON.stringify(out));
