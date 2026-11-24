// Start-up options for a new PGlite data directory, timed in one process:
// initdb, copying a pre-initialised template directory, and PGlite's own
// data-dir tarball (loadDataDir); plus compiling pglite.wasm, and whether a
// compiled module can be serialised for a code cache.
//
//   node [--liftoff-only] pglite-worker/scripts/startup_options.mjs SCRATCH_DIR [ROUNDS]
//
// Used by benchmarks/pglite_bench.py; prints one JSON object.

import fs from 'node:fs';
import path from 'node:path';
import v8 from 'node:v8';
import { createRequire } from 'node:module';
import { PGlite } from '@electric-sql/pglite';

const dir = process.argv[2];
const rounds = parseInt(process.argv[3] ?? '3', 10);
if (!dir) {
  console.error('usage: node pglite-worker/scripts/startup_options.mjs SCRATCH_DIR [ROUNDS]');
  process.exit(2);
}
fs.mkdirSync(dir, { recursive: true });
const require = createRequire(import.meta.url);
const wasmPath = new URL('pglite.wasm', `file://${require.resolve('@electric-sql/pglite')}`);

const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const out = { rounds, liftoff_only: process.execArgv.includes('--liftoff-only') };

const bytes = fs.readFileSync(wasmPath);
const compiles = [];
let wasmModule;
for (let i = 0; i < rounds; i++) {
  const t0 = performance.now();
  wasmModule = await WebAssembly.compile(bytes);
  compiles.push(performance.now() - t0);
}
out.compile_ms = median(compiles);
try {
  const t0 = performance.now();
  const ser = v8.serialize(wasmModule);
  out.module_serialize = `${ser.length} bytes in ${(performance.now() - t0).toFixed(1)} ms`;
} catch (e) {
  out.module_serialize = `refused: ${e.message}`;
}

async function timeOpen(target, opts = {}) {
  const t0 = performance.now();
  const pg = new PGlite(target, { pgliteWasmModule: wasmModule, ...opts });
  await pg.waitReady;
  await pg.query('select 1');
  const ms = performance.now() - t0;
  await pg.close();
  return ms;
}

const initdb = [];
for (let i = 0; i < rounds; i++) initdb.push(await timeOpen(path.join(dir, `initdb${i}`)));
out.initdb_open_ms = median(initdb);

const template = path.join(dir, 'initdb0');
const copies = [];
const copyOnly = [];
for (let i = 0; i < rounds; i++) {
  const target = path.join(dir, `copy${i}`);
  const t0 = performance.now();
  fs.cpSync(template, target, { recursive: true });
  copyOnly.push(performance.now() - t0);
  copies.push(copyOnly[i] + (await timeOpen(target)));
}
out.template_copy_ms = median(copyOnly);
out.template_copy_open_ms = median(copies);

const reopen = [];
for (let i = 0; i < rounds; i++) reopen.push(await timeOpen(template));
out.reopen_existing_ms = median(reopen);

// PGlite's tarball: dump the template once, load it into fresh directories.
{
  const pg = new PGlite(template, { pgliteWasmModule: wasmModule });
  await pg.waitReady;
  const t0 = performance.now();
  const blob = await pg.dumpDataDir('none');
  out.dump_ms = performance.now() - t0;
  out.dump_bytes = blob.size;
  await pg.close();
  const loads = [];
  for (let i = 0; i < rounds; i++) {
    try {
      loads.push(await timeOpen(path.join(dir, `tar${i}`), { loadDataDir: blob }));
    } catch (e) {
      out.load_data_dir_error = `${e?.name}: ${e?.message}`;
      break;
    }
  }
  if (loads.length) out.load_data_dir_open_ms = median(loads);
}

out.rss_mb = Math.round(process.memoryUsage().rss / 1e6);
console.log(JSON.stringify(out));
