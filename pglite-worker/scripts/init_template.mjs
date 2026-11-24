// Create an empty PGlite cluster at the given directory and shut it down
// cleanly, for PGliteWorker(path, template=...) to copy instead of running
// initdb for each new database.
//
//   node pglite-worker/scripts/init_template.mjs DIR

import { PGlite } from '@electric-sql/pglite';

const dir = process.argv[2];
if (!dir) {
  console.error('usage: node pglite-worker/scripts/init_template.mjs DIR');
  process.exit(2);
}
const t0 = performance.now();
const pg = new PGlite(dir);
await pg.waitReady;
await pg.query('select 1');
await pg.close();
console.log(`initialised ${dir} in ${(performance.now() - t0).toFixed(0)} ms`);
