// Where an executor thread's memory goes: V8 heap of a thread that has
// loaded and initialised the SQLite module, against an empty thread.
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import v8 from 'node:v8';

if (isMainThread) {
  for (const load of [false, true]) {
    const w = new Worker(new URL(import.meta.url), { workerData: { load } });
    const m = await new Promise((r) => w.once('message', r));
    console.log(JSON.stringify({ load, ...m }));
    await w.terminate();
  }
} else {
  if (workerData.load) {
    const { default: init } = await import('@sqlite.org/sqlite-wasm');
    const s = await init({ print: () => {}, printErr: () => {} });
    globalThis.keep = s;
  }
  globalThis.gc?.();
  const h = v8.getHeapStatistics();
  const mb = (x) => +(x / 2 ** 20).toFixed(1);
  parentPort.postMessage({
    heap_used_mb: mb(h.used_heap_size),
    heap_total_mb: mb(h.total_heap_size),
    external_mb: mb(h.external_memory),
    malloced_mb: mb(h.malloced_memory),
    code_mb: mb(v8.getHeapCodeStatistics().bytecode_and_metadata_size + v8.getHeapCodeStatistics().code_and_metadata_size),
  });
}
