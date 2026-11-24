// SQLSTATE -> dblayer exception class: the table of map_sqlstate() in
// dblayer/backends/_postgres_common.py (tests/worker/test_pglite_worker.py
// checks that the two agree).

export function errorType(state) {
  if (state === '57014') return 'QueryInterrupted';
  if (state === '40001' || state === '40P01') return 'WriteConflict';
  if (state === '25P03' || state === '25P04') return 'TransactionExpired';
  if (state.startsWith('23')) return 'IntegrityError';
  if (state.startsWith('53') || state.startsWith('54')) return 'ResourceLimitExceeded';
  return 'QueryError';
}
