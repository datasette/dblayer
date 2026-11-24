// The schema, as dblayer.backends.sqlite._read_schema reads it and
// dblayer.worker.protocol.schema_to_wire shapes it (ported from
// cworker/src/schema.c). `q(sql, arg)` runs one query and returns its rows.

function columns(q, table) {
  return q('select cid, name, type, "notnull", dflt_value, pk, hidden from pragma_table_xinfo(?)', table).map((r) => {
    const hidden = Number(r[6]);
    return [r[1], r[2] ?? '', Number(r[3]) !== 0, r[4], r[5], hidden === 1, hidden === 2 || hidden === 3];
  });
}

function pks(q, table) {
  return q('select name, pk from pragma_table_info(?) where pk > 0 order by pk', table).map((r) => r[0]);
}

function foreignKeys(q, table) {
  const rows = q('select id, seq, "table", "from", "to", on_update, on_delete, match from pragma_foreign_key_list(?) order by id, seq', table);
  const out = [];
  for (let i = 0; i < rows.length; ) {
    let e = i;
    let anyNull = false;
    while (e < rows.length && rows[e][0] === rows[i][0]) {
      anyNull ||= rows[e][4] === null;
      e++;
    }
    const group = rows.slice(i, e);
    const other = group[0][2];
    let to = group.map((r) => r[4]);
    if (anyNull && other !== null) {
      const p = pks(q, other);
      if (p.length > 0) to = p; // references the other table's primary key
    }
    out.push([group.map((r) => r[3]), other, to, group[0][5], group[0][6]]);
    i = e;
  }
  return out;
}

function indexes(q, table, master) {
  const rows = q('select seq, name, "unique", origin, partial from pragma_index_list(?) order by seq', table);
  rows.sort((a, b) => ((a[1] ?? '') < (b[1] ?? '') ? -1 : (a[1] ?? '') > (b[1] ?? '') ? 1 : 0));
  return rows.map((r) => {
    const cols = q('select name from pragma_index_info(?) order by seqno', r[1]).map((c) => c[0]);
    let def = null;
    for (const m of master) if (m[0] === 'index' && m[1] === r[1]) def = m[3];
    return [r[1], cols, Number(r[2]) !== 0, r[3], Number(r[4]) !== 0, def];
  });
}

/** Python's (sql or "").lstrip().upper().startswith("CREATE VIRTUAL") */
const looksVirtual = (sql) => typeof sql === 'string' && sql.replace(/^[\s\x1c-\x1f]+/, '').toUpperCase().startsWith('CREATE VIRTUAL');

function table(q, m, isView, kinds, master) {
  const kind = kinds.get(m[1]) ?? 'table';
  return {
    name: m[1],
    kind: isView ? 'view' : 'table',
    columns: columns(q, m[1]),
    foreign_keys: isView ? [] : foreignKeys(q, m[1]),
    indexes: isView ? [] : indexes(q, m[1], master),
    definition: m[3],
    virtual: !isView && (kind === 'virtual' || looksVirtual(m[3])),
    hidden: !isView && kind === 'shadow',
    checks: [],
  };
}

export function readSchema(q) {
  const version = q('pragma schema_version')[0][0];
  const master = q('select type, name, tbl_name, sql from sqlite_master order by rowid').map((r) => [r[0] ?? '', r[1] ?? '', r[2], r[3]]);
  const kinds = new Map();
  try {
    for (const r of q("select name, type from pragma_table_list where schema = 'main'")) kinds.set(r[0] ?? '', r[1] ?? 'table');
  } catch {
    // SQLite < 3.37 has no pragma_table_list: every table is a table
  }
  const pick = (type) => master.filter((m) => m[0] === type && !m[1].startsWith('sqlite_'));
  return {
    version,
    tables: pick('table').map((m) => table(q, m, false, kinds, master)),
    views: pick('view').map((m) => table(q, m, true, kinds, master)),
    triggers: master.filter((m) => m[0] === 'trigger').map((m) => [m[1], m[2], m[3]]),
  };
}
