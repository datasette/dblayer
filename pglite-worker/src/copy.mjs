// Rows of wire values as COPY text format (tab-separated, \N for NULL),
// each value written as the text psycopg's COPY would send for it.

import { param } from './values.mjs';

const te = new TextEncoder();

function escape(s) {
  return s.replace(/[\\\t\n\r]/g, (c) => (c === '\\' ? '\\\\' : c === '\t' ? '\\t' : c === '\n' ? '\\n' : '\\r'));
}

export function copyText(rows) {
  const lines = [];
  for (const row of rows) {
    const cells = [];
    for (const v of row) {
      if (v === null || v === undefined) {
        cells.push('\\N');
        continue;
      }
      const [text] = param(v);
      cells.push(text === null ? '\\N' : escape(text));
    }
    lines.push(cells.join('\t'));
  }
  return te.encode(lines.length ? lines.join('\n') + '\n' : '');
}
