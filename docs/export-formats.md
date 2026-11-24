# Export formats

`db.export(sql, params, format=...)` streams the result of one read as
bytes in one of the formats below. A backend may produce those bytes itself
(`SupportsExport`, "native"); otherwise the core serialises rows from
`stream()` in Python ("fallback"). This document is the definition both
must meet. The formats follow what Datasette serves today (`.csv`, `.tsv`,
`.json` with `_shape=objects|arrays|array`, `_nl=on`) where Datasette has a
rule, and say where they deliberately differ.

The parity table at the end says, per backend and format, how closely the
native output meets this definition, and how that is tested.

## Common rules

- Output is UTF-8. A SQLite TEXT value that is not valid UTF-8 is an error
  (`QueryError`, "Could not decode to UTF-8 ..."), exactly as `execute()`
  and `stream()` raise it; bytes already sent stay sent (see "Errors").
- Columns come out in result order, with the names the engine reports.
- The **text form** of a value, used by CSV and TSV fields and by JSON for
  types JSON has no literal for:

| Value | Text form |
| --- | --- |
| NULL | (no text: see each format) |
| integer | decimal digits, any size (`-9223372036854775808`, `12345678901234567890`) |
| float | Python's `repr()`: the shortest digits that read back as the same double, `1.0` not `1`, exponent from `1e+16` up and below `0.0001` (`1e-05`), `-0.0` keeps its sign. Not finite: `NaN`, `Infinity`, `-Infinity` |
| text | the text itself |
| blob | standard base64 (RFC 4648 section 4: `+/`, `=` padding, no line breaks) |
| boolean (DuckDB, PostgreSQL) | `true` / `false` |
| date, time, timestamp, timestamp with zone | ISO 8601 as the engine writes it: `2024-01-02`, `03:04:05.5`, `2024-01-02 03:04:05` (space separator); see parity |
| decimal / numeric, UUID | the exact engine text (`1.50`, `12345678-1234-...`) |
| anything else (lists, structs, maps, intervals, JSON columns, ranges) | unspecified: engine-specific; never covered by parity |

## CSV (`format="csv"`, `text/csv; charset=utf-8`)

RFC 4180, matching Python's `csv` module with the `excel` dialect (what
Datasette uses), with one change: an empty string is quoted.

- Delimiter `,`, quote `"`, record terminator CRLF (`\r\n`), including
  after the last record.
- A field is quoted when it contains `,`, `"`, CR or LF, **or is the empty
  string**; a `"` inside a quoted field is doubled. Nothing else is quoted
  (not leading or trailing spaces, not numbers).
- NULL is an empty, unquoted field. So NULL (`a,,b`) and the empty string
  (`a,"",b`) are told apart, which Datasette's CSV cannot do today (its
  `csv.writer` writes both as an empty field). This is also PostgreSQL's
  own CSV rule.
- First record: the column names, quoted by the same rule (`header=False`
  leaves it out).
- Zero columns: an empty body (no header, no records).
- A one-column NULL row is an empty line (`\r\n`). Python's `csv.writer`
  writes `""` there; this definition does not, so that NULL stays
  distinguishable. Readers must not skip blank lines (`csv.reader` returns
  `[]` for one).
- Blobs are base64 text with no marker: a consumer that must tell a blob
  from a text that looks like base64 should ask for JSON, which marks
  blobs. (Datasette writes a URL to the blob instead; that needs the
  request, which a backend does not have.)

## TSV (`format="tsv"`, `text/tab-separated-values; charset=utf-8`)

Exactly CSV with a tab delimiter: Python's `excel-tab` dialect, which
Datasette uses for `.tsv`, plus the empty-string rule. A field is quoted
when it contains a tab, `"`, CR or LF, or is empty; commas are not special.
There is no backslash escaping (this is not PostgreSQL's `TEXT` COPY
format): values pasted into a spreadsheet come out unchanged.

## JSON (`format="json"`, `application/json; charset=utf-8`)

Strict JSON (RFC 8259): it must load with any parser, so values JSON
cannot hold are mapped:

| Value | JSON |
| --- | --- |
| NULL | `null` |
| integer | a number with every digit, also beyond 2^53. JavaScript's `JSON.parse` rounds those; Datasette does the same today |
| float | a number in the text form (`1.0`, `1e+16`); NaN and infinities are `null` (strict JSON forbids them; Datasette turns infinities into `null` and today emits bare `NaN`, which is invalid JSON) |
| text | a string: `"` and `\` escaped, `\n \r \t \b \f` as such, other characters below U+0020 as `\u00XX` (lower-case hex), everything else as raw UTF-8 (Python's `json.dumps(..., ensure_ascii=False)`) |
| blob | `{"$base64": true, "encoded": "<base64>"}`, as Datasette's `CustomJSONEncoder` (written without spaces) |
| boolean | `true` / `false` |
| date, time, timestamp, decimal, UUID | a string holding the text form |
| other types | unspecified (the engine's own JSON, or the fallback's `json` encoding of the Python value) |

Rows are objects keyed by column name or arrays, by shape. In an object,
a column name that repeats is written once, with its first value (as
`Row.as_dict()` does).

No insignificant whitespace inside a row (`{"id":1,"name":"x"}`). The byte
layout around rows is fixed so that a backend can write rows one at a time
without knowing which is last: a row is followed by `\n`, and every row
after the first is preceded by `,`.

| `json_shape` | Body | Datasette equivalent |
| --- | --- | --- |
| `"objects"` (default) | `{"ok":true,"columns":[...],"rows":[\n{...}\n,{...}\n],"truncated":false}\n` | `.json` (default `_shape=objects`) |
| `"arrays"` | the same with `[...]` rows | `?_shape=arrays` |
| `"array"` | `[\n{...}\n,{...}\n]\n` | `?_shape=array` |

## Newline-delimited JSON (`format="ndjson"`, `application/x-ndjson`)

One JSON object per row, each followed by `\n`, values as in JSON. No
columns, no truncation marker. (Datasette: `?_shape=array&_nl=on`, which
puts no `\n` after the last row; this format does, as JSON Lines asks.)

## Limits, truncation, errors

- `max_rows`: the export stops after that many rows; this is not an error.
  `stream.truncated` is then true. Only the `objects` and `arrays` JSON
  shapes carry it in the body (`"truncated":true`, written last). CSV,
  TSV, `array` and NDJSON have nowhere to put it without corrupting the
  data, so a caller that must tell the client has to do it out of band
  (the demo app logs it; an HTTP trailer would be the right place).
- `max_bytes`: a cap on the body. The export raises `ResourceLimitExceeded`
  before yielding the chunk that would pass it; at most `max_bytes` bytes
  are ever delivered (chunk granularity, so usually fewer).
- `time_limit_ms`: a budget of time spent waiting on the backend, summed
  over chunks, as for `stream()`; time the consumer spends between chunks
  does not count. Past it the export raises `QueryInterrupted`.
- An error after some bytes were delivered is raised from the iterator.
  The body is then incomplete, and that is the only signal: nothing is
  appended to it. An HTTP server must abort the response (no terminating
  chunk, or a short `Content-Length`), so the client sees a failed
  transfer instead of a short but plausible file.
- Writes are refused as for `execute()`: exports run on a read connection.

## Parity

To be filled in from the tests (`tests/test_export_parity.py`): for each
native backend and format, one of

- **identical**: the native bytes equal the fallback's bytes for the same
  rows, tested byte for byte;
- **parse-equal**: both parse (CSV/TSV with `csv.reader`, JSON with
  `json.loads`) to equal values after a type-aware comparison of cells
  (floats compared as floats, timestamps as parsed ISO 8601), tested that
  way;
- **fallback**: the backend does not produce this natively; the core
  serialises in Python.
