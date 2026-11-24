# Export benchmark results (2026-11-24, provisional)

Produced by `benchmarks/export.py` (raw rows in `export-results.jsonl`) and
`benchmarks/export.py --http` (`export-http-results.tsv`). Apple Silicon,
18 cores, macOS; Python 3.15; SQLite 3.53.1 in the C worker; DuckDB
Python package; PostgreSQL 18.3 on the local socket with psycopg 3.3.9
(binary). **Seven other agents were working on this machine: the load
average was 18 to 30 through every run** (the `load1` column), so the
numbers are provisional, good to perhaps ±30%, and the small-size rows
are noisier still. Re-run alone before quoting them.

How to read them:

- **Python CPU** is user + system CPU of the process that would be the web
  server, during the export only. For the C worker, the worker's own CPU
  is separate (`worker CPU`). For in-process DuckDB the engine's threads
  are *in* the Python process, so its Python CPU includes DuckDB's own
  work (and can exceed wall time). PostgreSQL's server CPU is not
  measured: native export moves work into the PostgreSQL backend process.
- **Event-loop lag**: a coroutine sleeping 1 ms, how late it woke (max,
  p99). Noise from the loaded machine is about 2 to 10 ms.
- Bytes are consumed as an HTTP server would forward them (counted and
  dropped). `datasette` is what Datasette does today: CSV through
  `csv.writer` per page of rows, JSON built as one `json.dumps` of the
  whole response.
- Tables: `wide` is 7 mixed columns (integer key, nullable integer,
  float, short text, ~110-character text with quotes and commas, nullable
  text with empty strings, timestamp), 174 bytes per CSV row; `narrow` is
  four integers, 26 bytes per CSV row.

## Headline: 1,000,000 rows

Python-process CPU in seconds (lower is better), with the multiple of the
Python fallback's cost in brackets.

| | wide CSV | wide JSON | narrow CSV | narrow JSON |
| --- | ---: | ---: | ---: | ---: |
| C worker rows, Python fallback | 2.03 | 1.61 | 0.67 | 0.61 |
| C worker rows, Datasette-style | 2.05 | 1.72 | | |
| C worker, codec frames | 0.15 (13x less) | 0.20 (8x) | | |
| C worker, raw frames | 0.15 (14x) | 0.17 (10x) | 0.03 (23x) | 0.04 (15x) |
| C worker, fifo side channel | 0.47 (4x) | 0.55 (3x) | | |
| C worker, socket handoff | 0.06 (36x) | 0.05 (32x) | | |
| DuckDB, Python fallback (incl. engine) | 2.77 | 2.67 | 0.85 | 0.83 |
| DuckDB native (incl. engine) | 0.69 (4x) | 0.66 (4x) | 0.32 (2.7x) | 0.34 (2.5x) |
| PostgreSQL, Python fallback | 3.30 | 2.64 | 0.84 | 0.86 |
| PostgreSQL native (COPY) | 1.18 (2.8x) | 1.36 (1.9x) | 0.80 (1.05x) | 0.82 (1.05x) |

Event-loop lag stayed at noise level (max under 10 ms, p99 under 6 ms) for
every chunked mode, native or fallback, because both yield between
chunks. **The lag that matters is Datasette-style JSON**: one `json.dumps`
of the whole response blocked the loop for 730 ms (C worker rows),
1,194 ms (DuckDB) and 1,258 ms (PostgreSQL) at 1,000,000 rows, and grew
the process by 1.2 to 1.4 GB. Every `db.export()` mode grew it by under
7 MB (DuckDB in-process: about 275 MB, DuckDB's own buffers, the same
with or without native export).

Wall time at 1,000,000 wide rows: C worker native 1.6 to 2.0 s against
2.9 to 3.9 s fallback; DuckDB native 0.4 s against 2.8 s; PostgreSQL
native 2.2 to 2.7 s against 5.4 to 5.8 s.

Per row, Python CPU: the fallback costs about 2 µs per wide row and
0.6 µs per narrow row; forwarding raw frames costs 0.15 µs per wide row
(about 0.9 ms per MB) and 0.03 µs per narrow row; the socket handoff
costs 15 ms at 100,000 rows and 50 to 60 ms at 1,000,000 (a listener, an
accept, two control frames, and an idle wait of ~1.7 s on the control
pipe; where the 50 ms goes was not profiled).

**Why PostgreSQL native does not win on narrow rows:** psycopg's
`Copy.read()` returns one row per `await`, so Python still does per-row
work (0.8 µs per row: an await, a memoryview, a CRLF rewrite). Draining
libpq directly with `PQgetCopyData(async=1)` through `psycopg.pq`, waiting
on the socket only when it would block, cuts that 4x (0.65 s to 0.17 s
for 1,000,000 narrow rows; 0.88 s to 0.42 s wide):
`scripts/psycopg_copy_batch.py`. Not yet used by the backend.

## End to end over HTTP, 1,000,000 wide rows, `curl`

`benchmarks/export.py --http`: one `curl -o /dev/null` per row, best of 3
(2 for the demo); server CPU from `ps -o cputime` (10 ms resolution, so
0.00 means under 10 ms); the C worker's CPU is not included. 174 MB CSV,
240 MB JSON.

| Server | Mode | Format | wall s | server CPU s | MB/s | load1 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| handoff_server (asyncio) | Python fallback | CSV | 4.40 | 2.16 | 40 | 21.7 |
| | native, codec frames | CSV | 2.10 | 0.22 | 83 | 26.5 |
| | native, raw frames | CSV | 1.94 | 0.21 | 90 | 26.1 |
| | native, fifo | CSV | 2.18 | 0.70 | 80 | 24.7 |
| | **socket handoff** | CSV | 1.50 | **0.00** | 117 | 25.5 |
| | Python fallback | JSON | 2.95 | 1.58 | 81 | 24.7 |
| | native, codec frames | JSON | 1.70 | 0.22 | 141 | 24.3 |
| | native, raw frames | JSON | 1.93 | 0.24 | 124 | 22.0 |
| | native, fifo | JSON | 2.48 | 0.98 | 97 | 21.7 |
| | **socket handoff** | JSON | 1.68 | **0.01** | 142 | 21.9 |
| demo app (Starlette, uvicorn) | C worker native (raw) | CSV | 1.79 | 0.24 | 97 | 21.7 |
| | C worker fallback | CSV | 2.89 | 2.19 | 60 | 20.4 |
| | C worker native (raw) | JSON | 1.85 | 0.28 | 130 | 20.4 |
| | C worker fallback | JSON | 2.24 | 1.53 | 107 | 19.6 |
| | DuckDB native | CSV | 0.55 | 0.71 | 314 | 18.8 |
| | DuckDB fallback | CSV | 2.68 | 2.69 | 65 | 18.0 |
| | DuckDB native | JSON | 0.50 | 0.66 | 476 | 18.0 |
| | DuckDB fallback | JSON | 2.27 | 2.11 | 105 | 19.6 |
| | PostgreSQL native | CSV | 6.20 | 1.69 | 28 | 27.2 |
| | PostgreSQL fallback | CSV | 3.95 | 3.47 | 44 | 25.3 |
| | PostgreSQL native | JSON | 2.52 | 1.72 | 95 | 25.4 |
| | PostgreSQL fallback | JSON | 3.10 | 2.73 | 77 | 24.2 |

uvicorn adds little: 0.24 s of server CPU for 174 MB forwarded, against
0.21 s in the bare asyncio server. The PostgreSQL native CSV wall time
(6.2 s, slower than the fallback's 3.9 s while using half the Python
CPU) is unexplained: a single sample on a loaded machine, and the COPY
projection (float re-layout, base64 in SQL) runs in the PostgreSQL
server process, whose CPU this does not measure.

## All in-process runs

Generated from `export-results.jsonl`.

#### wide, 10,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.005 | 2.9 / 2.9 | 0.018 | 95 | 0.016 | 0.8 | 19.1 |
| C worker, codec frames | 0.005 | 3.2 / 3.2 | 0.017 | 102 | 0.015 | 0.9 | 19.1 |
| C worker, fifo side channel | 0.010 | 6.0 / 6.0 | 0.023 | 74 | 0.015 | 0.1 | 19.1 |
| C worker rows, Python fallback | 0.022 | 4.1 / 4.1 | 0.027 | 62 | 0.008 | 4.3 | 19.1 |
| C worker rows, Datasette-style | 0.022 | 4.9 / 4.9 | 0.026 | 64 | 0.008 | 3.1 | 19.1 |
| C worker, socket handoff | 0.008 | 10.2 / 10.2 | 0.026 | 64 | 0.016 | 0.1 | 19.1 |
| DuckDB native | 0.019 | 1.6 / 1.6 | 0.024 | 70 | - | 16.4 | 19.8 |
| DuckDB, Python fallback | 0.031 | 2.2 / 2.2 | 0.032 | 53 | - | 8.0 | 19.8 |
| DuckDB, Datasette-style | 0.030 | 1.9 / 1.9 | 0.031 | 55 | - | 7.9 | 19.8 |
| PostgreSQL native (COPY) | 0.012 | 1.9 / 1.9 | 0.015 | 115 | - | 0.3 | 18.8 |
| PostgreSQL, Python fallback | 0.033 | 3.5 / 3.5 | 0.037 | 46 | - | 2.9 | 18.8 |
| PostgreSQL, Datasette-style | 0.036 | 4.2 / 4.2 | 0.066 | 25 | - | 3.4 | 18.8 |

#### wide, 10,000 rows, TSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.006 | 7.0 / 7.0 | 0.034 | 50 | 0.015 | 1.0 | 19.1 |
| C worker rows, Python fallback | 0.025 | 6.4 / 6.4 | 0.046 | 37 | 0.009 | 4.2 | 19.1 |
| C worker rows, Datasette-style | 0.026 | 8.7 / 8.7 | 0.059 | 29 | 0.009 | 4.6 | 19.1 |
| DuckDB native | 0.016 | 2.1 / 2.1 | 0.016 | 104 | - | 15.9 | 19.8 |
| DuckDB, Python fallback | 0.034 | 2.5 / 2.5 | 0.034 | 50 | - | 8.3 | 19.8 |
| DuckDB, Datasette-style | 0.030 | 2.2 / 2.2 | 0.030 | 56 | - | 8.3 | 19.8 |
| PostgreSQL native (COPY) | 0.022 | 18.6 / 18.6 | 0.114 | 15 | - | 0.3 | 18.8 |
| PostgreSQL, Python fallback | 0.039 | 5.1 / 5.1 | 0.085 | 20 | - | 3.3 | 18.8 |
| PostgreSQL, Datasette-style | 0.038 | 3.4 / 3.4 | 0.082 | 21 | - | 3.4 | 18.8 |

#### wide, 10,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.007 | 4.3 / 4.3 | 0.050 | 47 | 0.016 | 1.1 | 19.1 |
| C worker, codec frames | 0.008 | 4.0 / 4.0 | 0.052 | 45 | 0.017 | 1.1 | 19.1 |
| C worker, fifo side channel | 0.020 | 4.5 / 4.5 | 0.130 | 18 | 0.018 | 0.1 | 19.1 |
| C worker rows, Python fallback | 0.017 | 5.9 / 5.9 | 0.042 | 56 | 0.008 | 3.1 | 19.1 |
| C worker rows, Datasette-style | 0.021 | 8.1 / 8.1 | 0.044 | 56 | 0.009 | 14.1 | 19.1 |
| C worker, socket handoff | 0.010 | 11.0 / 11.0 | 0.032 | 73 | 0.018 | 0.1 | 19.8 |
| DuckDB native | 0.019 | 2.8 / 2.8 | 0.021 | 111 | - | 20.5 | 19.8 |
| DuckDB, Python fallback | 0.026 | 1.6 / 1.6 | 0.027 | 86 | - | 6.5 | 19.8 |
| DuckDB, Datasette-style | 0.025 | 10.4 / 10.4 | 0.025 | 98 | - | 14.3 | 19.8 |
| PostgreSQL native (COPY) | 0.024 | 4.4 / 4.4 | 0.076 | 31 | - | 0.3 | 18.8 |
| PostgreSQL, Python fallback | 0.031 | 2.4 / 2.4 | 0.041 | 57 | - | 2.0 | 18.8 |
| PostgreSQL, Datasette-style | 0.032 | 13.6 / 13.6 | 0.044 | 56 | - | 14.0 | 18.8 |

#### wide, 10,000 rows, NDJSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.006 | 3.5 / 3.5 | 0.020 | 119 | 0.016 | 1.2 | 19.8 |
| C worker rows, Python fallback | 0.017 | 2.9 / 2.9 | 0.021 | 111 | 0.008 | 3.3 | 19.8 |
| C worker rows, Datasette-style | 0.030 | 18.8 / 18.8 | 0.035 | 70 | 0.008 | 13.6 | 19.8 |
| DuckDB native | 0.016 | 1.8 / 1.8 | 0.016 | 145 | - | 21.7 | 19.8 |
| DuckDB, Python fallback | 0.023 | 2.1 / 2.1 | 0.024 | 98 | - | 6.8 | 19.8 |
| DuckDB, Datasette-style | 0.035 | 22.9 / 22.9 | 0.037 | 66 | - | 16.4 | 19.8 |
| PostgreSQL native (COPY) | 0.019 | 2.2 / 2.2 | 0.036 | 65 | - | 0.4 | 18.8 |
| PostgreSQL, Python fallback | 0.033 | 9.3 / 9.3 | 0.123 | 19 | - | 2.3 | 18.8 |
| PostgreSQL, Datasette-style | 0.045 | 29.4 / 29.4 | 0.083 | 30 | - | 13.5 | 18.8 |

#### wide, 100,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.027 | 3.0 / 1.1 | 0.179 | 96 | 0.145 | 1.8 | 20.1 |
| C worker, codec frames | 0.026 | 3.1 / 0.5 | 0.181 | 95 | 0.150 | 1.0 | 20.1 |
| C worker, fifo side channel | 0.069 | 5.4 / 0.4 | 0.234 | 73 | 0.160 | 0.1 | 20.1 |
| C worker rows, Python fallback | 0.223 | 4.7 / 4.7 | 0.388 | 44 | 0.088 | 4.4 | 20.1 |
| C worker rows, Datasette-style | 0.216 | 5.7 / 5.7 | 0.302 | 57 | 0.086 | 4.6 | 20.1 |
| C worker, socket handoff | 0.015 | 8.3 / 4.1 | 0.245 | 70 | 0.159 | 0.1 | 20.1 |
| DuckDB native | 0.091 | 2.3 / 2.3 | 0.083 | 207 | - | 55.5 | 20.9 |
| DuckDB, Python fallback | 0.349 | 10.8 / 6.0 | 0.425 | 40 | - | 43.5 | 20.9 |
| DuckDB, Datasette-style | 0.314 | 5.6 / 5.5 | 0.369 | 46 | - | 42.5 | 20.9 |
| PostgreSQL native (COPY) | 0.097 | 1.4 / 0.3 | 0.133 | 129 | - | 0.4 | 21.0 |
| PostgreSQL, Python fallback | 0.341 | 3.2 / 3.2 | 0.429 | 40 | - | 4.6 | 21.0 |
| PostgreSQL, Datasette-style | 0.282 | 2.7 / 2.6 | 0.355 | 48 | - | 3.3 | 21.0 |

#### wide, 100,000 rows, TSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.029 | 3.3 / 3.0 | 0.327 | 52 | 0.174 | 1.5 | 20.1 |
| C worker rows, Python fallback | 0.220 | 5.8 / 5.8 | 0.302 | 57 | 0.079 | 6.1 | 20.1 |
| C worker rows, Datasette-style | 0.217 | 8.6 / 6.4 | 0.334 | 51 | 0.089 | 5.9 | 19.4 |
| DuckDB native | 0.089 | 2.8 / 2.8 | 0.073 | 234 | - | 51.6 | 20.9 |
| DuckDB, Python fallback | 0.336 | 8.8 / 4.3 | 0.371 | 46 | - | 42.1 | 20.9 |
| DuckDB, Datasette-style | 0.307 | 5.3 / 5.0 | 0.344 | 50 | - | 43.0 | 20.9 |
| PostgreSQL native (COPY) | 0.104 | 1.7 / 1.4 | 0.153 | 112 | - | 0.3 | 21.0 |
| PostgreSQL, Python fallback | 0.309 | 3.6 / 2.9 | 0.379 | 45 | - | 4.6 | 21.0 |
| PostgreSQL, Datasette-style | 0.290 | 2.8 / 2.7 | 0.365 | 47 | - | 3.9 | 20.8 |

#### wide, 100,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.029 | 3.2 / 0.4 | 0.194 | 122 | 0.161 | 1.6 | 19.4 |
| C worker, codec frames | 0.031 | 3.2 / 0.4 | 0.207 | 114 | 0.167 | 1.4 | 19.4 |
| C worker, fifo side channel | 0.077 | 2.4 / 0.4 | 0.255 | 93 | 0.179 | 0.2 | 19.4 |
| C worker rows, Python fallback | 0.158 | 3.2 / 3.2 | 0.251 | 94 | 0.079 | 4.1 | 19.4 |
| C worker rows, Datasette-style | 0.167 | 81.1 / 81.1 | 0.255 | 98 | 0.088 | 120.8 | 19.4 |
| C worker, socket handoff | 0.014 | 7.8 / 0.5 | 0.201 | 118 | 0.168 | 0.1 | 19.4 |
| DuckDB native | 0.092 | 1.5 / 1.5 | 0.072 | 328 | - | 60.2 | 20.9 |
| DuckDB, Python fallback | 0.257 | 2.6 / 2.5 | 0.269 | 88 | - | 40.9 | 20.9 |
| DuckDB, Datasette-style | 0.248 | 126.8 / 126.8 | 0.260 | 96 | - | 152.9 | 20.9 |
| PostgreSQL native (COPY) | 0.145 | 2.0 / 0.3 | 0.234 | 101 | - | 0.5 | 20.8 |
| PostgreSQL, Python fallback | 0.259 | 2.5 / 2.4 | 0.330 | 72 | - | 2.7 | 20.8 |
| PostgreSQL, Datasette-style | 0.248 | 112.3 / 1.9 | 0.303 | 82 | - | 118.4 | 20.8 |

#### wide, 100,000 rows, NDJSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.032 | 3.2 / 0.5 | 0.224 | 105 | 0.170 | 1.6 | 19.4 |
| C worker rows, Python fallback | 0.164 | 3.2 / 3.2 | 0.265 | 89 | 0.101 | 4.0 | 19.4 |
| C worker rows, Datasette-style | 0.309 | 210.7 / 3.0 | 0.427 | 58 | 0.093 | 127.0 | 19.4 |
| DuckDB native | 0.088 | 2.1 / 2.1 | 0.068 | 346 | - | 56.7 | 20.9 |
| DuckDB, Python fallback | 0.250 | 2.4 / 2.3 | 0.259 | 91 | - | 40.6 | 21.0 |
| DuckDB, Datasette-style | 0.316 | 222.0 / 222.0 | 0.327 | 76 | - | 160.5 | 21.0 |
| PostgreSQL native (COPY) | 0.146 | 1.3 / 0.4 | 0.232 | 102 | - | 0.5 | 20.8 |
| PostgreSQL, Python fallback | 0.259 | 2.5 / 2.4 | 0.322 | 73 | - | 2.1 | 20.8 |
| PostgreSQL, Datasette-style | 0.383 | 235.6 / 2.0 | 0.476 | 52 | - | 124.8 | 20.8 |

#### wide, 1,000,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.147 | 3.1 / 0.4 | 1.710 | 102 | 1.475 | 2.0 | 24.0 |
| C worker, codec frames | 0.154 | 2.6 / 0.3 | 1.582 | 110 | 1.418 | 1.6 | 22.9 |
| C worker, fifo side channel | 0.473 | 3.0 / 0.4 | 1.962 | 89 | 1.526 | 0.3 | 22.9 |
| C worker rows, Python fallback | 2.029 | 5.3 / 4.5 | 2.920 | 60 | 0.832 | 6.2 | 21.9 |
| C worker rows, Datasette-style | 2.045 | 6.6 / 4.4 | 3.093 | 56 | 0.848 | 4.9 | 21.9 |
| C worker, socket handoff | 0.057 | 9.0 / 0.4 | 1.862 | 94 | 1.435 | 0.1 | 21.4 |
| DuckDB native | 0.687 | 1.2 / 0.5 | 0.406 | 429 | - | 275.4 | 20.9 |
| DuckDB, Python fallback | 2.765 | 8.7 / 3.7 | 2.842 | 61 | - | 249.8 | 19.9 |
| DuckDB, Datasette-style | 2.654 | 9.2 / 4.8 | 3.004 | 58 | - | 249.8 | 21.1 |
| PostgreSQL native (COPY) | 1.177 | 1.2 / 0.3 | 2.189 | 80 | - | 0.6 | 27.6 |
| PostgreSQL, Python fallback | 3.301 | 3.5 / 3.0 | 5.381 | 32 | - | 5.8 | 27.6 |
| PostgreSQL, Datasette-style | 2.760 | 3.2 / 2.6 | 4.601 | 38 | - | 4.3 | 26.5 |

#### wide, 1,000,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.168 | 1.3 / 0.3 | 1.603 | 150 | 1.427 | 1.9 | 21.4 |
| C worker, codec frames | 0.203 | 2.8 / 0.3 | 1.999 | 120 | 1.473 | 1.6 | 21.4 |
| C worker, fifo side channel | 0.553 | 7.4 / 0.3 | 2.063 | 116 | 1.566 | 0.3 | 21.2 |
| C worker rows, Python fallback | 1.610 | 22.6 / 5.5 | 3.902 | 61 | 1.054 | 4.5 | 21.8 |
| C worker rows, Datasette-style | 1.723 | 730.3 / 2.1 | 2.925 | 86 | 0.830 | 1185.0 | 21.8 |
| C worker, socket handoff | 0.051 | 9.5 / 0.4 | 1.762 | 136 | 1.548 | 0.3 | 20.9 |
| DuckDB native | 0.664 | 0.7 / 0.6 | 0.365 | 656 | - | 274.5 | 21.1 |
| DuckDB, Python fallback | 2.669 | 216.0 / 101.1 | 10.841 | 22 | - | 249.9 | 29.9 |
| DuckDB, Datasette-style | 2.390 | 1194.4 / 10.4 | 2.427 | 104 | - | 1410.2 | 29.9 |
| PostgreSQL native (COPY) | 1.356 | 2.0 / 0.3 | 2.688 | 89 | - | 0.7 | 26.5 |
| PostgreSQL, Python fallback | 2.644 | 3.0 / 2.3 | 5.772 | 42 | - | 3.6 | 25.4 |
| PostgreSQL, Datasette-style | 2.947 | 1258.3 / 1.5 | 5.541 | 46 | - | 1170.1 | 24.2 |

#### narrow, 10,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.004 | 3.1 / 3.1 | 0.006 | 34 | 0.005 | 0.5 | 19.8 |
| C worker rows, Python fallback | 0.008 | 1.0 / 1.0 | 0.010 | 22 | 0.005 | 0.5 | 19.8 |
| DuckDB native | 0.008 | 1.9 / 1.9 | 0.009 | 25 | - | 3.9 | 19.8 |
| DuckDB, Python fallback | 0.009 | 0.6 / 0.6 | 0.009 | 22 | - | 2.6 | 19.8 |
| PostgreSQL native (COPY) | 0.012 | 2.0 / 2.0 | 0.020 | 11 | - | 0.3 | 20.1 |
| PostgreSQL, Python fallback | 0.009 | 0.8 / 0.8 | 0.012 | 18 | - | 0.6 | 20.1 |

#### narrow, 10,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.004 | 3.3 / 3.3 | 0.006 | 64 | 0.006 | 0.4 | 19.8 |
| C worker rows, Python fallback | 0.008 | 0.7 / 0.7 | 0.010 | 41 | 0.005 | 0.4 | 19.8 |
| DuckDB native | 0.009 | 2.2 / 2.2 | 0.009 | 43 | - | 4.7 | 19.8 |
| DuckDB, Python fallback | 0.010 | 0.7 / 0.7 | 0.010 | 40 | - | 2.7 | 18.8 |
| PostgreSQL native (COPY) | 0.011 | 2.2 / 2.2 | 0.013 | 30 | - | 0.4 | 20.1 |
| PostgreSQL, Python fallback | 0.010 | 1.1 / 1.1 | 0.013 | 31 | - | 0.6 | 20.1 |

#### narrow, 100,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.008 | 3.3 / 3.3 | 0.058 | 42 | 0.034 | 1.0 | 19.4 |
| C worker rows, Python fallback | 0.082 | 2.8 / 2.8 | 0.131 | 18 | 0.034 | 1.0 | 19.4 |
| DuckDB native | 0.036 | 2.0 / 2.0 | 0.037 | 65 | - | 12.4 | 21.0 |
| DuckDB, Python fallback | 0.072 | 0.8 / 0.8 | 0.077 | 31 | - | 9.1 | 21.0 |
| PostgreSQL native (COPY) | 0.092 | 6.5 / 6.5 | 0.130 | 19 | - | 0.4 | 20.8 |
| PostgreSQL, Python fallback | 0.090 | 2.0 / 1.1 | 0.130 | 19 | - | 0.9 | 20.8 |

#### narrow, 100,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.010 | 2.9 / 2.9 | 0.045 | 93 | 0.037 | 1.4 | 19.4 |
| C worker rows, Python fallback | 0.087 | 4.4 / 4.4 | 0.150 | 28 | 0.036 | 0.8 | 19.4 |
| DuckDB native | 0.039 | 2.3 / 2.3 | 0.042 | 100 | - | 16.1 | 21.0 |
| DuckDB, Python fallback | 0.080 | 1.0 / 1.0 | 0.088 | 48 | - | 9.0 | 21.0 |
| PostgreSQL native (COPY) | 0.088 | 2.1 / 2.0 | 0.136 | 31 | - | 0.5 | 26.5 |
| PostgreSQL, Python fallback | 0.094 | 1.2 / 0.9 | 0.136 | 31 | - | 0.9 | 26.5 |

#### narrow, 1,000,000 rows, CSV

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.029 | 3.1 / 0.4 | 0.278 | 98 | 0.248 | 1.8 | 20.9 |
| C worker rows, Python fallback | 0.665 | 1.5 / 1.0 | 0.881 | 31 | 0.243 | 1.1 | 20.9 |
| DuckDB native | 0.318 | 4.8 / 1.5 | 0.210 | 129 | - | 50.2 | 29.0 |
| DuckDB, Python fallback | 0.846 | 6.3 / 2.8 | 1.215 | 22 | - | 45.7 | 29.0 |
| PostgreSQL native (COPY) | 0.799 | 2.2 / 0.5 | 1.355 | 20 | - | 0.3 | 24.2 |
| PostgreSQL, Python fallback | 0.840 | 1.1 / 0.9 | 1.987 | 14 | - | 1.1 | 24.0 |

#### narrow, 1,000,000 rows, JSON

| Mode | Python CPU s | loop lag max / p99 ms | wall s | MB/s | worker CPU s | peak RSS growth MB | load1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C worker, raw frames | 0.040 | 2.2 / 0.4 | 0.288 | 156 | 0.251 | 1.5 | 20.9 |
| C worker rows, Python fallback | 0.614 | 1.0 / 0.8 | 0.825 | 55 | 0.237 | 1.1 | 20.9 |
| DuckDB native | 0.338 | 1.9 / 0.7 | 0.207 | 218 | - | 51.5 | 29.0 |
| DuckDB, Python fallback | 0.831 | 4.0 / 1.8 | 0.947 | 48 | - | 45.9 | 29.0 |
| PostgreSQL native (COPY) | 0.819 | 1.9 / 0.4 | 1.924 | 23 | - | 0.5 | 24.0 |
| PostgreSQL, Python fallback | 0.857 | 1.1 / 0.9 | 1.925 | 23 | - | 1.3 | 24.0 |
