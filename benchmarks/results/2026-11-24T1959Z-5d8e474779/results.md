# dblayer benchmarks: 2026-11-24T1959Z-5d8e474779

**FULL RUN at 5d8e474 on Fly.io performance-8x (sjc), nothing else running**

- Mode: full; started 2026-11-24T19:59:13+00:00
- Commit: `5d8e4747797780acac528327fe6939705bb8632e` (5d8e474 2026-11-24T11:48:09-08:00 Full-run coverage: loadgen (non-Python driver) and memory-limit topics, DuckDB workers and PGlite in scale_wake_query, results.md after every topic)
- Machine: AMD EPYC, 8 CPUs, 15.6 GB, kernel 6.12.118-fly, x86_64, Fly performance-8x in sjc
- Data filesystem: `/dev/vdc ext4 rw,relatime`; temporary files: `/dev/vdc ext4`
- Versions: python 3.14.8; sqlite (python sqlite3) 3.53.1; uv uv 0.13.0 (x86_64-unknown-linux-gnu); node v24.22.3; rustc rustc 1.100.0 (0ef1bc386 2026-11-09); cargo cargo 1.100.0 (3acc79c0c 2026-11-05); cc cc (Debian 14.2.0-19) 14.2.0; postgres (psql client) psql (PostgreSQL) 18.7 (Debian 18.7-1.pgdg13+5); duckdb 2.0.1; asyncpg 0.32.0; psycopg 3.3.9; psycopg-binary 3.3.9; apsw 3.54.1.0; wasmtime 51.0.0; chdb 4.5.1; msgpack 1.2.4; pytest 9.2.0; starlette 1.10.0; uvicorn 0.54.0; postgres server 18.7 (Debian 18.7-1.pgdg13+5); libpq 180006; sqlite (C worker) dblayer-cworker protocol 1, SQLite 3.53.1; sqlite-rsworker dblayer-sqlite-rsworker protocol 1, SQLite 3.53.1 (-O2 -g -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_DQS=3); duckdb-rsworker dblayer-duckdb-rsworker protocol 1, DuckDB v2.0.1; duckdb-cppworker dblayer-duckdb-worker protocol 1, DuckDB v2.0.1

Every topic ran alone, one after another, in this one session. Compare figures within this file only.

| topic | status | seconds | load before | load after | reason |
| --- | --- | --- | --- | --- | --- |
| worker_overhead | ok | 137.5 | 0.78 | 1.42 |  |
| cpool | ok | 40.8 | 0.52 | 0.96 |  |
| rspool | ok | 150.3 | 0.49 | 1.14 |  |
| wasm | ok | 144.7 | 0.49 | 0.99 |  |
| nodeworker | ok | 21.8 | 0.5 | 0.51 |  |
| duckdb_memory | ok | 390.9 | 0.47 | 1.5 |  |
| postgres_drivers | ok | 49.8 | 0.55 | 1.02 |  |
| pglite | ok | 117.2 | 0.48 | 1.19 |  |
| clickhouse | ok | 181.9 | 0.47 | 1.25 |  |
| export | ok | 242.2 | 0.5 | 1.1 |  |
| loadgen | ok | 388.4 | 0.47 | 7.18 |  |
| rlimit | failed | 24.7 | 2.63 | 2.08 |  |
| scale_wake_query | ok | 785.4 | 0.76 | 0.96 |  |
| scale_tests | ok | 480.9 | 0.49 | 45.04 |  |

## Worker process boundary against in-process SQLite

`worker_overhead`: **ok** in 137.5 s, load average (1 min) 0.78 before, 1.42 after

<details><summary>output</summary>

```
SQLite worker overhead (python 3.14.8)

One-row read by primary key, round trip (median / p90 us); one-row update (median us)
  in-process           read    219.8 /    278.4   write   1583.4
  apsw                 read    246.0 /    322.2   write   1478.2
  apsw-inline          read     43.8 /     70.2   write   1382.1
  worker-json          read    506.4 /    656.7   write   1598.9
  worker-msgpack       read    465.5 /    613.8   write   1689.7
  cworker-json         read    139.9 /    187.7   write    859.4
  cworker-msgpack      read    119.3 /    166.4   write   1016.4
  cpool-json           read    140.3 /    192.0   write    925.4
  cpool-msgpack        read    130.8 /    190.6   write   1093.9
  nodeworker-json      read    444.1 /    593.4   write   1812.1
  nodeworker-msgpack   read    601.6 /    948.8   write   3538.0

Read of 1,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute      1.36 ms   1.36 us/row   stream      1.64 ms   1.64 us/row
  apsw                 execute      1.09 ms   1.09 us/row   stream      1.31 ms   1.31 us/row
  apsw-inline          execute      1.48 ms   1.48 us/row   stream      1.78 ms   1.78 us/row
  worker-json          execute      6.38 ms   6.38 us/row   stream      5.16 ms   5.16 us/row
  worker-msgpack       execute      3.00 ms   3.00 us/row   stream      2.69 ms   2.69 us/row
  cworker-json         execute      2.72 ms   2.72 us/row   stream      2.73 ms   2.73 us/row
  cworker-msgpack      execute      1.27 ms   1.27 us/row   stream      1.43 ms   1.43 us/row
  cpool-json           execute      2.49 ms   2.49 us/row   stream      2.63 ms   2.63 us/row
  cpool-msgpack        execute      1.24 ms   1.24 us/row   stream      1.09 ms   1.09 us/row
  nodeworker-json      execute      5.91 ms   5.91 us/row   stream      5.75 ms   5.75 us/row
  nodeworker-msgpack   execute      5.19 ms   5.19 us/row   stream      4.52 ms   4.52 us/row

Read of 100,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute    173.50 ms   1.73 us/row   stream    210.71 ms   2.11 us/row
  apsw                 execute    137.74 ms   1.38 us/row   stream    173.50 ms   1.74 us/row
  apsw-inline          execute    130.42 ms   1.30 us/row   stream    164.85 ms   1.65 us/row
  worker-json          execute    674.82 ms   6.75 us/row   stream    531.87 ms   5.32 us/row
  worker-msgpack       execute    439.73 ms   4.40 us/row   stream    213.95 ms   2.14 us/row
  cworker-json         execute    413.19 ms   4.13 us/row   stream    229.45 ms   2.29 us/row
  cworker-msgpack      execute    298.06 ms   2.98 us/row   stream    127.13 ms   1.27 us/row
  cpool-json           execute    362.06 ms   3.62 us/row   stream    253.06 ms   2.53 us/row
  cpool-msgpack        execute    261.05 ms   2.61 us/row   stream    122.57 ms   1.23 us/row
  nodeworker-json      execute    510.93 ms   5.11 us/row   stream    412.41 ms   4.12 us/row
  nodeworker-msgpack   execute    367.96 ms   3.68 us/row   stream    279.05 ms   2.79 us/row

Insert with alter (2 rows, add one column): median us
  in-process           interactive transaction   5111.1   one named operation   4917.0
  apsw                 interactive transaction   4406.5   one named operation   4218.1
  apsw-inline          interactive transaction   4309.4   one named operation   4308.4
  worker-json          interactive transaction   6531.4   one named operation   5127.0
  worker-msgpack       interactive transaction   5122.4   one named operation   4132.3
  cworker-json         interactive transaction   3658.2   one named operation   4022.1
  cworker-msgpack      interactive transaction   3399.2   one named operation   3482.0
  cpool-json           interactive transaction   4101.6   one named operation   3845.9
  cpool-msgpack        interactive transaction   4307.2   one named operation   4399.2
  nodeworker-json      interactive transaction  12480.9   one named operation  12336.7
  nodeworker-msgpack   interactive transaction  12043.9   one named operation  12269.8

SQLite versions: in-process sqlite3 3.53.1; apsw 3.54.1.0 bundles SQLite 3.54.1
Connection open (median us), execute_many of 100,000 rows in a transaction (ms), memory per open connection (KB of process RSS; SQLite's own bytes)
  in-process           open    271.2   execute_many    428.8 ms   rss  167.6 KB/conn
  apsw                 open    220.2   execute_many    482.7 ms   rss  185.1 KB/conn (db_status 21.4 KB)
  cworker-json         open   1808.4   execute_many    486.4 ms
  cworker-msgpack      open   1566.4   execute_many    349.1 ms

Multi-tenant C pool (load average 1.0 0.8 0.4 at start, 1.4 after; provisional)
  new database on a warm pool: first result 0.93 ms; bare open round trip 0.176 ms
  memory per open database inside the process: 170.7 KB footprint, 170.7 KB RSS (400 databases; process base 1.7 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.4 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    4946 (1.03; 0.00; 0p; 9fd)   4:    3969 (1.36; 0.00; 0p; 12fd)  16:    3462 (1.50; 0.00; 0p; 24fd)  64:    3335 (1.50; 0.00; 0p; 72fd)
    cworker     1:    7225 (0.75; 0.28; 1p; 11fd)   4:   11021 (0.98; 0.70; 4p; 20fd)  16:   13138 (0.99; 0.82; 16p; 56fd)  64:   11057 (0.97; 0.92; 64p; 200fd)
    cpool       1:    7086 (0.75; 0.29; 1p; 11fd)   4:   12139 (0.99; 0.67; 2p; 14fd)  16:   14318 (0.99; 0.80; 2p; 14fd)  64:   13259 (0.98; 0.98; 2p; 14fd)

Open to first result of a new Database (ms)
  in-process 0.72; worker dedicated 136.5 (a process per connection); shared: first 142.8, then 1.16 on the running process
  C worker dedicated 2.5; shared: first 2.6, then 0.50 on the running process

Start-up breakdown, raw pipes (median ms)
  spawn and wait for /usr/bin/true 0.74; python -S -c pass 11.8; python -c pass 33.4; dblayer-cworker --version 1.12
  python  spawn to hello  122.63  open  15.77  first query   0.86  (second  0.45)  total  139.16
  c       spawn to hello    1.12  open   0.26  first query   0.26  (second  0.10)  total    1.68

Resident memory of one idle worker with a database open (KB)
  python 27884; c 3452
```
</details>

## Multi-tenant C pool (sqlite-cpool)

`cpool`: **ok** in 40.8 s, load average (1 min) 0.52 before, 0.96 after

<details><summary>output</summary>

```
Multi-tenant C pool (load average 0.5 0.8 0.4 at start, 1.0 after; provisional)
  new database on a warm pool: first result 1.38 ms; bare open round trip 0.428 ms
  memory per open database inside the process: 170.7 KB footprint, 170.7 KB RSS (400 databases; process base 1.7 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.4 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    3489 (0.90; 0.00; 0p; 9fd)   4:    2849 (1.24; 0.00; 0p; 12fd)  16:    2678 (1.43; 0.00; 0p; 24fd)  64:    2603 (1.43; 0.00; 0p; 72fd)
    cworker     1:    5859 (0.68; 0.29; 1p; 11fd)   4:    9494 (0.94; 0.72; 4p; 20fd)  16:   11429 (0.97; 0.90; 16p; 56fd)  64:    8297 (0.90; 0.86; 64p; 200fd)
    cpool       1:    4885 (0.62; 0.27; 1p; 11fd)   4:    9695 (0.95; 0.70; 2p; 14fd)  16:   10344 (0.92; 0.89; 2p; 14fd)  64:   10779 (0.94; 1.04; 2p; 14fd)
```
</details>

## Multi-tenant C worker against its Rust port (sqlite-cpool, sqlite-rspool)

`rspool`: **ok** in 150.3 s, load average (1 min) 0.49 before, 1.14 after

| Measure (median over rounds) | C worker | Rust worker | Rust / C |
| --- | --- | --- | --- |
| Warm open to first result (ms) | 0.952 | 0.964 | 1.01 |
| Bare open round trip (ms) | 0.179 | 0.204 | 1.14 |
| One-row read (µs) | 228 | 247 | 1.08 |
| One-row update (µs) | 1.01e+03 | 1.17e+03 | 1.15 |
| execute 1,000 rows, msgpack (µs/row) | 1.11 | 1.23 | 1.11 |
| execute 100,000 rows, msgpack (µs/row) | 2.23 | 2.14 | 0.96 |
| stream 1,000 rows, msgpack (µs/row) | 1.15 | 1.21 | 1.05 |
| stream 100,000 rows, msgpack (µs/row) | 1.02 | 1.07 | 1.05 |
| execute 1,000 rows, JSON (µs/row) | 2.6 | 1.78 | 0.68 |
| execute 100,000 rows, JSON (µs/row) | 3.17 | 2.74 | 0.86 |
| stream 1,000 rows, JSON (µs/row) | 2.55 | 1.77 | 0.69 |
| stream 100,000 rows, JSON (µs/row) | 2.02 | 1.72 | 0.85 |
| Process base footprint (KB) | 847 | 1.45e+03 | 1.71 |
| Process base RSS (KB) | 2.44e+03 | 3.29e+03 | 1.35 |
| Footprint per open database (KB) | 171 | 169 | 0.99 |
| 1 clients: requests/s | 6.97e+03 | 7.17e+03 | 1.03 |
| 1 clients: parent CPU (cores) | 0.745 | 0.745 | 1.00 |
| 1 clients: worker CPU (cores) | 0.282 | 0.296 | 1.05 |
| 4 clients: requests/s | 1.16e+04 | 1.19e+04 | 1.03 |
| 4 clients: parent CPU (cores) | 0.984 | 0.986 | 1.00 |
| 4 clients: worker CPU (cores) | 0.686 | 0.696 | 1.01 |
| 16 clients: requests/s | 1.47e+04 | 1.42e+04 | 0.96 |
| 16 clients: parent CPU (cores) | 0.99 | 0.99 | 1.00 |
| 16 clients: worker CPU (cores) | 0.826 | 0.834 | 1.01 |
| 64 clients: requests/s | 1.4e+04 | 1.37e+04 | 0.98 |
| 64 clients: parent CPU (cores) | 0.99 | 0.99 | 1.00 |
| 64 clients: worker CPU (cores) | 0.927 | 0.913 | 0.98 |

## SQLite in WebAssembly (sqlite-wasm)

`wasm`: **ok** in 144.7 s, load average (1 min) 0.49 before, 0.99 after

| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.64 | 0.60 | 0.44 | 139.25 | 3.44 | 2.60 | 1.52 | 1.88 |
| One-row read, median (us) | 190 | 207 | 43 | 390 | 144 | 135 | 321 | 152 |
| One-row read, p90 (us) | 253 | 267 | 72 | 479 | 228 | 188 | 412 | 205 |
| One-row update (us) | 1233 | 1006 | 1046 | 1297 | 1392 | 1188 | 1795 | 1367 |
| Per row, 1,000-row read (us) | 1.54 | 1.40 | 1.14 | 2.56 | 1.47 | 1.35 | 1.67 | 1.24 |
| Per row, 100,000-row read (us) | 1.96 | 1.38 | 1.35 | 4.42 | 2.87 | 2.58 | 2.66 | 2.52 |
| Per row, 100,000-row stream (us) | 1.70 | 1.33 | 1.21 | 2.64 | 1.48 | 1.05 | 1.90 | 1.71 |
| execute_many, 100,000 rows (ms) | 383 | 399 | 401 | 504 | 425 | 375 | 464 | 453 |
| Memory per open connection, this process (KB) | 162 | 174 | 170 | 14 | 29 | 5 | 300 | 300 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 16069 | 334 | 18234 | 0 | 0 |

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 264 | 511 | 614 | 738 |
| sort_200k | 111 | 199 | 272 | 291 |
| instr_quadratic | 161 | 281 | 390 | 356 |

<details><summary>output</summary>

```
load average at start 0.5/0.8/0.5, at end 1.0/0.9/0.6 (provisional if high)
module: 1740 KB; compile 573 ms; load cached compilation 12.3 ms; import wasmtime 130 ms
| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.64 | 0.60 | 0.44 | 139.25 | 3.44 | 2.60 | 1.52 | 1.88 |
| One-row read, median (us) | 190 | 207 | 43 | 390 | 144 | 135 | 321 | 152 |
| One-row read, p90 (us) | 253 | 267 | 72 | 479 | 228 | 188 | 412 | 205 |
| One-row update (us) | 1233 | 1006 | 1046 | 1297 | 1392 | 1188 | 1795 | 1367 |
| Per row, 1,000-row read (us) | 1.54 | 1.40 | 1.14 | 2.56 | 1.47 | 1.35 | 1.67 | 1.24 |
| Per row, 100,000-row read (us) | 1.96 | 1.38 | 1.35 | 4.42 | 2.87 | 2.58 | 2.66 | 2.52 |
| Per row, 100,000-row stream (us) | 1.70 | 1.33 | 1.21 | 2.64 | 1.48 | 1.05 | 1.90 | 1.71 |
| execute_many, 100,000 rows (ms) | 383 | 399 | 401 | 504 | 425 | 375 | 464 | 453 |
| Memory per open connection, this process (KB) | 162 | 174 | 170 | 14 | 29 | 5 | 300 | 300 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 16069 | 334 | 18234 | 0 | 0 |
host VFS: full scan of 4010 pages through the host 22.8 ms, from a warm page cache 3.3 ms (native sqlite3, cache_size 10: 4.3 ms): 4.87 us per page read; a one-row read makes 6 host calls {'lock': 1, 'access': 2, 'read': 1, 'file_size': 1, 'unlock': 1}

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 264 | 511 | 614 | 738 |
| sort_200k | 111 | 199 | 272 | 291 |
| instr_quadratic | 161 | 281 | 390 | 356 |
```
</details>

## SQLite-WASM in a Node.js worker (sqlite-nodeworker)

`nodeworker`: **ok** in 21.8 s, load average (1 min) 0.5 before, 0.51 after

<details><summary>output</summary>

```
Node worker start-up and memory (load average 0.5 0.8 0.6 at start, 0.5 0.8 0.6 at end)

Spawn and wait: /usr/bin/true 1.2 ms; node -e 0 36.6 ms

Raw pipes, median ms: spawn to hello, open, first query (second), total
  python                   hello  134.9  open   17.5  first  0.99  ( 0.50)  total  156.2
  node                     hello   62.9  open  134.9  first  4.65  ( 1.51)  total  207.0
  node, no compile cache   hello   58.9  open  130.9  first  3.89  ( 1.17)  total  199.3
  node, no spare           hello   56.4  open  130.1  first  3.97  ( 1.14)  total  189.1
  c                        hello    1.2  open    0.3  first  0.27  ( 0.12)  total    1.9

Inside a Node worker, ms since its process started (median)
  node                     main loaded  41.7  wasm compiled  60.3  thread created  50.4  thread running 126.7  SQLite JS loaded 130.1  SQLite ready 170.3
  node, no compile cache   main loaded  40.8  wasm compiled  57.1  thread created  48.7  thread running 120.0  SQLite JS loaded 131.0  SQLite ready 168.0
  node, no spare           main loaded  40.2  wasm compiled  56.1  thread created  53.7  thread running 120.5  SQLite JS loaded 124.7  SQLite ready 159.0

Open to first result of a new database, through Database (median ms)
  in-process 0.40; python worker 144.21; node worker 191.66; C worker 2.19

Idle memory with a database open (MB, summed over the worker processes)
  python worker                footprint   19.1  RSS   27.2  (1 process(es))
  node worker                  footprint   84.8  RSS   87.3  (1 process(es))
  C worker                     footprint    1.7  RSS    3.3  (1 process(es))
  node worker, 4 connections   footprint  137.1  RSS  139.8  (1 process(es))

Inside Node, no protocol, in-memory database (SQLite 3.53.4 WASM vs node:sqlite 3.54.1 native)
  one-row read: WASM (oo1 API) 7.12 us, native 2.17 us
  100,000 rows into JS values: WASM raw exports 123.9 ms (oo1 exec 981.3), native .all() 215.3 ms
  WASM module load and init 90.1 ms; native open 0.38 ms
```
</details>

## DuckDB memory after sleep, in-process against workers

`duckdb_memory`: **ok** in 390.9 s, load average (1 min) 0.47 before, 1.5 after

<details><summary>output</summary>

```
 23 children None

rust-worker n=50 cap=40 load [1.95, 1.09, 0.72] -> [1.81, 1.09, 0.72]
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 35/26 children 2918/1090 total 2952/1116 procs 40 | after sleep parent 35/26 children 0/0 procs 0 | wake median 98.1 ms p95 131.7 ms | wall 1.43 s | fds parent 125 children 195
  cycle 2: peak parent 36/27 children 2924/1096 total 2960/1123 procs 40 | after sleep parent 36/27 children 0/0 procs 0 | wake median 93.1 ms p95 145.5 ms | wall 1.45 s | fds parent 125 children None
  cycle 3: peak parent 36/28 children 2937/1104 total 2974/1132 procs 40 | after sleep parent 36/28 children 0/0 procs 0 | wake median 107.0 ms p95 130.2 ms | wall 1.41 s | fds parent 128 children None

cpp-worker n=50 cap=40 load [1.81, 1.09, 0.72] -> [1.6, 1.07, 0.72]
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 36/27 children 2872/1063 total 2908/1089 procs 40 | after sleep parent 33/25 children 0/0 procs 0 | wake median 106.5 ms p95 190.0 ms | wall 1.46 s | fds parent 128 children 200
  cycle 2: peak parent 36/27 children 2863/1050 total 2898/1077 procs 40 | after sleep parent 34/25 children 0/0 procs 0 | wake median 106.2 ms p95 179.0 ms | wall 1.3 s | fds parent 128 children None
  cycle 3: peak parent 37/28 children 2841/1028 total 2878/1057 procs 40 | after sleep parent 34/26 children 0/0 procs 0 | wake median 93.1 ms p95 191.7 ms | wall 1.26 s | fds parent 128 children None

in-process n=150 cap=40 load [1.6, 1.07, 0.72] -> [1.25, 1.02, 0.71]
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 505/475 children 0/0 total 505/475 procs 0 | after sleep parent 116/87 children 0/0 procs 0 | wake median 85.8 ms p95 130.8 ms | wall 3.26 s | fds parent 48 children 0
  cycle 2: peak parent 482/456 children 0/0 total 482/456 procs 0 | after sleep parent 127/98 children 0/0 procs 0 | wake median 77.9 ms p95 148.5 ms | wall 3.52 s | fds parent 47 children None
  cycle 3: peak parent 476/445 children 0/0 total 476/445 procs 0 | after sleep parent 143/114 children 0/0 procs 0 | wake median 84.7 ms p95 135.8 ms | wall 3.68 s | fds parent 46 children None

in-process+gc n=150 cap=40 load [1.25, 1.02, 0.71] -> [2.24, 1.26, 0.79] PROVISIONAL
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 493/463 children 0/0 total 493/463 procs 0 | after sleep parent 135/106 children 0/0 procs 0 | wake median 91.6 ms p95 144.5 ms | wall 3.82 s | fds parent 46 children 0
  cycle 2: peak parent 495/466 children 0/0 total 495/466 procs 0 | after sleep parent 123/94 children 0/0 procs 0 | wake median 78.9 ms p95 143.8 ms | wall 3.49 s | fds parent 48 children None
  cycle 3: peak parent 536/510 children 0/0 total 536/510 procs 0 | after sleep parent 139/110 children 0/0 procs 0 | wake median 83.1 ms p95 126.3 ms | wall 3.3 s | fds parent 47 children None

in-process+trim n=150 cap=40 load [2.24, 1.26, 0.79] -> [1.81, 1.21, 0.79] PROVISIONAL
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 486/460 children 0/0 total 486/460 procs 0 | after sleep parent 115/86 children 0/0 procs 0 | wake median 83.6 ms p95 126.1 ms | wall 3.26 s | fds parent 47 children 0 | malloc_trim -> 1
  cycle 2: peak parent 500/471 children 0/0 total 500/471 procs 0 | after sleep parent 120/90 children 0/0 procs 0 | wake median 77.4 ms p95 136.6 ms | wall 3.2 s | fds parent 46 children None | malloc_trim -> 1
  cycle 3: peak parent 512/485 children 0/0 total 512/485 procs 0 | after sleep parent 120/91 children 0/0 procs 0 | wake median 85.0 ms p95 154.1 ms | wall 3.75 s | fds parent 46 children None | malloc_trim -> 1

in-process+settings n=150 cap=40 load [1.81, 1.21, 0.79] -> [3.18, 1.54, 0.9] PROVISIONAL
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 443/410 children 0/0 total 443/410 procs 0 | after sleep parent 113/84 children 0/0 procs 0 | wake median 94.4 ms p95 159.6 ms | wall 4.87 s | fds parent 48 children 0 | malloc_trim -> 1
  cycle 2: peak parent 463/428 children 0/0 total 463/428 procs 0 | after sleep parent 116/87 children 0/0 procs 0 | wake median 75.2 ms p95 118.6 ms | wall 3.96 s | fds parent 46 children None | malloc_trim -> 1
  cycle 3: peak parent 451/428 children 0/0 total 451/428 procs 0 | after sleep parent 118/89 children 0/0 procs 0 | wake median 75.6 ms p95 123.1 ms | wall 4.06 s | fds parent 47 children None | malloc_trim -> 1

worker n=150 cap=40 load [3.18, 1.54, 0.9] -> [4.73, 2.16, 1.14] PROVISIONAL
  start: parent 30/22 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 40/29 children 3828/1809 total 3869/1838 procs 40 | after sleep parent 40/32 children 0/0 procs 0 | wake median 382.9 ms p95 520.4 ms | wall 10.65 s | fds parent 128 children 360
  cycle 2: peak parent 43/32 children 3880/1860 total 3923/1891 procs 40 | after sleep parent 43/35 children 0/0 procs 0 | wake median 380.1 ms p95 463.4 ms | wall 10.0 s | fds parent 128 children None
  cycle 3: peak parent 44/35 children 3679/1729 total 3723/1761 procs 40 | after sleep parent 44/36 children 0/0 procs 0 | wake median 368.2 ms p95 462.7 ms | wall 10.32 s | fds parent 125 children None

worker-pool n=150 cap=40 load [4.73, 2.16, 1.14] -> [4.8, 2.36, 1.23] PROVISIONAL
  start: parent 30/22 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 32/22 children 1032/661 total 1064/683 procs 8 | after sleep parent 32/24 children 0/0 procs 0 | wake median 140.3 ms p95 320.6 ms | wall 5.83 s | fds parent 23 children 80
  cycle 2: peak parent 33/24 children 1041/670 total 1074/692 procs 8 | after sleep parent 33/25 children 0/0 procs 0 | wake median 162.1 ms p95 319.4 ms | wall 6.09 s | fds parent 23 children None
  cycle 3: peak parent 34/25 children 1080/708 total 1114/732 procs 8 | after sleep parent 34/26 children 0/0 procs 0 | wake median 164.9 ms p95 316.7 ms | wall 6.03 s | fds parent 23 children None

rust-worker n=150 cap=40 load [4.8, 2.36, 1.23] -> [4.79, 2.53, 1.31] PROVISIONAL
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 37/29 children 2937/1103 total 2975/1132 procs 40 | after sleep parent 37/29 children 0/0 procs 0 | wake median 132.1 ms p95 265.3 ms | wall 5.14 s | fds parent 128 children 200
  cycle 2: peak parent 39/31 children 2936/1102 total 2975/1133 procs 40 | after sleep parent 39/31 children 0/0 procs 0 | wake median 122.5 ms p95 179.2 ms | wall 4.86 s | fds parent 128 children None
  cycle 3: peak parent 40/32 children 2910/1085 total 2950/1116 procs 40 | after sleep parent 40/32 children 0/0 procs 0 | wake median 109.7 ms p95 193.4 ms | wall 4.58 s | fds parent 128 children None

cpp-worker n=150 cap=40 load [4.79, 2.53, 1.31] -> [6.48, 3.08, 1.52] PROVISIONAL
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 38/30 children 2852/1042 total 2888/1069 procs 40 | after sleep parent 38/29 children 0/0 procs 0 | wake median 116.8 ms p95 193.5 ms | wall 4.72 s | fds parent 128 children 200
  cycle 2: peak parent 40/32 children 2858/1047 total 2897/1078 procs 40 | after sleep parent 40/32 children 0/0 procs 0 | wake median 118.8 ms p95 192.8 ms | wall 4.49 s | fds parent 128 children None
  cycle 3: peak parent 42/34 children 2852/1041 total 2893/1073 procs 40 | after sleep parent 42/34 children 0/0 procs 0 | wake median 114.1 ms p95 184.9 ms | wall 4.54 s | fds parent 122 children None

native (load [6.48, 3.08, 1.52] -> [2.91, 2.66, 1.47]):
  floor: {"worker": {"spawn_to_hello_ms": 197.9, "floor_mb": [44.7, 28.5], "open_ms": 40.5, "first_query_ms": 5.4, "spawn_to_first_result_ms": 247.4, "one_db_open_mb": [79.5, 50.5], "after_analytical_mb": [96.1, 66.9]}, "rust-worker": {"spawn_to_hello_ms": 9.8, "floor_mb": [20.4, 17.5], "open_ms": 43.4, "first_query_ms": 6.3, "spawn_to_first_result_ms": 59.7, "one_db_open_mb": [56.6, 53.5], "after_analytical_mb": [73.2, 70.1]}, "cpp-worker": {"spawn_to_hello_ms": 9.4, "floor_mb": [19.6, 16.7], "open_ms": 48.7, "first_query_ms": 5.1, "spawn_to_first_result_ms": 64.5, "one_db_open_mb": [56.7, 53.7], "after_analytical_mb": [72.0, 69.0]}}
  concurrent: {"in-process": {"point_read_quiet_ms": 1.56, "point_read_during_long_query_ms": 2.12, "long_query_still_running": true}, "worker": {"point_read_quiet_ms": 3.15, "point_read_during_long_query_ms": 7806.21, "long_query_still_running": false}, "rust-worker": {"point_read_quiet_ms": 2.13, "point_read_during_long_query_ms": 2.57, "long_query_still_running": true}, "cpp-worker": {"point_read_quiet_ms": 2.21, "point_read_during_long_query_ms": 2.84, "long_query_still_running": true}}
  insert_rows_10k: {"in-process": {"default_ms": 7737.4, "appender_ms": 816.3, "speedup": 9.5}, "worker": {"default_ms": 7671.5, "appender_ms": 897.0, "speedup": 8.6}, "rust-worker": {"default_ms": 7569.2, "appender_ms": 964.6, "speedup": 7.8}, "cpp-worker": {"default_ms": 7961.9, "appender_ms": 1027.0, "speedup": 7.8}}

overhead (load [2.91, 2.66, 1.47] -> [1.5, 2.25, 1.43]):
  in-process: {"one_row_us": 2281.4, "plain_1000_ms": 3.11, "plain_100000_ms": 142.18, "typed_1000_ms": 2.93, "typed_100000_ms": 122.4, "wake_to_first_result_ms": 27.5, "wake_p90_ms": 29.1}
  worker-json: {"one_row_us": 2535.8, "plain_1000_ms": 5.95, "plain_100000_ms": 419.61, "typed_1000_ms": 11.03, "typed_100000_ms": 868.79, "wake_to_first_result_ms": 229.1, "wake_p90_ms": 254.8, "plain_1000_extra_us_per_row": 2.84, "plain_100000_extra_us_per_row": 2.774, "typed_1000_extra_us_per_row": 8.1, "typed_100000_extra_us_per_row": 7.464}
  worker-msgpack: {"one_row_us": 1937.2, "plain_1000_ms": 4.39, "plain_100000_ms": 296.68, "typed_1000_ms": 12.07, "typed_100000_ms": 997.79, "wake_to_first_result_ms": 252.8, "wake_p90_ms": 273.9, "plain_1000_extra_us_per_row": 1.28, "plain_100000_extra_us_per_row": 1.545, "typed_1000_extra_us_per_row": 9.14, "typed_100000_extra_us_per_row": 8.754}
  worker+spare: {"one_row_us": 2993.6, "plain_1000_ms": 6.11, "plain_100000_ms": 425.81, "typed_1000_ms": 11.33, "typed_100000_ms": 851.83, "wake_to_first_result_ms": 38.3, "wake_p90_ms": 48.9, "plain_1000_extra_us_per_row": 3.0, "plain_100000_extra_us_per_row": 2.836, "typed_1000_extra_us_per_row": 8.4, "typed_100000_extra_us_per_row": 7.294}
  worker-pool (process already up): {"one_row_us": 2262.2, "plain_1000_ms": 6.16, "plain_100000_ms": 396.82, "typed_1000_ms": 10.78, "typed_100000_ms": 979.08, "wake_to_first_result_ms": 28.7, "wake_p90_ms": 33.4, "plain_1000_extra_us_per_row": 3.05, "plain_100000_extra_us_per_row": 2.546, "typed_1000_extra_us_per_row": 7.85, "typed_100000_extra_us_per_row": 8.567}
  rust-worker-json: {"one_row_us": 2553.5, "plain_1000_ms": 4.62, "plain_100000_ms": 310.08, "typed_1000_ms": 8.37, "typed_100000_ms": 503.82, "wake_to_first_result_ms": 61.7, "wake_p90_ms": 65.2, "plain_1000_extra_us_per_row": 1.51, "plain_100000_extra_us_per_row": 1.679, "typed_1000_extra_us_per_row": 5.44, "typed_100000_extra_us_per_row": 3.814}
  rust-worker-msgpack: {"one_row_us": 2921.8, "plain_1000_ms": 4.37, "plain_100000_ms": 186.92, "typed_1000_ms": 6.07, "typed_100000_ms": 396.5, "wake_to_first_result_ms": 55.4, "wake_p90_ms": 61.5, "plain_1000_extra_us_per_row": 1.26, "plain_100000_extra_us_per_row": 0.447, "typed_1000_extra_us_per_row": 3.14, "typed_100000_extra_us_per_row": 2.741}
  cpp-worker-json: {"one_row_us": 2906.4, "plain_1000_ms": 6.68, "plain_100000_ms": 400.22, "typed_1000_ms": 9.67, "typed_100000_ms": 584.87, "wake_to_first_result_ms": 68.5, "wake_p90_ms": 75.3, "plain_1000_extra_us_per_row": 3.57, "plain_100000_extra_us_per_row": 2.58, "typed_1000_extra_us_per_row": 6.74, "typed_100000_extra_us_per_row": 4.625}
  cpp-worker-msgpack: {"one_row_us": 3552.7, "plain_1000_ms": 3.86, "plain_100000_ms": 183.12, "typed_1000_ms": 6.72, "typed_100000_ms": 374.38, "wake_to_first_result_ms": 67.7, "wake_p90_ms": 75.3, "plain_1000_extra_us_per_row": 0.75, "plain_100000_extra_us_per_row": 0.409, "typed_1000_extra_us_per_row": 3.79, "typed_100000_extra_us_per_row": 2.52}
```
</details>

## PostgreSQL drivers: asyncpg against psycopg 3

`postgres_drivers`: **ok** in 49.8 s, load average (1 min) 0.55 before, 1.02 after

<details><summary>output</summary>

```
connect + close: asyncpg 4.22 ms, psycopg 2.75 ms, psycopg prepared 2.47 ms
  read 1 row: asyncpg 139 us, psycopg 151 us, psycopg prepared 134 us
  read 1 row, max_rows=1000: asyncpg 133 us, psycopg 145 us, psycopg prepared 148 us
  read 1,000 rows, fetch: asyncpg 1.92 ms, psycopg 1.83 ms, psycopg prepared 1.72 ms
  read 1,000 rows, fetch, max_rows=1000: asyncpg 1.91 ms, psycopg 2.11 ms, psycopg prepared 2.14 ms
  read 1,000 rows, stream: asyncpg 2.16 ms, psycopg 2.09 ms, psycopg prepared 2.13 ms
  read 100,000 rows, fetch: asyncpg 215.86 ms, psycopg 198.18 ms, psycopg prepared 205.39 ms
  read 100,000 rows, fetch, max_rows=100000: asyncpg 201.78 ms, psycopg 204.48 ms, psycopg prepared 181.95 ms
  read 100,000 rows, stream: asyncpg 187.76 ms, psycopg 149.51 ms, psycopg prepared 154.11 ms
  insert_rows 100,000, default path: asyncpg 1.97 s, psycopg 3.34 s, psycopg prepared 1.63 s
  insert_rows 100,000, COPY: asyncpg 498.37 ms, psycopg 509.45 ms, psycopg prepared 469.91 ms
  tx statement (1-row update), with savepoints: asyncpg 220 us, psycopg 268 us, psycopg prepared 223 us
  tx statement (1-row update), without savepoints: asyncpg 140 us, psycopg 175 us, psycopg prepared 128 us
  schema(), 50 tables: asyncpg 13.04 ms, psycopg 15.43 ms, psycopg prepared 12.86 ms
  schema_version(): asyncpg 3.01 ms, psycopg 4.69 ms, psycopg prepared 2.86 ms

best of 5 runs (inserts: 2), local socket, load average 1.0, 1.8, 1.3; asyncpg 0.32.0, psycopg 3.3.9 (libpq 180006, binary)

                                                          asyncpg           psycopg  psycopg prepared
-----------------------------------------------------------------------------------------------------
connect + close                                           4.22 ms           2.75 ms           2.47 ms
read 1 row                                                 139 us            151 us            134 us
read 1 row, max_rows=1000                                  133 us            145 us            148 us
read 1,000 rows, fetch                                    1.92 ms           1.83 ms           1.72 ms
read 1,000 rows, fetch, max_rows=1000                     1.91 ms           2.11 ms           2.14 ms
read 1,000 rows, stream                                   2.16 ms           2.09 ms           2.13 ms
read 100,000 rows, fetch                                215.86 ms         198.18 ms         205.39 ms
read 100,000 rows, fetch, max_rows=100000               201.78 ms         204.48 ms         181.95 ms
read 100,000 rows, stream                               187.76 ms         149.51 ms         154.11 ms
insert_rows 100,000, default path                          1.97 s            3.34 s            1.63 s
insert_rows 100,000, COPY                               498.37 ms         509.45 ms         469.91 ms
tx statement (1-row update), with savepoints               220 us            268 us            223 us
tx statement (1-row update), without savepoints            140 us            175 us            128 us
schema(), 50 tables                                      13.04 ms          15.43 ms          12.86 ms
schema_version()                                          3.01 ms           4.69 ms           2.86 ms
```
</details>

## PGlite worker against psycopg and the SQLite workers

`pglite`: **ok** in 117.2 s, load average (1 min) 0.48 before, 1.19 after

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 457.22 | 3166.39 | 493.21 | 161.00 | 2.02 | 4.87 |
| wake_ms | 421.76 | 422.25 | 404.60 | 172.70 | 1.84 | 4.67 |
| read_1_ms | 0.42 | 0.46 | 0.47 | 0.42 | 0.12 | 0.19 |
| read_1k_ms | 11.45 | 11.02 | 10.74 | 2.66 | 1.12 | 1.00 |
| read_100k_ms | 1109.47 | 1084.62 | 958.05 | 207.56 | 174.52 | 99.74 |
| insert_many_ms | 5261.44 | 5767.01 | 3847.38 | 1163.99 | 421.73 | 2759.68 |
| insert_rows_ms | 1076.28 | 1234.47 | 841.44 | 876.10 | 579.97 | 492.89 |
| small_tx_ms | 2.28 | 1.93 | 2.29 | 2.55 | 1.56 | 0.95 |
| sleep_ms | 76.68 | 75.64 | 116.70 | 2.44 | 0.23 | 0.39 |
| idle_floor_mb | 149.17 | 149.41 | 447.88 | 85.17 | 1.82 | n/a |
| awake_footprint_mb | 503.63 | 515.49 | 561.31 | 332.06 | 9.25 | n/a |
| awake_20_footprint_mb | 1931.19 | n/a | 3260.51 | 1228.43 | 16.46 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 90.28 | 90.28 | 90.28 | 8.18 | 8.18 | n/a |
| load | 0.82, 1.49, 1.27 | 1.12, 1.50, 1.28 | 1.23, 1.49, 1.29 | 1.20, 1.48, 1.28 | 1.18, 1.47, 1.28 | 1.18, 1.47, 1.28 |

<details><summary>output</summary>

```
# pglite (load average 0.52, 1.50, 1.27)
  create_ms: 457.22
  idle_floor_mb: 149.17
  empty_disk_mb: 40.08
  wake_ms: 421.76
  wake_breakdown: {'boot_ms': 83.97, 'compile_ms': 30.04, 'wait_compile_ms': 15.75, 'open_ms': 269.72, 'first_query_ms': 22.25, 'spawn_to_hello_ms': 101.02696399985689}
  read_1_ms: 0.42
  insert_many_ms: 5261.44
  insert_rows_ms: 1076.28
  read_1k_ms: 11.45
  read_100k_ms: 1109.47
  small_tx_ms: 2.28
  awake_footprint_mb: 503.63
  disk_mb_200k_rows: 90.28
  sleep_ms: 76.68
  awake_n_footprint_mb: 1931.19
  kill: {'bystander_read_ms': 925.6544620000113, 'time_limit_300_raised_after_ms': 1026.628245999973, 'wake_after_kill_ms': 441.74010899996574}
  load: 0.82, 1.49, 1.27
# pglite-initdb (load average 0.82, 1.49, 1.27)
  create_ms: 3166.39
  idle_floor_mb: 149.41
  empty_disk_mb: 40.08
  wake_ms: 422.25
  wake_breakdown: {'boot_ms': 95.38, 'compile_ms': 32.59, 'wait_compile_ms': 15.97, 'open_ms': 279.66, 'first_query_ms': 22.37, 'spawn_to_hello_ms': 114.62929599997551}
  read_1_ms: 0.46
  insert_many_ms: 5767.01
  insert_rows_ms: 1234.47
  read_1k_ms: 11.02
  read_100k_ms: 1084.62
  small_tx_ms: 1.93
  awake_footprint_mb: 515.49
  disk_mb_200k_rows: 90.28
  sleep_ms: 75.64
  load: 1.12, 1.50, 1.28
# pglite-turbofan (load average 1.12, 1.50, 1.28)
  create_ms: 493.21
  idle_floor_mb: 447.88
  empty_disk_mb: 40.08
  wake_ms: 404.6
  wake_breakdown: {'boot_ms': 63.33, 'compile_ms': 25.85, 'wait_compile_ms': 12.06, 'open_ms': 258.85, 'first_query_ms': 21.85, 'spawn_to_hello_ms': 79.39146600006097}
  read_1_ms: 0.47
  insert_many_ms: 3847.38
  insert_rows_ms: 841.44
  read_1k_ms: 10.74
  read_100k_ms: 958.05
  small_tx_ms: 2.29
  awake_footprint_mb: 561.31
  disk_mb_200k_rows: 90.28
  sleep_ms: 116.7
  awake_n_footprint_mb: 3260.51
  load: 1.23, 1.49, 1.29
# sqlite-nodeworker (load average 1.23, 1.49, 1.29)
  create_ms: 161.0
  idle_floor_mb: 85.17
  empty_disk_mb: 0.0
  wake_ms: 172.7
  wake_breakdown: {}
  read_1_ms: 0.42
  insert_many_ms: 1163.99
  insert_rows_ms: 876.1
  read_1k_ms: 2.66
  read_100k_ms: 207.56
  small_tx_ms: 2.55
  awake_footprint_mb: 332.06
  disk_mb_200k_rows: 8.18
  sleep_ms: 2.44
  awake_n_footprint_mb: 1228.43
  load: 1.20, 1.48, 1.28
# sqlite-cworker (load average 1.20, 1.48, 1.28)
  create_ms: 2.02
  idle_floor_mb: 1.82
  empty_disk_mb: 0.0
  wake_ms: 1.84
  wake_breakdown: {}
  read_1_ms: 0.12
  insert_many_ms: 421.73
  insert_rows_ms: 579.97
  read_1k_ms: 1.12
  read_100k_ms: 174.52
  small_tx_ms: 1.56
  awake_footprint_mb: 9.25
  disk_mb_200k_rows: 8.18
  sleep_ms: 0.23
  awake_n_footprint_mb: 16.46
  load: 1.18, 1.47, 1.28
# psycopg (load average 1.18, 1.47, 1.28)
  create_ms: 4.87
  idle_floor_mb: None
  wake_ms: 4.67
  wake_breakdown: {}
  read_1_ms: 0.19
  insert_many_ms: 2759.68
  insert_rows_ms: 492.89
  read_1k_ms: 1.0
  read_100k_ms: 99.74
  small_tx_ms: 0.95
  awake_footprint_mb: None
  disk_mb_200k_rows: None
  sleep_ms: 0.39
  awake_n_footprint_mb: None
  load: 1.18, 1.47, 1.28

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 457.22 | 3166.39 | 493.21 | 161.00 | 2.02 | 4.87 |
| wake_ms | 421.76 | 422.25 | 404.60 | 172.70 | 1.84 | 4.67 |
| read_1_ms | 0.42 | 0.46 | 0.47 | 0.42 | 0.12 | 0.19 |
| read_1k_ms | 11.45 | 11.02 | 10.74 | 2.66 | 1.12 | 1.00 |
| read_100k_ms | 1109.47 | 1084.62 | 958.05 | 207.56 | 174.52 | 99.74 |
| insert_many_ms | 5261.44 | 5767.01 | 3847.38 | 1163.99 | 421.73 | 2759.68 |
| insert_rows_ms | 1076.28 | 1234.47 | 841.44 | 876.10 | 579.97 | 492.89 |
| small_tx_ms | 2.28 | 1.93 | 2.29 | 2.55 | 1.56 | 0.95 |
| sleep_ms | 76.68 | 75.64 | 116.70 | 2.44 | 0.23 | 0.39 |
| idle_floor_mb | 149.17 | 149.41 | 447.88 | 85.17 | 1.82 | n/a |
| awake_footprint_mb | 503.63 | 515.49 | 561.31 | 332.06 | 9.25 | n/a |
| awake_20_footprint_mb | 1931.19 | n/a | 3260.51 | 1228.43 | 16.46 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 90.28 | 90.28 | 90.28 | 8.18 | 8.18 | n/a |
| load | 0.82, 1.49, 1.27 | 1.12, 1.50, 1.28 | 1.23, 1.49, 1.29 | 1.20, 1.48, 1.28 | 1.18, 1.47, 1.28 | 1.18, 1.47, 1.28 |

pglite wake breakdown (ms): {'boot_ms': 83.97, 'compile_ms': 30.04, 'wait_compile_ms': 15.75, 'open_ms': 269.72, 'first_query_ms': 22.25, 'spawn_to_hello_ms': 101.02696399985689}
pglite time-limit kill: {'bystander_read_ms': 925.6544620000113, 'time_limit_300_raised_after_ms': 1026.628245999973, 'wake_after_kill_ms': 441.74010899996574}

pglite-initdb wake breakdown (ms): {'boot_ms': 95.38, 'compile_ms': 32.59, 'wait_compile_ms': 15.97, 'open_ms': 279.66, 'first_query_ms': 22.37, 'spawn_to_hello_ms': 114.62929599997551}

pglite-turbofan wake breakdown (ms): {'boot_ms': 63.33, 'compile_ms': 25.85, 'wait_compile_ms': 12.06, 'open_ms': 258.85, 'first_query_ms': 21.85, 'spawn_to_hello_ms': 79.39146600006097}

new-database options in one Node process (liftoff_only=True, load 1.18, 1.47, 1.28):
  {'rounds': 3, 'liftoff_only': True, 'compile_ms': 23.81804000000001, 'module_serialize': '2 bytes in 0.1 ms', 'initdb_open_ms': 2146.4827819999996, 'template_copy_ms': 60.515190000000075, 'template_copy_open_ms': 220.005134, 'reopen_existing_ms': 161.55422499999986, 'dump_ms': 129.4482279999993, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 487.5068169999995, 'rss_mb': 518}

new-database options in one Node process (liftoff_only=False, load 1.15, 1.45, 1.28):
  {'rounds': 3, 'liftoff_only': False, 'compile_ms': 24.814699999999988, 'module_serialize': '2 bytes in 0.1 ms', 'initdb_open_ms': 1813.313173, 'template_copy_ms': 53.32054499999958, 'template_copy_open_ms': 207.58756300000005, 'reopen_existing_ms': 149.88196499999958, 'dump_ms': 133.1291709999996, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 464.19200699999965, 'rss_mb': 582}
```
</details>

## ClickHouse (chDB) against DuckDB and SQLite

`clickhouse`: **ok** in 181.9 s, load average (1 min) 0.47 before, 1.25 after

<details><summary>output</summary>

```
## lifecycle and memory   (load average 0.47, 1.20, 1.20)
import chdb 0.25s; first statement with the server down 179 ms; second database while up 11.4 ms; wake after sleep 76 ms
RSS MB: before import 33, after import 264, server up 291, after 1M insert + group by 410, after sleep 411

## analytical reads and point reads, 1,000,000 rows   (load average 0.47, 1.20, 1.20)
clickhouse fill 0.27s  group by 20 ms  join 39 ms  point read 7.01 ms  disk 9.4 MB
duckdb     fill 0.95s  group by 10 ms  join 10 ms  point read 2.38 ms  disk 12.1 MB
sqlite     fill 1.00s  group by 507 ms  join 522 ms  point read 0.20 ms  disk 23.0 MB

## analytical reads and point reads, 10,000,000 rows   (load average 0.55, 1.19, 1.20)
clickhouse fill 1.16s  group by 85 ms  join 204 ms  point read 5.98 ms  disk 155.9 MB
duckdb     fill 6.92s  group by 48 ms  join 50 ms  point read 2.21 ms  disk 107.0 MB
sqlite     fill 8.39s  group by 5679 ms  join 5962 ms  point read 0.22 ms  disk 237.8 MB

## insert 1,000,000 rows   (load average 0.82, 1.16, 1.18)
clickhouse native        insert_rows 1,000,000: 14.36s (69,656 rows/s)
clickhouse default path  insert_rows 1,000,000: 23.31s (42,901 rows/s)
duckdb                   insert_rows 1,000,000: 4.35s (229,842 rows/s)
sqlite                   insert_rows 1,000,000: 6.23s (160,502 rows/s)
clickhouse VALUES batches insert_rows 1,000,000: 2420.01s (413 rows/s; 20,000 rows, scaled)
clickhouse row by row: 5.00 ms a row, 5005s for 1,000,000 (scaled)

## export 1,000,000 rows   (load average 1.27, 1.16, 1.18)
clickhouse export csv 1,000,000 engine: 0.19s wall, 0.26s process CPU
clickhouse export csv 1,000,000 python fallback: 2.82s wall, 2.99s process CPU
clickhouse export json 1,000,000 engine: 0.37s wall, 0.44s process CPU
clickhouse export json 1,000,000 python fallback: 2.50s wall, 2.63s process CPU

load average at the end: 1.25, 1.16, 1.17
```
</details>

## db.export(): native serialisation against the Python fallback

`export`: **ok** in 242.2 s, load average (1 min) 0.5 before, 1.1 after

| backend | table | rows | format | mode | transport | cpu_s | wall_s | child_cpu_s | rss_growth_mb | mb_s | rows_s | lag_max_ms | lag_p99_ms | load1 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cworker | wide | 10000 | csv | native | raw | 0.016 | 0.053 | 0.041 | 0.000 | 31.734 | 188738.880 | 6.644 | 6.644 | 0.6 |
| cworker | wide | 10000 | csv | native | frames | 0.010 | 0.042 | 0.038 | 0.000 | 39.583 | 235423.324 | 1.699 | 1.699 | 0.6 |
| cworker | wide | 10000 | csv | native | fifo | 0.010 | 0.043 | 0.042 | 0.000 | 38.691 | 230119.531 | 2.297 | 2.297 | 0.6 |
| cworker | wide | 10000 | csv | fallback |  | 0.082 | 0.101 | 0.024 | 0.000 | 16.633 | 98923.999 | 16.621 | 16.621 | 0.6 |
| cworker | wide | 10000 | csv | datasette |  | 0.068 | 0.083 | 0.022 | 0.000 | 20.303 | 121042.136 | 11.925 | 11.925 | 0.6 |
| cworker | wide | 10000 | csv | handoff |  | 0.008 | 0.040 | 0.034 | 0.000 | 41.566 | 247182.649 | 7.280 | 7.280 | 0.6 |
| cworker | wide | 10000 | tsv | native | raw | 0.011 | 0.045 | 0.041 | 0.000 | 37.005 | 220088.547 | 1.121 | 1.121 | 0.6 |
| cworker | wide | 10000 | tsv | fallback |  | 0.084 | 0.096 | 0.019 | 0.000 | 17.457 | 103828.564 | 17.446 | 17.446 | 0.6 |
| cworker | wide | 10000 | tsv | datasette |  | 0.063 | 0.078 | 0.021 | 0.000 | 21.494 | 128139.621 | 11.328 | 11.328 | 0.6 |
| cworker | wide | 10000 | json | native | raw | 0.012 | 0.043 | 0.036 | 0.000 | 54.182 | 231829.677 | 0.941 | 0.941 | 0.6 |
| cworker | wide | 10000 | json | native | frames | 0.013 | 0.045 | 0.036 | 0.000 | 51.371 | 219805.447 | 2.231 | 2.231 | 0.6 |
| cworker | wide | 10000 | json | native | fifo | 0.009 | 0.038 | 0.036 | 0.000 | 61.554 | 263373.716 | 1.448 | 1.448 | 0.6 |
| cworker | wide | 10000 | json | fallback |  | 0.045 | 0.060 | 0.021 | 0.000 | 38.727 | 165701.730 | 7.993 | 7.993 | 0.6 |
| cworker | wide | 10000 | json | datasette |  | 0.063 | 0.074 | 0.019 | 0.000 | 33.156 | 134390.571 | 27.297 | 27.297 | 0.6 |
| cworker | wide | 10000 | json | handoff |  | 0.007 | 0.046 | 0.042 | 0.000 | 51.208 | 219073.465 | 4.132 | 4.132 | 0.6 |
| cworker | wide | 10000 | ndjson | native | raw | 0.012 | 0.044 | 0.038 | 0.000 | 52.799 | 226896.353 | 1.892 | 1.892 | 0.6 |
| cworker | wide | 10000 | ndjson | fallback |  | 0.046 | 0.057 | 0.018 | 0.000 | 40.595 | 174450.174 | 7.235 | 7.235 | 0.6 |
| cworker | wide | 10000 | ndjson | datasette |  | 0.097 | 0.111 | 0.020 | 0.000 | 22.054 | 89759.659 | 64.448 | 64.448 | 0.6 |
| cworker | narrow | 10000 | csv | native | raw | 0.003 | 0.009 | 0.009 | 0.000 | 23.327 | 1104757.894 | 0.787 | 0.787 | 0.6 |
| cworker | narrow | 10000 | csv | fallback |  | 0.020 | 0.027 | 0.011 | 0.000 | 7.916 | 374925.549 | 3.657 | 3.657 | 0.6 |
| cworker | narrow | 10000 | json | native | raw | 0.004 | 0.011 | 0.010 | 0.000 | 34.309 | 876996.120 | 1.668 | 1.668 | 0.6 |
| cworker | narrow | 10000 | json | fallback |  | 0.024 | 0.030 | 0.010 | 0.000 | 12.852 | 328522.181 | 4.951 | 4.951 | 0.6 |
| duckdb | wide | 10000 | csv | native |  | 0.049 | 0.046 | 0.000 | 0.000 | 36.332 | 216084.799 | 4.689 | 4.689 | 0.6 |
| duckdb | wide | 10000 | csv | fallback |  | 0.121 | 0.121 | 0.000 | 0.000 | 13.857 | 82416.702 | 10.094 | 10.094 | 0.6 |
| duckdb | wide | 10000 | csv | datasette |  | 0.094 | 0.093 | 0.000 | 0.000 | 17.946 | 106991.570 | 6.940 | 6.940 | 0.6 |
| duckdb | wide | 10000 | tsv | native |  | 0.044 | 0.041 | 0.000 | 0.000 | 40.608 | 241522.185 | 3.727 | 3.727 | 0.6 |
| duckdb | wide | 10000 | tsv | fallback |  | 0.105 | 0.105 | 0.000 | 0.000 | 16.087 | 95679.636 | 9.037 | 9.037 | 0.6 |
| duckdb | wide | 10000 | tsv | datasette |  | 0.092 | 0.091 | 0.000 | 0.000 | 18.444 | 109959.607 | 7.709 | 7.709 | 0.6 |
| duckdb | wide | 10000 | json | native |  | 0.049 | 0.047 | 0.000 | 0.000 | 49.442 | 211549.821 | 3.464 | 3.464 | 0.6 |
| duckdb | wide | 10000 | json | fallback |  | 0.080 | 0.080 | 0.000 | 0.000 | 29.289 | 125321.236 | 10.085 | 10.085 | 0.6 |
| duckdb | wide | 10000 | json | datasette |  | 0.094 | 0.094 | 0.000 | 0.000 | 26.291 | 106565.344 | 39.256 | 39.256 | 0.6 |
| duckdb | wide | 10000 | ndjson | native |  | 0.049 | 0.047 | 0.000 | 0.000 | 49.947 | 214637.745 | 3.389 | 3.389 | 0.6 |
| duckdb | wide | 10000 | ndjson | fallback |  | 0.096 | 0.095 | 0.000 | 0.000 | 24.386 | 104794.968 | 6.225 | 6.225 | 0.6 |
| duckdb | wide | 10000 | ndjson | datasette |  | 0.136 | 0.136 | 0.000 | 0.000 | 18.082 | 73594.871 | 89.390 | 89.390 | 0.6 |
| duckdb | narrow | 10000 | csv | native |  | 0.025 | 0.023 | 0.000 | 0.000 | 9.018 | 427146.912 | 3.828 | 3.828 | 0.6 |
| duckdb | narrow | 10000 | csv | fallback |  | 0.028 | 0.027 | 0.000 | 0.000 | 7.691 | 364280.239 | 2.701 | 2.701 | 0.6 |
| duckdb | narrow | 10000 | json | native |  | 0.023 | 0.022 | 0.000 | 0.000 | 17.910 | 457825.609 | 2.626 | 2.626 | 0.6 |
| duckdb | narrow | 10000 | json | fallback |  | 0.028 | 0.027 | 0.000 | 0.000 | 14.230 | 363768.736 | 1.868 | 1.868 | 0.6 |
| postgres | wide | 10000 | csv | native |  | 0.033 | 0.039 | 0.000 | 0.000 | 43.133 | 256535.215 | 6.454 | 6.454 | 0.6 |
| postgres | wide | 10000 | csv | fallback |  | 0.099 | 0.105 | 0.000 | 0.000 | 16.018 | 95270.135 | 10.939 | 10.939 | 0.7 |
| postgres | wide | 10000 | csv | datasette |  | 0.088 | 0.092 | 0.000 | 0.000 | 18.162 | 108276.203 | 9.328 | 9.328 | 0.7 |
| postgres | wide | 10000 | tsv | native |  | 0.029 | 0.033 | 0.000 | 0.000 | 51.151 | 304225.106 | 1.064 | 1.064 | 0.7 |
| postgres | wide | 10000 | tsv | fallback |  | 0.105 | 0.110 | 0.000 | 0.000 | 15.342 | 91248.543 | 11.428 | 11.428 | 0.7 |
| postgres | wide | 10000 | tsv | datasette |  | 0.088 | 0.092 | 0.000 | 0.000 | 18.190 | 108445.289 | 8.983 | 8.983 | 0.7 |
| postgres | wide | 10000 | json | native |  | 0.036 | 0.043 | 0.000 | 0.000 | 54.368 | 232625.623 | 1.255 | 1.255 | 0.7 |
| postgres | wide | 10000 | json | fallback |  | 0.082 | 0.088 | 0.000 | 0.000 | 26.504 | 113405.129 | 9.275 | 9.275 | 0.7 |
| postgres | wide | 10000 | json | datasette |  | 0.081 | 0.085 | 0.000 | 0.000 | 28.919 | 117217.957 | 43.100 | 43.100 | 0.7 |
| postgres | wide | 10000 | ndjson | native |  | 0.040 | 0.050 | 0.000 | 0.000 | 46.451 | 199614.975 | 1.273 | 1.273 | 0.7 |
| postgres | wide | 10000 | ndjson | fallback |  | 0.081 | 0.085 | 0.000 | 0.000 | 27.229 | 117011.287 | 7.863 | 7.863 | 0.7 |
| postgres | wide | 10000 | ndjson | datasette |  | 0.102 | 0.107 | 0.000 | 0.000 | 23.032 | 93738.995 | 69.231 | 69.231 | 0.7 |
| postgres | narrow | 10000 | csv | native |  | 0.022 | 0.024 | 0.000 | 0.000 | 8.655 | 409922.082 | 2.137 | 2.137 | 0.7 |
| postgres | narrow | 10000 | csv | fallback |  | 0.029 | 0.032 | 0.000 | 0.000 | 6.677 | 316237.862 | 3.728 | 3.728 | 0.7 |
| postgres | narrow | 10000 | json | native |  | 0.022 | 0.025 | 0.000 | 0.000 | 15.664 | 400393.330 | 1.342 | 1.342 | 0.7 |
| postgres | narrow | 10000 | json | fallback |  | 0.033 | 0.035 | 0.000 | 0.000 | 11.321 | 289386.610 | 3.664 | 3.664 | 0.7 |
| cworker | wide | 100000 | csv | native | raw | 0.077 | 0.417 | 0.365 | 0.000 | 41.062 | 239906.666 | 1.144 | 1.067 | 0.7 |
| cworker | wide | 100000 | csv | native | frames | 0.080 | 0.412 | 0.360 | 0.000 | 41.580 | 242933.104 | 1.987 | 1.043 | 0.7 |
| cworker | wide | 100000 | csv | native | fifo | 0.086 | 0.398 | 0.366 | 0.000 | 43.056 | 251559.409 | 2.835 | 1.110 | 0.7 |
| cworker | wide | 100000 | csv | fallback |  | 0.855 | 1.004 | 0.184 | 0.000 | 17.045 | 99586.014 | 18.824 | 18.032 | 0.7 |
| cworker | wide | 100000 | csv | datasette |  | 0.743 | 0.906 | 0.197 | 0.000 | 18.843 | 110350.743 | 17.569 | 16.618 | 0.7 |
| cworker | wide | 100000 | csv | handoff |  | 0.023 | 0.335 | 0.329 | 0.000 | 51.094 | 298482.320 | 4.218 | 0.857 | 0.7 |
| cworker | wide | 100000 | tsv | native | raw | 0.069 | 0.390 | 0.343 | 0.000 | 43.913 | 256565.232 | 1.791 | 1.119 | 0.7 |
| cworker | wide | 100000 | tsv | fallback |  | 0.776 | 0.926 | 0.185 | 0.000 | 18.481 | 107978.219 | 17.990 | 16.588 | 0.7 |
| cworker | wide | 100000 | tsv | datasette |  | 0.671 | 0.808 | 0.172 | 0.000 | 21.138 | 123788.079 | 15.656 | 15.633 | 0.7 |
| cworker | wide | 100000 | json | native | raw | 0.082 | 0.440 | 0.379 | 0.000 | 53.814 | 227322.006 | 1.748 | 1.125 | 0.7 |
| cworker | wide | 100000 | json | native | frames | 0.075 | 0.419 | 0.368 | 0.000 | 56.565 | 238945.212 | 1.962 | 1.068 | 0.7 |
| cworker | wide | 100000 | json | native | fifo | 0.090 | 0.421 | 0.389 | 0.000 | 56.212 | 237453.034 | 2.418 | 1.032 | 0.7 |
| cworker | wide | 100000 | json | fallback |  | 0.486 | 0.631 | 0.184 | 0.000 | 37.522 | 158500.563 | 10.918 | 9.813 | 0.7 |
| cworker | wide | 100000 | json | datasette |  | 0.638 | 0.773 | 0.180 | 0.000 | 32.303 | 129353.995 | 335.983 | 9.642 | 0.7 |
| cworker | wide | 100000 | json | handoff |  | 0.031 | 0.369 | 0.364 | 0.000 | 64.158 | 270982.891 | 3.597 | 0.666 | 0.7 |
| cworker | wide | 100000 | ndjson | native | raw | 0.094 | 0.443 | 0.376 | 0.000 | 53.239 | 225848.578 | 2.039 | 1.183 | 0.7 |
| cworker | wide | 100000 | ndjson | fallback |  | 0.442 | 0.579 | 0.175 | 0.000 | 40.748 | 172860.153 | 11.904 | 10.320 | 0.7 |
| cworker | wide | 100000 | ndjson | datasette |  | 0.856 | 0.995 | 0.181 | 0.000 | 24.996 | 100495.956 | 596.735 | 7.595 | 0.8 |
| cworker | narrow | 100000 | csv | native | raw | 0.012 | 0.078 | 0.072 | 0.000 | 31.072 | 1288610.890 | 1.982 | 1.982 | 0.8 |
| cworker | narrow | 100000 | csv | fallback |  | 0.193 | 0.252 | 0.078 | 0.000 | 9.577 | 397190.055 | 4.568 | 4.266 | 0.8 |
| cworker | narrow | 100000 | json | native | raw | 0.014 | 0.074 | 0.067 | 0.000 | 56.640 | 1344950.803 | 1.398 | 1.398 | 0.8 |
| cworker | narrow | 100000 | json | fallback |  | 0.197 | 0.259 | 0.080 | 0.000 | 16.271 | 386362.014 | 4.984 | 4.984 | 0.8 |
| duckdb | wide | 100000 | csv | native |  | 0.237 | 0.187 | 0.000 | 0.000 | 91.508 | 534646.351 | 5.113 | 1.220 | 0.8 |
| duckdb | wide | 100000 | csv | fallback |  | 0.944 | 0.930 | 0.000 | 0.000 | 18.401 | 107511.305 | 9.711 | 9.344 | 0.8 |
| duckdb | wide | 100000 | csv | datasette |  | 0.865 | 0.844 | 0.000 | 0.000 | 20.224 | 118435.036 | 9.874 | 7.781 | 0.8 |
| duckdb | wide | 100000 | tsv | native |  | 0.265 | 0.203 | 0.000 | 0.000 | 84.120 | 491476.737 | 2.526 | 2.494 | 0.8 |
| duckdb | wide | 100000 | tsv | fallback |  | 0.952 | 0.934 | 0.000 | 0.000 | 18.334 | 107119.681 | 10.049 | 9.766 | 0.9 |
| duckdb | wide | 100000 | tsv | datasette |  | 0.854 | 0.833 | 0.000 | 0.000 | 20.492 | 120004.081 | 8.498 | 7.967 | 0.9 |
| duckdb | wide | 100000 | json | native |  | 0.272 | 0.211 | 0.000 | 0.000 | 112.274 | 474271.492 | 4.200 | 1.580 | 0.9 |
| duckdb | wide | 100000 | json | fallback |  | 0.704 | 0.684 | 0.000 | 0.000 | 34.615 | 146222.412 | 6.020 | 5.894 | 0.9 |
| duckdb | wide | 100000 | json | datasette |  | 0.740 | 0.725 | 0.000 | 13.173 | 34.428 | 137861.912 | 370.806 | 3.432 | 0.9 |
| duckdb | wide | 100000 | ndjson | native |  | 0.259 | 0.187 | 0.000 | 0.000 | 126.096 | 534921.591 | 4.772 | 2.025 | 0.9 |
| duckdb | wide | 100000 | ndjson | fallback |  | 0.660 | 0.639 | 0.000 | 0.000 | 36.871 | 156414.347 | 9.343 | 5.784 | 0.9 |
| duckdb | wide | 100000 | ndjson | datasette |  | 1.047 | 1.028 | 0.000 | 22.725 | 24.205 | 97316.124 | 666.888 | 3.450 | 0.9 |
| duckdb | narrow | 100000 | csv | native |  | 0.102 | 0.100 | 0.000 | 0.000 | 24.132 | 1000811.328 | 4.008 | 4.008 | 0.9 |
| duckdb | narrow | 100000 | csv | fallback |  | 0.256 | 0.255 | 0.000 | 0.000 | 9.449 | 391863.253 | 3.157 | 2.472 | 0.9 |
| duckdb | narrow | 100000 | json | native |  | 0.127 | 0.125 | 0.000 | 0.000 | 33.701 | 800245.483 | 4.294 | 4.294 | 0.9 |
| duckdb | narrow | 100000 | json | fallback |  | 0.246 | 0.244 | 0.000 | 0.000 | 17.257 | 409788.681 | 2.993 | 2.485 | 0.9 |
| postgres | wide | 100000 | csv | native |  | 0.260 | 0.293 | 0.000 | 0.000 | 58.396 | 341186.754 | 0.904 | 0.665 | 1.0 |
| postgres | wide | 100000 | csv | fallback |  | 0.972 | 1.007 | 0.000 | 0.000 | 16.993 | 99286.042 | 13.059 | 12.697 | 1.0 |
| postgres | wide | 100000 | csv | datasette |  | 0.900 | 0.936 | 0.000 | 0.000 | 18.252 | 106891.095 | 11.740 | 11.451 | 1.0 |
| postgres | wide | 100000 | tsv | native |  | 0.264 | 0.298 | 0.000 | 0.000 | 57.522 | 336078.616 | 1.242 | 0.780 | 1.0 |
| postgres | wide | 100000 | tsv | fallback |  | 0.921 | 0.960 | 0.000 | 0.000 | 17.836 | 104208.213 | 11.359 | 11.321 | 1.0 |
| postgres | wide | 100000 | tsv | datasette |  | 0.854 | 0.889 | 0.000 | 0.000 | 19.207 | 112483.677 | 10.648 | 10.262 | 1.0 |
| postgres | wide | 100000 | json | native |  | 0.312 | 0.377 | 0.000 | 0.000 | 62.759 | 265107.450 | 1.013 | 0.476 | 1.0 |
| postgres | wide | 100000 | json | fallback |  | 0.701 | 0.738 | 0.000 | 0.000 | 32.072 | 135480.672 | 8.903 | 8.301 | 1.0 |
| postgres | wide | 100000 | json | datasette |  | 0.751 | 0.799 | 0.000 | 0.000 | 31.269 | 125210.432 | 396.297 | 5.926 | 1.0 |
| postgres | wide | 100000 | ndjson | native |  | 0.322 | 0.383 | 0.000 | 0.000 | 61.493 | 260863.838 | 0.996 | 0.552 | 1.0 |
| postgres | wide | 100000 | ndjson | fallback |  | 0.628 | 0.666 | 0.000 | 0.000 | 35.392 | 150140.547 | 8.080 | 7.539 | 1.0 |
| postgres | wide | 100000 | ndjson | datasette |  | 1.070 | 1.109 | 0.000 | 0.000 | 22.420 | 90137.533 | 741.203 | 6.149 | 1.0 |
| postgres | narrow | 100000 | csv | native |  | 0.216 | 0.235 | 0.000 | 0.000 | 10.256 | 425345.890 | 2.478 | 2.478 | 1.0 |
| postgres | narrow | 100000 | csv | fallback |  | 0.253 | 0.275 | 0.000 | 0.000 | 8.779 | 364103.198 | 3.476 | 3.423 | 1.0 |
| postgres | narrow | 100000 | json | native |  | 0.181 | 0.196 | 0.000 | 0.000 | 21.433 | 508946.169 | 1.376 | 1.275 | 1.0 |
| postgres | narrow | 100000 | json | fallback |  | 0.237 | 0.255 | 0.000 | 0.000 | 16.523 | 392338.028 | 3.059 | 2.344 | 1.0 |
| cworker | wide | 1000000 | csv | native | raw | 0.470 | 3.649 | 3.291 | 0.000 | 47.725 | 274036.810 | 6.502 | 1.110 | 1.0 |
| cworker | wide | 1000000 | csv | native | frames | 0.511 | 3.542 | 3.205 | 0.000 | 49.170 | 282331.199 | 2.204 | 1.099 | 1.0 |
| cworker | wide | 1000000 | csv | native | fifo | 0.703 | 3.672 | 3.406 | 0.000 | 47.432 | 272351.369 | 3.054 | 1.101 | 1.0 |
| cworker | wide | 1000000 | csv | fallback |  | 7.150 | 8.534 | 1.703 | 0.000 | 20.408 | 117181.484 | 19.317 | 16.147 | 1.0 |
| cworker | wide | 1000000 | csv | datasette |  | 6.557 | 7.937 | 1.677 | 0.000 | 21.893 | 125998.057 | 16.742 | 14.504 | 1.0 |
| cworker | wide | 1000000 | csv | handoff |  | 0.172 | 3.212 | 3.187 | 0.000 | 54.225 | 311318.226 | 3.542 | 0.251 | 1.0 |
| cworker | wide | 1000000 | json | native | raw | 0.557 | 3.776 | 3.353 | 0.000 | 63.480 | 264800.745 | 1.427 | 1.037 | 1.0 |
| cworker | wide | 1000000 | json | native | frames | 0.592 | 3.874 | 3.439 | 0.000 | 61.885 | 258148.666 | 1.968 | 1.043 | 1.0 |
| cworker | wide | 1000000 | json | native | fifo | 0.747 | 3.652 | 3.450 | 0.000 | 65.647 | 273838.679 | 1.251 | 1.004 | 1.0 |
| cworker | wide | 1000000 | json | fallback |  | 4.245 | 5.653 | 1.759 | 0.000 | 42.410 | 176909.151 | 11.648 | 9.510 | 1.0 |
| cworker | wide | 1000000 | json | datasette |  | 6.295 | 7.710 | 1.819 | 981.742 | 32.779 | 129699.882 | 2667.241 | 7.364 | 1.0 |
| cworker | wide | 1000000 | json | handoff |  | 0.209 | 3.539 | 3.515 | 0.000 | 67.755 | 282593.187 | 3.438 | 0.418 | 1.0 |
| cworker | narrow | 1000000 | csv | native | raw | 0.133 | 0.802 | 0.712 | 0.000 | 33.817 | 1247278.767 | 1.576 | 1.092 | 1.0 |
| cworker | narrow | 1000000 | csv | fallback |  | 2.058 | 2.644 | 0.769 | 0.000 | 10.256 | 378272.077 | 5.977 | 4.423 | 1.0 |
| cworker | narrow | 1000000 | json | native | raw | 0.136 | 0.780 | 0.687 | 0.000 | 57.841 | 1282146.340 | 1.719 | 0.943 | 1.0 |
| cworker | narrow | 1000000 | json | fallback |  | 2.192 | 2.762 | 0.760 | 0.000 | 16.333 | 362051.703 | 5.839 | 5.013 | 1.0 |
| duckdb | wide | 1000000 | csv | native |  | 2.615 | 1.401 | 0.000 | 122.806 | 124.304 | 713753.204 | 3.052 | 1.509 | 1.0 |
| duckdb | wide | 1000000 | csv | fallback |  | 11.044 | 10.748 | 0.000 | 109.044 | 16.204 | 93044.746 | 19.622 | 11.191 | 0.9 |
| duckdb | wide | 1000000 | csv | datasette |  | 10.179 | 9.884 | 0.000 | 109.142 | 17.580 | 101175.884 | 11.858 | 9.861 | 0.9 |
| duckdb | wide | 1000000 | json | native |  | 2.872 | 1.550 | 0.000 | 130.224 | 154.619 | 644978.011 | 3.424 | 2.434 | 0.9 |
| duckdb | wide | 1000000 | json | fallback |  | 8.082 | 7.753 | 0.000 | 109.670 | 30.923 | 128990.572 | 9.620 | 7.843 | 0.8 |
| duckdb | wide | 1000000 | json | datasette |  | 9.104 | 8.850 | 0.000 | 1259.262 | 28.556 | 112990.593 | 4177.892 | 3.790 | 0.9 |
| duckdb | narrow | 1000000 | csv | native |  | 0.867 | 0.507 | 0.000 | 0.000 | 53.446 | 1971299.601 | 13.368 | 1.409 | 0.9 |
| duckdb | narrow | 1000000 | csv | fallback |  | 2.400 | 2.303 | 0.000 | 0.000 | 11.775 | 434300.920 | 4.781 | 2.333 | 0.9 |
| duckdb | narrow | 1000000 | json | native |  | 0.982 | 0.529 | 0.000 | 0.000 | 85.321 | 1891303.601 | 2.905 | 1.196 | 0.9 |
| duckdb | narrow | 1000000 | json | fallback |  | 2.841 | 2.731 | 0.000 | 0.000 | 16.518 | 366144.534 | 3.455 | 2.935 | 1.0 |
| postgres | wide | 1000000 | csv | native |  | 2.733 | 2.946 | 0.000 | 0.000 | 59.106 | 339385.972 | 1.212 | 0.696 | 1.0 |
| postgres | wide | 1000000 | csv | fallback |  | 9.146 | 9.366 | 0.000 | 0.000 | 18.595 | 106769.966 | 12.193 | 11.503 | 1.0 |
| postgres | wide | 1000000 | csv | datasette |  | 8.528 | 8.749 | 0.000 | 0.000 | 19.861 | 114301.220 | 11.661 | 10.789 | 1.1 |
| postgres | wide | 1000000 | json | native |  | 3.575 | 4.061 | 0.000 | 0.000 | 59.027 | 246226.084 | 1.219 | 0.562 | 1.2 |
| postgres | wide | 1000000 | json | fallback |  | 7.054 | 7.283 | 0.000 | 0.000 | 32.914 | 137298.927 | 22.199 | 8.607 | 1.1 |
| postgres | wide | 1000000 | json | datasette |  | 7.747 | 7.960 | 0.000 | 979.661 | 31.749 | 125626.532 | 3665.930 | 5.742 | 1.1 |
| postgres | narrow | 1000000 | csv | native |  | 2.025 | 2.224 | 0.000 | 0.000 | 12.190 | 449624.067 | 2.910 | 2.252 | 1.1 |
| postgres | narrow | 1000000 | csv | fallback |  | 2.266 | 2.461 | 0.000 | 0.000 | 11.016 | 406307.441 | 3.235 | 2.277 | 1.1 |
| postgres | narrow | 1000000 | json | native |  | 2.013 | 2.222 | 0.000 | 0.000 | 20.307 | 450137.754 | 2.165 | 1.815 | 1.1 |
| postgres | narrow | 1000000 | json | fallback |  | 2.318 | 2.516 | 0.000 | 0.000 | 17.932 | 397502.636 | 2.506 | 2.225 | 1.1 |

<details><summary>output</summary>

```
fallback		0.046	0.057	0.018	0.000	40.595	174450.174	7.235	7.235	0.6
cworker	wide	10000	ndjson	datasette		0.097	0.111	0.020	0.000	22.054	89759.659	64.448	64.448	0.6
cworker	narrow	10000	csv	native	raw	0.003	0.009	0.009	0.000	23.327	1104757.894	0.787	0.787	0.6
cworker	narrow	10000	csv	fallback		0.020	0.027	0.011	0.000	7.916	374925.549	3.657	3.657	0.6
cworker	narrow	10000	json	native	raw	0.004	0.011	0.010	0.000	34.309	876996.120	1.668	1.668	0.6
cworker	narrow	10000	json	fallback		0.024	0.030	0.010	0.000	12.852	328522.181	4.951	4.951	0.6
duckdb	wide	10000	csv	native		0.049	0.046	0.000	0.000	36.332	216084.799	4.689	4.689	0.6
duckdb	wide	10000	csv	fallback		0.121	0.121	0.000	0.000	13.857	82416.702	10.094	10.094	0.6
duckdb	wide	10000	csv	datasette		0.094	0.093	0.000	0.000	17.946	106991.570	6.940	6.940	0.6
duckdb	wide	10000	tsv	native		0.044	0.041	0.000	0.000	40.608	241522.185	3.727	3.727	0.6
duckdb	wide	10000	tsv	fallback		0.105	0.105	0.000	0.000	16.087	95679.636	9.037	9.037	0.6
duckdb	wide	10000	tsv	datasette		0.092	0.091	0.000	0.000	18.444	109959.607	7.709	7.709	0.6
duckdb	wide	10000	json	native		0.049	0.047	0.000	0.000	49.442	211549.821	3.464	3.464	0.6
duckdb	wide	10000	json	fallback		0.080	0.080	0.000	0.000	29.289	125321.236	10.085	10.085	0.6
duckdb	wide	10000	json	datasette		0.094	0.094	0.000	0.000	26.291	106565.344	39.256	39.256	0.6
duckdb	wide	10000	ndjson	native		0.049	0.047	0.000	0.000	49.947	214637.745	3.389	3.389	0.6
duckdb	wide	10000	ndjson	fallback		0.096	0.095	0.000	0.000	24.386	104794.968	6.225	6.225	0.6
duckdb	wide	10000	ndjson	datasette		0.136	0.136	0.000	0.000	18.082	73594.871	89.390	89.390	0.6
duckdb	narrow	10000	csv	native		0.025	0.023	0.000	0.000	9.018	427146.912	3.828	3.828	0.6
duckdb	narrow	10000	csv	fallback		0.028	0.027	0.000	0.000	7.691	364280.239	2.701	2.701	0.6
duckdb	narrow	10000	json	native		0.023	0.022	0.000	0.000	17.910	457825.609	2.626	2.626	0.6
duckdb	narrow	10000	json	fallback		0.028	0.027	0.000	0.000	14.230	363768.736	1.868	1.868	0.6
postgres	wide	10000	csv	native		0.033	0.039	0.000	0.000	43.133	256535.215	6.454	6.454	0.6
postgres	wide	10000	csv	fallback		0.099	0.105	0.000	0.000	16.018	95270.135	10.939	10.939	0.7
postgres	wide	10000	csv	datasette		0.088	0.092	0.000	0.000	18.162	108276.203	9.328	9.328	0.7
postgres	wide	10000	tsv	native		0.029	0.033	0.000	0.000	51.151	304225.106	1.064	1.064	0.7
postgres	wide	10000	tsv	fallback		0.105	0.110	0.000	0.000	15.342	91248.543	11.428	11.428	0.7
postgres	wide	10000	tsv	datasette		0.088	0.092	0.000	0.000	18.190	108445.289	8.983	8.983	0.7
postgres	wide	10000	json	native		0.036	0.043	0.000	0.000	54.368	232625.623	1.255	1.255	0.7
postgres	wide	10000	json	fallback		0.082	0.088	0.000	0.000	26.504	113405.129	9.275	9.275	0.7
postgres	wide	10000	json	datasette		0.081	0.085	0.000	0.000	28.919	117217.957	43.100	43.100	0.7
postgres	wide	10000	ndjson	native		0.040	0.050	0.000	0.000	46.451	199614.975	1.273	1.273	0.7
postgres	wide	10000	ndjson	fallback		0.081	0.085	0.000	0.000	27.229	117011.287	7.863	7.863	0.7
postgres	wide	10000	ndjson	datasette		0.102	0.107	0.000	0.000	23.032	93738.995	69.231	69.231	0.7
postgres	narrow	10000	csv	native		0.022	0.024	0.000	0.000	8.655	409922.082	2.137	2.137	0.7
postgres	narrow	10000	csv	fallback		0.029	0.032	0.000	0.000	6.677	316237.862	3.728	3.728	0.7
postgres	narrow	10000	json	native		0.022	0.025	0.000	0.000	15.664	400393.330	1.342	1.342	0.7
postgres	narrow	10000	json	fallback		0.033	0.035	0.000	0.000	11.321	289386.610	3.664	3.664	0.7
cworker	wide	100000	csv	native	raw	0.077	0.417	0.365	0.000	41.062	239906.666	1.144	1.067	0.7
cworker	wide	100000	csv	native	frames	0.080	0.412	0.360	0.000	41.580	242933.104	1.987	1.043	0.7
cworker	wide	100000	csv	native	fifo	0.086	0.398	0.366	0.000	43.056	251559.409	2.835	1.110	0.7
cworker	wide	100000	csv	fallback		0.855	1.004	0.184	0.000	17.045	99586.014	18.824	18.032	0.7
cworker	wide	100000	csv	datasette		0.743	0.906	0.197	0.000	18.843	110350.743	17.569	16.618	0.7
cworker	wide	100000	csv	handoff		0.023	0.335	0.329	0.000	51.094	298482.320	4.218	0.857	0.7
cworker	wide	100000	tsv	native	raw	0.069	0.390	0.343	0.000	43.913	256565.232	1.791	1.119	0.7
cworker	wide	100000	tsv	fallback		0.776	0.926	0.185	0.000	18.481	107978.219	17.990	16.588	0.7
cworker	wide	100000	tsv	datasette		0.671	0.808	0.172	0.000	21.138	123788.079	15.656	15.633	0.7
cworker	wide	100000	json	native	raw	0.082	0.440	0.379	0.000	53.814	227322.006	1.748	1.125	0.7
cworker	wide	100000	json	native	frames	0.075	0.419	0.368	0.000	56.565	238945.212	1.962	1.068	0.7
cworker	wide	100000	json	native	fifo	0.090	0.421	0.389	0.000	56.212	237453.034	2.418	1.032	0.7
cworker	wide	100000	json	fallback		0.486	0.631	0.184	0.000	37.522	158500.563	10.918	9.813	0.7
cworker	wide	100000	json	datasette		0.638	0.773	0.180	0.000	32.303	129353.995	335.983	9.642	0.7
cworker	wide	100000	json	handoff		0.031	0.369	0.364	0.000	64.158	270982.891	3.597	0.666	0.7
cworker	wide	100000	ndjson	native	raw	0.094	0.443	0.376	0.000	53.239	225848.578	2.039	1.183	0.7
cworker	wide	100000	ndjson	fallback		0.442	0.579	0.175	0.000	40.748	172860.153	11.904	10.320	0.7
cworker	wide	100000	ndjson	datasette		0.856	0.995	0.181	0.000	24.996	100495.956	596.735	7.595	0.8
cworker	narrow	100000	csv	native	raw	0.012	0.078	0.072	0.000	31.072	1288610.890	1.982	1.982	0.8
cworker	narrow	100000	csv	fallback		0.193	0.252	0.078	0.000	9.577	397190.055	4.568	4.266	0.8
cworker	narrow	100000	json	native	raw	0.014	0.074	0.067	0.000	56.640	1344950.803	1.398	1.398	0.8
cworker	narrow	100000	json	fallback		0.197	0.259	0.080	0.000	16.271	386362.014	4.984	4.984	0.8
duckdb	wide	100000	csv	native		0.237	0.187	0.000	0.000	91.508	534646.351	5.113	1.220	0.8
duckdb	wide	100000	csv	fallback		0.944	0.930	0.000	0.000	18.401	107511.305	9.711	9.344	0.8
duckdb	wide	100000	csv	datasette		0.865	0.844	0.000	0.000	20.224	118435.036	9.874	7.781	0.8
duckdb	wide	100000	tsv	native		0.265	0.203	0.000	0.000	84.120	491476.737	2.526	2.494	0.8
duckdb	wide	100000	tsv	fallback		0.952	0.934	0.000	0.000	18.334	107119.681	10.049	9.766	0.9
duckdb	wide	100000	tsv	datasette		0.854	0.833	0.000	0.000	20.492	120004.081	8.498	7.967	0.9
duckdb	wide	100000	json	native		0.272	0.211	0.000	0.000	112.274	474271.492	4.200	1.580	0.9
duckdb	wide	100000	json	fallback		0.704	0.684	0.000	0.000	34.615	146222.412	6.020	5.894	0.9
duckdb	wide	100000	json	datasette		0.740	0.725	0.000	13.173	34.428	137861.912	370.806	3.432	0.9
duckdb	wide	100000	ndjson	native		0.259	0.187	0.000	0.000	126.096	534921.591	4.772	2.025	0.9
duckdb	wide	100000	ndjson	fallback		0.660	0.639	0.000	0.000	36.871	156414.347	9.343	5.784	0.9
duckdb	wide	100000	ndjson	datasette		1.047	1.028	0.000	22.725	24.205	97316.124	666.888	3.450	0.9
duckdb	narrow	100000	csv	native		0.102	0.100	0.000	0.000	24.132	1000811.328	4.008	4.008	0.9
duckdb	narrow	100000	csv	fallback		0.256	0.255	0.000	0.000	9.449	391863.253	3.157	2.472	0.9
duckdb	narrow	100000	json	native		0.127	0.125	0.000	0.000	33.701	800245.483	4.294	4.294	0.9
duckdb	narrow	100000	json	fallback		0.246	0.244	0.000	0.000	17.257	409788.681	2.993	2.485	0.9
postgres	wide	100000	csv	native		0.260	0.293	0.000	0.000	58.396	341186.754	0.904	0.665	1.0
postgres	wide	100000	csv	fallback		0.972	1.007	0.000	0.000	16.993	99286.042	13.059	12.697	1.0
postgres	wide	100000	csv	datasette		0.900	0.936	0.000	0.000	18.252	106891.095	11.740	11.451	1.0
postgres	wide	100000	tsv	native		0.264	0.298	0.000	0.000	57.522	336078.616	1.242	0.780	1.0
postgres	wide	100000	tsv	fallback		0.921	0.960	0.000	0.000	17.836	104208.213	11.359	11.321	1.0
postgres	wide	100000	tsv	datasette		0.854	0.889	0.000	0.000	19.207	112483.677	10.648	10.262	1.0
postgres	wide	100000	json	native		0.312	0.377	0.000	0.000	62.759	265107.450	1.013	0.476	1.0
postgres	wide	100000	json	fallback		0.701	0.738	0.000	0.000	32.072	135480.672	8.903	8.301	1.0
postgres	wide	100000	json	datasette		0.751	0.799	0.000	0.000	31.269	125210.432	396.297	5.926	1.0
postgres	wide	100000	ndjson	native		0.322	0.383	0.000	0.000	61.493	260863.838	0.996	0.552	1.0
postgres	wide	100000	ndjson	fallback		0.628	0.666	0.000	0.000	35.392	150140.547	8.080	7.539	1.0
postgres	wide	100000	ndjson	datasette		1.070	1.109	0.000	0.000	22.420	90137.533	741.203	6.149	1.0
postgres	narrow	100000	csv	native		0.216	0.235	0.000	0.000	10.256	425345.890	2.478	2.478	1.0
postgres	narrow	100000	csv	fallback		0.253	0.275	0.000	0.000	8.779	364103.198	3.476	3.423	1.0
postgres	narrow	100000	json	native		0.181	0.196	0.000	0.000	21.433	508946.169	1.376	1.275	1.0
postgres	narrow	100000	json	fallback		0.237	0.255	0.000	0.000	16.523	392338.028	3.059	2.344	1.0
cworker	wide	1000000	csv	native	raw	0.470	3.649	3.291	0.000	47.725	274036.810	6.502	1.110	1.0
cworker	wide	1000000	csv	native	frames	0.511	3.542	3.205	0.000	49.170	282331.199	2.204	1.099	1.0
cworker	wide	1000000	csv	native	fifo	0.703	3.672	3.406	0.000	47.432	272351.369	3.054	1.101	1.0
cworker	wide	1000000	csv	fallback		7.150	8.534	1.703	0.000	20.408	117181.484	19.317	16.147	1.0
cworker	wide	1000000	csv	datasette		6.557	7.937	1.677	0.000	21.893	125998.057	16.742	14.504	1.0
cworker	wide	1000000	csv	handoff		0.172	3.212	3.187	0.000	54.225	311318.226	3.542	0.251	1.0
cworker	wide	1000000	json	native	raw	0.557	3.776	3.353	0.000	63.480	264800.745	1.427	1.037	1.0
cworker	wide	1000000	json	native	frames	0.592	3.874	3.439	0.000	61.885	258148.666	1.968	1.043	1.0
cworker	wide	1000000	json	native	fifo	0.747	3.652	3.450	0.000	65.647	273838.679	1.251	1.004	1.0
cworker	wide	1000000	json	fallback		4.245	5.653	1.759	0.000	42.410	176909.151	11.648	9.510	1.0
cworker	wide	1000000	json	datasette		6.295	7.710	1.819	981.742	32.779	129699.882	2667.241	7.364	1.0
cworker	wide	1000000	json	handoff		0.209	3.539	3.515	0.000	67.755	282593.187	3.438	0.418	1.0
cworker	narrow	1000000	csv	native	raw	0.133	0.802	0.712	0.000	33.817	1247278.767	1.576	1.092	1.0
cworker	narrow	1000000	csv	fallback		2.058	2.644	0.769	0.000	10.256	378272.077	5.977	4.423	1.0
cworker	narrow	1000000	json	native	raw	0.136	0.780	0.687	0.000	57.841	1282146.340	1.719	0.943	1.0
cworker	narrow	1000000	json	fallback		2.192	2.762	0.760	0.000	16.333	362051.703	5.839	5.013	1.0
duckdb	wide	1000000	csv	native		2.615	1.401	0.000	122.806	124.304	713753.204	3.052	1.509	1.0
duckdb	wide	1000000	csv	fallback		11.044	10.748	0.000	109.044	16.204	93044.746	19.622	11.191	0.9
duckdb	wide	1000000	csv	datasette		10.179	9.884	0.000	109.142	17.580	101175.884	11.858	9.861	0.9
duckdb	wide	1000000	json	native		2.872	1.550	0.000	130.224	154.619	644978.011	3.424	2.434	0.9
duckdb	wide	1000000	json	fallback		8.082	7.753	0.000	109.670	30.923	128990.572	9.620	7.843	0.8
duckdb	wide	1000000	json	datasette		9.104	8.850	0.000	1259.262	28.556	112990.593	4177.892	3.790	0.9
duckdb	narrow	1000000	csv	native		0.867	0.507	0.000	0.000	53.446	1971299.601	13.368	1.409	0.9
duckdb	narrow	1000000	csv	fallback		2.400	2.303	0.000	0.000	11.775	434300.920	4.781	2.333	0.9
duckdb	narrow	1000000	json	native		0.982	0.529	0.000	0.000	85.321	1891303.601	2.905	1.196	0.9
duckdb	narrow	1000000	json	fallback		2.841	2.731	0.000	0.000	16.518	366144.534	3.455	2.935	1.0
postgres	wide	1000000	csv	native		2.733	2.946	0.000	0.000	59.106	339385.972	1.212	0.696	1.0
postgres	wide	1000000	csv	fallback		9.146	9.366	0.000	0.000	18.595	106769.966	12.193	11.503	1.0
postgres	wide	1000000	csv	datasette		8.528	8.749	0.000	0.000	19.861	114301.220	11.661	10.789	1.1
postgres	wide	1000000	json	native		3.575	4.061	0.000	0.000	59.027	246226.084	1.219	0.562	1.2
postgres	wide	1000000	json	fallback		7.054	7.283	0.000	0.000	32.914	137298.927	22.199	8.607	1.1
postgres	wide	1000000	json	datasette		7.747	7.960	0.000	979.661	31.749	125626.532	3665.930	5.742	1.1
postgres	narrow	1000000	csv	native		2.025	2.224	0.000	0.000	12.190	449624.067	2.910	2.252	1.1
postgres	narrow	1000000	csv	fallback		2.266	2.461	0.000	0.000	11.016	406307.441	3.235	2.277	1.1
postgres	narrow	1000000	json	native		2.013	2.222	0.000	0.000	20.307	450137.754	2.165	1.815	1.1
postgres	narrow	1000000	json	fallback		2.318	2.516	0.000	0.000	17.932	397502.636	2.506	2.225	1.1
```
</details>

## Multi-tenant SQLite workers under a non-Python load generator (C against Rust)

`loadgen`: **ok** in 388.4 s, load average (1 min) 0.47 before, 7.18 after

| codec | clients | C req/s | Rust req/s | Rust / C | C worker CPU (cores) | Rust worker CPU (cores) | C CPU µs/req | Rust CPU µs/req |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| msgpack | 1 | 24,684 | 24,773 | 1.00 | 0.73 | 0.74 | 29.4 | 30.1 |
| msgpack | 4 | 58,445 | 84,376 | 1.44 | 2.55 | 2.77 | 43.7 | 33.3 |
| msgpack | 16 | 65,105 | 103,793 | 1.59 | 4.51 | 5.14 | 70.1 | 49.5 |
| msgpack | 64 | 34,917 | 112,594 | 3.22 | 3.23 | 6.23 | 92.6 | 55.3 |
| json | 1 | 23,860 | 22,518 | 0.94 | 0.72 | 0.74 | 30.4 | 32.2 |
| json | 4 | 60,070 | 76,975 | 1.28 | 2.62 | 2.73 | 43.6 | 35.1 |
| json | 16 | 62,444 | 95,025 | 1.52 | 4.50 | 4.79 | 72.1 | 50.4 |
| json | 64 | 31,259 | 110,048 | 3.52 | 3.04 | 6.14 | 98.6 | 55.8 |

## Memory limits (RLIMIT_AS, heap limits) for each worker that claims them

`rlimit`: **failed** in 24.7 s, load average (1 min) 2.63 before, 2.08 after

| test | outcome |
| --- | --- |
| `tests/faults/test_duckdb_worker_faults.py::test_duckdb_memory_limit_applies_inside_the_worker[duckdb-worker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_duckdb_memory_limit_applies_inside_the_worker[duckdb-cppworker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_duckdb_memory_limit_applies_inside_the_worker[duckdb-rsworker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_memory_limit_capability[duckdb-worker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_memory_limit_capability[duckdb-cppworker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_memory_limit_capability[duckdb-rsworker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_rlimit_as_bounds_the_whole_worker[duckdb-worker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_rlimit_as_bounds_the_whole_worker[duckdb-cppworker]` | PASSED |
| `tests/faults/test_duckdb_worker_faults.py::test_rlimit_as_bounds_the_whole_worker[duckdb-rsworker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged[py-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged[c-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged[c-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged[rust-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_multi_gigabyte_value_stopped_by_heap_limit_parent_memory_unchanged[node-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_huge_single_allocation_stopped_by_heap_limit[py-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_huge_single_allocation_stopped_by_heap_limit[c-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_huge_single_allocation_stopped_by_heap_limit[c-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_huge_single_allocation_stopped_by_heap_limit[rust-pool]` | FAILED |
| `tests/faults/test_worker_limits.py::test_huge_single_allocation_stopped_by_heap_limit[node-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_memory_limit_capability_is_linux_only[py-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_memory_limit_capability_is_linux_only[c-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_memory_limit_capability_is_linux_only[c-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_memory_limit_capability_is_linux_only[rust-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_memory_limit_capability_is_linux_only[node-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_stops_python_side_allocation[py-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_stops_python_side_allocation[c-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_stops_python_side_allocation[c-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_stops_python_side_allocation[rust-pool]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_stops_python_side_allocation[node-worker]` | SKIPPED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_in_init_hook_function[py-worker]` | PASSED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_in_init_hook_function[c-worker]` | SKIPPED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_in_init_hook_function[c-pool]` | SKIPPED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_in_init_hook_function[rust-pool]` | SKIPPED |
| `tests/faults/test_worker_limits.py::test_rlimit_as_in_init_hook_function[node-worker]` | SKIPPED |
| `tests/worker/test_cworker_multi.py::test_heap_limit_is_process_wide_and_one_tenant_recovers[c]` | PASSED |
| `tests/worker/test_cworker_multi.py::test_heap_limit_is_process_wide_and_one_tenant_recovers[rust]` | PASSED |
| `tests/worker/test_duckdb_worker.py::test_memory_limit_and_threads_apply_inside_the_worker[duckdb-worker]` | PASSED |
| `tests/worker/test_duckdb_worker.py::test_memory_limit_and_threads_apply_inside_the_worker[duckdb-cppworker]` | PASSED |
| `tests/worker/test_duckdb_worker.py::test_memory_limit_and_threads_apply_inside_the_worker[duckdb-rsworker]` | PASSED |
| `tests/worker/test_nodeworker.py::test_sql_limits_and_heap_limit` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[1]` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[2]` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[4]` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[8]` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[16]` | PASSED |
| `tests/worker/test_nodeworker.py::test_rlimit_as_against_v8[32]` | PASSED |
| `tests/test_sqlite_apsw.py::test_memory_limit_stops_a_huge_value` | PASSED |
| `tests/test_clickhouse_backend.py::test_memory_limit` | PASSED |

pytest: `1 failed, 42 passed, 5 skipped, 985 deselected in 23.53s`

- SKIPPED [1] tests/faults/test_worker_limits.py:85: the Node worker does not use RLIMIT_AS (V8 reserves more address space than any useful limit; its WebAssembly memory maximum caps SQLite instead)
- SKIPPED [3] tests/faults/conftest.py:74: init hooks need the Python worker
- SKIPPED [1] tests/faults/test_worker_limits.py:109: the Node worker does not use RLIMIT_AS (V8 reserves more address space than any useful limit; its WebAssembly memory maximum caps SQLite instead)

## Scale backends: wake cost and query cost, separately

`scale_wake_query`: **ok** in 785.4 s, load average (1 min) 0.76 before, 0.96 after

| backend | wake_ms | warm_median_ms | warm_p90_ms | cold_median_ms | cold_p90_ms | sleep_median_ms | load1_before | load1_after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sqlite | 0.421 | 0.213 | 0.278 | 0.634 | 0.821 | 0.233 | 0.76 | 0.78 |
| sqlite-wasm | 1.517 | 0.342 | 0.484 | 1.859 | 2.552 | 0.330 | 0.78 | 0.78 |
| duckdb | 22.947 | 1.271 | 1.966 | 24.218 | 27.924 | 5.049 | 0.78 | 0.85 |
| sqlite-worker | 129.798 | 0.483 | 0.636 | 130.281 | 145.488 | 0.185 | 0.85 | 0.98 |
| sqlite-cworker | 1.754 | 0.141 | 0.205 | 1.895 | 2.364 | 0.127 | 0.98 | 0.98 |
| sqlite-nodeworker | 174.326 | 1.027 | 1.550 | 175.354 | 196.205 | 1.674 | 0.98 | 1.54 |
| sqlite-cpool | 2.152 | 0.175 | 0.258 | 2.327 | 2.988 | 0.287 | 1.54 | 1.57 |
| sqlite-rspool | 2.646 | 0.192 | 0.294 | 2.839 | 3.738 | 0.334 | 1.57 | 1.45 |
| duckdb-worker | 230.098 | 1.709 | 2.573 | 231.808 | 257.661 | 13.893 | 1.45 | 0.95 |
| duckdb-cppworker | 55.823 | 1.388 | 1.892 | 57.211 | 64.409 | 9.017 | 0.95 | 1.07 |
| duckdb-rsworker | 51.665 | 1.366 | 1.813 | 53.031 | 59.117 | 12.810 | 1.07 | 0.96 |
| pglite | RuntimeError: asyncio.run() cannot be called from a running event loop | | | | | | | |

## Scale tests (tests/scale, through their harness)

`scale_tests`: **ok** in 480.9 s, load average (1 min) 0.49 before, 45.04 after

```
[scale:sqlite] 10000 databases, created in 0.8s
  register: 8.0 us/db; best-of-3 2.29 us/db at 1000, 2.38 us/db at 10000
  asleep, never used: 463 B/db (tracemalloc), 583 B/db (RSS delta)
  asleep after a wake/sleep cycle: +3.6 B/db growth (tracemalloc, 1000 dbs)
  load: 20000 requests (10% writes) over 3363 databases, Zipf s=1.1, 96 workers, in 10.2s (1963 req/s)
  fds: baseline 9, peak 77, limit 97 (cap 64 x 1 per place + allowance 24), after sleep 9; 1606 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=11128 sleeps=11077 cap_evictions=14958 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-wasm] 1000 databases, created in 0.1s
  register: 99.2 us/db; best-of-3 20.29 us/db at 100, 19.78 us/db at 1000
  asleep, never used: 903 B/db (tracemalloc), 397 B/db (RSS delta)
  asleep after a wake/sleep cycle: +63.1 B/db growth (tracemalloc, 1000 dbs)
  load: 20000 requests (10% writes) over 956 databases, Zipf s=1.1, 96 workers, in 33.8s (592 req/s)
  fds: baseline 11, peak 68, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 2559 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=8859 sleeps=8806 cap_evictions=12894 idle_closes=0; after idle timeout: awake=0 open=0

[scale:duckdb] 1000 databases, created in 0.3s
  register: 47.0 us/db; best-of-3 10.08 us/db at 100, 9.14 us/db at 1000
  asleep, never used: 1283 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +407.7 B/db growth (tracemalloc, 1000 dbs)
  load: 3000 requests (10% writes) over 514 databases, Zipf s=1.1, 96 workers, in 12.7s (236 req/s)
  fds: baseline 11, peak 79, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 2034 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=1356 sleeps=1325 cap_evictions=1835 idle_closes=238; after idle timeout: awake=0 open=0

[scale:sqlite-worker] 1000 databases, created in 0.1s
  register: 107.8 us/db; best-of-3 17.40 us/db at 100, 17.11 us/db at 1000
  asleep, never used: 786 B/db (tracemalloc), 213 B/db (RSS delta)
  asleep after a wake/sleep cycle: +153.3 B/db growth (tracemalloc, 200 dbs)
  load: 2000 requests (10% writes) over 416 databases, Zipf s=1.1, 96 workers, in 31.6s (63 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1051 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=896 sleeps=866 cap_evictions=1399 idle_closes=52; after idle timeout: awake=0 open=0

[scale:sqlite-cworker] 1000 databases, created in 0.1s
  register: 155.0 us/db; best-of-3 35.24 us/db at 100, 31.45 us/db at 1000
  asleep, never used: 791 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +64.1 B/db growth (tracemalloc, 1000 dbs)
  load: 10000 requests (10% writes) over 851 databases, Zipf s=1.1, 96 workers, in 10.5s (951 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 867 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=4481 sleeps=4433 cap_evictions=6339 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-nodeworker] 1000 databases, created in 0.1s
  register: 206.7 us/db; best-of-3 56.03 us/db at 100, 51.36 us/db at 1000
  asleep, never used: 1492 B/db (tracemalloc), 127 B/db (RSS delta)
  asleep after a wake/sleep cycle: +1569.3 B/db growth (tracemalloc, 200 dbs)
  load: 2000 requests (10% writes) over 416 databases, Zipf s=1.1, 96 workers, in 43.0s (47 req/s)
  fds: baseline 11, peak 179, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1946 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=910 sleeps=880 cap_evictions=1501 idle_closes=125; after idle timeout: awake=0 open=0

[scale:sqlite-cpool] 1000 databases, created in 0.1s
  register: 172.7 us/db; best-of-3 37.49 us/db at 100, 35.35 us/db at 1000
  asleep, never used: 986 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +46.1 B/db growth (tracemalloc, 1000 dbs)
  load: 50000 requests (10% writes) over 1000 databases, Zipf s=1.1, 96 workers, in 8.7s (5727 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1830 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=21761 sleeps=21715 cap_evictions=31372 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-rspool] 1000 databases, created in 0.1s
  register: 182.7 us/db; best-of-3 41.18 us/db at 100, 38.92 us/db at 1000
  asleep, never used: 984 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +30.7 B/db growth (tracemalloc, 1000 dbs)
  load: 50000 requests (10% writes) over 1000 databases, Zipf s=1.1, 96 workers, in 8.3s (6011 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1770 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=21788 sleeps=21738 cap_evictions=31471 idle_closes=0; after idle timeout: awake=0 open=0

[threads:sqlite] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-wasm] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:duckdb] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-worker] standalone, 100 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-cworker] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-nodeworker] standalone, 100 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-cpool] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-rspool] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite] registry: 13 before, 13-13 during, 13 after, new: []

[threads:sqlite-wasm] registry: 14 before, 14-14 during, 14 after, new: []

[threads:duckdb] registry: 14 before, 14-14 during, 14 after, new: []

[threads:sqlite-worker] registry: 10 before, 9-10 during, 9 after, new: []

[threads:sqlite-cworker] registry: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-nodeworker] registry: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-cpool] registry: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-rspool] registry: 9 before, 9-9 during, 9 after, new: []
```

pytest: `24 passed, 1 deselected in 480.16s (0:08:00)`
