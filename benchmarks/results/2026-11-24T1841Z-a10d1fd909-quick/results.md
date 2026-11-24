# dblayer benchmarks: 2026-11-24T1841Z-a10d1fd909-quick

**TRIAL 2 of the Fly.io mechanism (--quick, precompiled stdlib): not final numbers**

- Mode: quick; started 2026-11-24T18:41:47+00:00
- Commit: `a10d1fd909949b0037f68055c3f263791f41be44` (a10d1fd 2026-11-24T10:28:19-08:00 Notes: Rust multi-tenant SQLite worker final report; C against Rust)
- Machine: AMD EPYC, 8 CPUs, 15.6 GB, kernel 6.12.118-fly, x86_64, Fly performance-8x in sjc
- Data filesystem: `/dev/vdc ext4 rw,relatime`; temporary files: `/dev/vdc ext4`
- Versions: python 3.14.8; sqlite (python sqlite3) 3.53.1; uv uv 0.13.0 (x86_64-unknown-linux-gnu); node v24.22.3; rustc rustc 1.100.0 (0ef1bc386 2026-11-09); cargo cargo 1.100.0 (3acc79c0c 2026-11-05); cc cc (Debian 14.2.0-19) 14.2.0; postgres (psql client) psql (PostgreSQL) 18.7 (Debian 18.7-1.pgdg13+5); duckdb 2.0.1; asyncpg 0.32.0; psycopg 3.3.9; psycopg-binary 3.3.9; apsw 3.54.1.0; wasmtime 51.0.0; chdb 4.5.1; msgpack 1.2.4; pytest 9.2.0; starlette 1.10.0; uvicorn 0.54.0; postgres server 18.7 (Debian 18.7-1.pgdg13+5); libpq 180006; sqlite (C worker) dblayer-cworker protocol 1, SQLite 3.53.1; sqlite-rsworker dblayer-sqlite-rsworker protocol 1, SQLite 3.53.1 (-O2 -g -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_DQS=3); duckdb-rsworker dblayer-duckdb-rsworker protocol 1, DuckDB v2.0.1; duckdb-cppworker dblayer-duckdb-worker protocol 1, DuckDB v2.0.1

Every topic ran alone, one after another, in this one session. Compare figures within this file only.

| topic | status | seconds | load before | load after | reason |
| --- | --- | --- | --- | --- | --- |
| worker_overhead | ok | 64.1 | 1.71 | 1.33 |  |
| cpool | ok | 18.6 | 1.33 | 1.42 |  |
| rspool | ok | 41.5 | 1.42 | 1.56 |  |
| wasm | ok | 58.7 | 1.56 | 1.16 |  |
| nodeworker | ok | 15.0 | 1.16 | 1.05 |  |
| duckdb_memory | ok | 193.3 | 1.05 | 1.14 |  |
| postgres_drivers | ok | 23.8 | 1.14 | 1.23 |  |
| pglite | ok | 68.2 | 1.23 | 1.34 |  |
| clickhouse | ok | 78.2 | 1.34 | 0.94 |  |
| export | ok | 37.2 | 0.94 | 1.05 |  |
| scale_wake_query | ok | 39.1 | 1.05 | 1.11 |  |
| scale_tests | ok | 487.1 | 1.11 | 39.6 |  |

## Worker process boundary against in-process SQLite

`worker_overhead`: **ok** in 64.1 s, load average (1 min) 1.71 before, 1.33 after

<details><summary>output</summary>

```
SQLite worker overhead (python 3.14.8)

One-row read by primary key, round trip (median / p90 us); one-row update (median us)
  in-process           read    180.1 /    238.8   write   1314.0
  apsw                 read    235.5 /    316.7   write   1132.3
  apsw-inline          read     51.5 /     71.9   write   1144.7
  worker-json          read    484.4 /    608.7   write   1477.1
  worker-msgpack       read    610.2 /   1175.4   write   2932.6
  cworker-json         read    156.0 /    219.4   write   1280.0
  cworker-msgpack      read    128.1 /    176.0   write   1055.4
  cpool-json           read    149.6 /    198.1   write   1040.9
  cpool-msgpack        read    147.1 /    253.2   write   1405.7
  nodeworker-json      read    616.5 /    885.3   write   1973.8
  nodeworker-msgpack   read    574.4 /    812.8   write   1996.3

Read of 1,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute      2.02 ms   2.02 us/row   stream      2.54 ms   2.54 us/row
  apsw                 execute      1.12 ms   1.12 us/row   stream      1.56 ms   1.56 us/row
  apsw-inline          execute      2.10 ms   2.10 us/row   stream      2.35 ms   2.35 us/row
  worker-json          execute      6.58 ms   6.58 us/row   stream      5.06 ms   5.06 us/row
  worker-msgpack       execute      4.18 ms   4.18 us/row   stream      3.55 ms   3.55 us/row
  cworker-json         execute      3.61 ms   3.61 us/row   stream      4.22 ms   4.22 us/row
  cworker-msgpack      execute      1.35 ms   1.35 us/row   stream      1.33 ms   1.33 us/row
  cpool-json           execute      3.03 ms   3.03 us/row   stream      3.02 ms   3.02 us/row
  cpool-msgpack        execute      1.36 ms   1.36 us/row   stream      1.38 ms   1.38 us/row
  nodeworker-json      execute      6.96 ms   6.96 us/row   stream      6.51 ms   6.51 us/row
  nodeworker-msgpack   execute      5.05 ms   5.05 us/row   stream      3.75 ms   3.75 us/row

Read of 100,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute    178.07 ms   1.78 us/row   stream    150.72 ms   1.51 us/row
  apsw                 execute    139.84 ms   1.40 us/row   stream    152.15 ms   1.52 us/row
  apsw-inline          execute    142.66 ms   1.43 us/row   stream    153.63 ms   1.54 us/row
  worker-json          execute    650.14 ms   6.50 us/row   stream    578.41 ms   5.78 us/row
  worker-msgpack       execute    441.30 ms   4.41 us/row   stream    271.68 ms   2.72 us/row
  cworker-json         execute    399.62 ms   4.00 us/row   stream    326.47 ms   3.26 us/row
  cworker-msgpack      execute    284.83 ms   2.85 us/row   stream    143.45 ms   1.43 us/row
  cpool-json           execute    429.58 ms   4.30 us/row   stream    315.26 ms   3.15 us/row
  cpool-msgpack        execute    236.56 ms   2.37 us/row   stream     94.80 ms   0.95 us/row
  nodeworker-json      execute    521.45 ms   5.21 us/row   stream    434.41 ms   4.34 us/row
  nodeworker-msgpack   execute    308.67 ms   3.09 us/row   stream    193.11 ms   1.93 us/row

Insert with alter (2 rows, add one column): median us
  in-process           interactive transaction   2896.5   one named operation   2940.8
  apsw                 interactive transaction   2977.2   one named operation   2811.0
  apsw-inline          interactive transaction   3782.4   one named operation   3878.9
  worker-json          interactive transaction   8298.6   one named operation   6585.3
  worker-msgpack       interactive transaction   6190.8   one named operation   5588.2
  cworker-json         interactive transaction   3219.3   one named operation   3052.9
  cworker-msgpack      interactive transaction   3482.5   one named operation   3695.6
  cpool-json           interactive transaction   3751.8   one named operation   3825.8
  cpool-msgpack        interactive transaction   3190.8   one named operation   3471.1
  nodeworker-json      interactive transaction   9168.5   one named operation   9471.8
  nodeworker-msgpack   interactive transaction   7910.3   one named operation   8051.9

SQLite versions: in-process sqlite3 3.53.1; apsw 3.54.1.0 bundles SQLite 3.54.1
Connection open (median us), execute_many of 100,000 rows in a transaction (ms), memory per open connection (KB of process RSS; SQLite's own bytes)
  in-process           open    226.7   execute_many    397.1 ms   rss  168.0 KB/conn
  apsw                 open    272.0   execute_many    392.6 ms   rss  185.6 KB/conn (db_status 21.4 KB)
  cworker-json         open   1892.5   execute_many    470.2 ms
  cworker-msgpack      open   2204.9   execute_many    366.0 ms

Multi-tenant C pool (load average 1.4 0.8 0.4 at start, 1.3 after; provisional)
  new database on a warm pool: first result 0.94 ms; bare open round trip 0.195 ms
  memory per open database inside the process: 170.4 KB footprint, 170.4 KB RSS (100 databases; process base 1.7 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.3 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    4996 (1.02; 0.00; 0p; 9fd)   4:    3753 (1.35; 0.00; 0p; 12fd)  16:    3345 (1.51; 0.00; 0p; 24fd)  64:    3310 (1.48; 0.00; 0p; 72fd)
    cworker     1:    6075 (0.67; 0.00; 1p; 11fd)   4:    9833 (0.94; 0.00; 4p; 20fd)  16:   11934 (0.97; 0.00; 16p; 56fd)  64:   12572 (0.99; 0.00; 64p; 200fd)
    cpool       1:    6985 (0.75; 0.00; 1p; 11fd)   4:   11867 (0.99; 0.00; 2p; 14fd)  16:   13343 (0.99; 0.00; 2p; 14fd)  64:   13046 (0.98; 0.00; 2p; 14fd)

Open to first result of a new Database (ms)
  in-process 0.65; worker dedicated 171.0 (a process per connection); shared: first 144.9, then 1.37 on the running process
  C worker dedicated 2.0; shared: first 2.2, then 0.45 on the running process

Start-up breakdown, raw pipes (median ms)
  spawn and wait for /usr/bin/true 0.65; python -S -c pass 11.5; python -c pass 29.6; dblayer-cworker --version 1.08
  python  spawn to hello  120.61  open  15.90  first query   0.92  (second  0.42)  total  137.82
  c       spawn to hello    1.50  open   0.35  first query   0.31  (second  0.15)  total    2.29

Resident memory of one idle worker with a database open (KB)
  python 27880; c 3540
```
</details>

## Multi-tenant C pool (sqlite-cpool)

`cpool`: **ok** in 18.6 s, load average (1 min) 1.33 before, 1.42 after

<details><summary>output</summary>

```
Multi-tenant C pool (load average 1.3 0.9 0.4 at start, 1.4 after; provisional)
  new database on a warm pool: first result 1.17 ms; bare open round trip 0.479 ms
  memory per open database inside the process: 170.4 KB footprint, 170.4 KB RSS (100 databases; process base 1.8 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.3 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    4831 (1.01; 0.00; 0p; 9fd)   4:    3398 (1.30; 0.00; 0p; 12fd)  16:    3888 (1.52; 0.00; 0p; 24fd)  64:    4052 (1.54; 0.00; 0p; 72fd)
    cworker     1:    7970 (0.77; 0.00; 1p; 11fd)   4:   11267 (0.98; 0.00; 4p; 20fd)  16:   11870 (0.97; 0.00; 16p; 56fd)  64:   12067 (0.98; 0.00; 64p; 200fd)
    cpool       1:    6216 (0.71; 0.00; 1p; 11fd)   4:   11002 (0.99; 0.00; 2p; 14fd)  16:   10479 (0.95; 0.00; 2p; 14fd)  64:   10817 (0.95; 0.00; 2p; 14fd)
```
</details>

## Multi-tenant C worker against its Rust port (sqlite-cpool, sqlite-rspool)

`rspool`: **ok** in 41.5 s, load average (1 min) 1.42 before, 1.56 after

| Measure (median over rounds) | C worker | Rust worker | Rust / C |
| --- | --- | --- | --- |
| Warm open to first result (ms) | 1.15 | 1.33 | 1.16 |
| Bare open round trip (ms) | 0.234 | 0.338 | 1.44 |
| One-row read (µs) | 440 | 349 | 0.79 |
| One-row update (µs) | 2.07e+03 | 1.58e+03 | 0.77 |
| execute 1,000 rows, msgpack (µs/row) | 2.15 | 1.6 | 0.75 |
| execute 100,000 rows, msgpack (µs/row) | 2.92 | 2.14 | 0.73 |
| stream 1,000 rows, msgpack (µs/row) | 1.85 | 1.48 | 0.80 |
| stream 100,000 rows, msgpack (µs/row) | 1.49 | 1.43 | 0.96 |
| execute 1,000 rows, JSON (µs/row) | 3.04 | 2.32 | 0.76 |
| execute 100,000 rows, JSON (µs/row) | 3.6 | 2.92 | 0.81 |
| stream 1,000 rows, JSON (µs/row) | 2.98 | 2.51 | 0.84 |
| stream 100,000 rows, JSON (µs/row) | 2.83 | 2.16 | 0.76 |
| Process base footprint (KB) | 840 | 1.42e+03 | 1.69 |
| Process base RSS (KB) | 2.51e+03 | 3.24e+03 | 1.29 |
| Footprint per open database (KB) | 171 | 169 | 0.99 |
| 1 clients: requests/s | 6.48e+03 | 6.23e+03 | 0.96 |
| 1 clients: parent CPU (cores) | 0.721 | 0.696 | 0.97 |
| 1 clients: worker CPU (cores) | 0 | 0 | nan |
| 4 clients: requests/s | 9.63e+03 | 9.58e+03 | 1.00 |
| 4 clients: parent CPU (cores) | 0.942 | 0.947 | 1.01 |
| 4 clients: worker CPU (cores) | 0 | 0 | nan |
| 16 clients: requests/s | 1.22e+04 | 1.1e+04 | 0.91 |
| 16 clients: parent CPU (cores) | 0.963 | 0.938 | 0.97 |
| 16 clients: worker CPU (cores) | 0 | 0 | nan |
| 64 clients: requests/s | 1.07e+04 | 1.17e+04 | 1.09 |
| 64 clients: parent CPU (cores) | 0.921 | 0.963 | 1.05 |
| 64 clients: worker CPU (cores) | 0 | 0 | nan |

## SQLite in WebAssembly (sqlite-wasm)

`wasm`: **ok** in 58.7 s, load average (1 min) 1.56 before, 1.16 after

| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.60 | 0.80 | 0.90 | 135.49 | 2.41 | 2.46 | 3.55 | 3.14 |
| One-row read, median (us) | 161 | 184 | 38 | 403 | 145 | 139 | 400 | 208 |
| One-row read, p90 (us) | 223 | 267 | 58 | 507 | 217 | 200 | 541 | 250 |
| One-row update (us) | 1063 | 941 | 1125 | 1671 | 1063 | 1215 | 2514 | 1912 |
| Per row, 1,000-row read (us) | 1.68 | 1.75 | 1.68 | 2.67 | 1.87 | 1.28 | 2.50 | 1.66 |
| Per row, 100,000-row read (us) | 1.64 | 1.19 | 1.39 | 3.58 | 2.13 | 2.73 | 2.71 | 2.73 |
| Per row, 100,000-row stream (us) | 1.36 | 1.12 | 1.48 | 2.35 | 1.28 | 1.16 | 1.89 | 1.85 |
| execute_many, 100,000 rows (ms) | 372 | 425 | 409 | 452 | 365 | 382 | 518 | 477 |
| Memory per open connection, this process (KB) | 189 | 229 | 226 | -22 | 38 | 19 | 514 | 514 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 16236 | 376 | 5334 | 0 | 0 |

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 251 | 519 | 697 | 685 |
| sort_200k | 111 | 256 | 280 | 279 |
| instr_quadratic | 165 | 386 | 362 | 336 |

<details><summary>output</summary>

```
load average at start 1.6/1.0/0.5, at end 1.2/1.0/0.5 (provisional if high)
module: 1740 KB; compile 602 ms; load cached compilation 13.7 ms; import wasmtime 129 ms
| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.60 | 0.80 | 0.90 | 135.49 | 2.41 | 2.46 | 3.55 | 3.14 |
| One-row read, median (us) | 161 | 184 | 38 | 403 | 145 | 139 | 400 | 208 |
| One-row read, p90 (us) | 223 | 267 | 58 | 507 | 217 | 200 | 541 | 250 |
| One-row update (us) | 1063 | 941 | 1125 | 1671 | 1063 | 1215 | 2514 | 1912 |
| Per row, 1,000-row read (us) | 1.68 | 1.75 | 1.68 | 2.67 | 1.87 | 1.28 | 2.50 | 1.66 |
| Per row, 100,000-row read (us) | 1.64 | 1.19 | 1.39 | 3.58 | 2.13 | 2.73 | 2.71 | 2.73 |
| Per row, 100,000-row stream (us) | 1.36 | 1.12 | 1.48 | 2.35 | 1.28 | 1.16 | 1.89 | 1.85 |
| execute_many, 100,000 rows (ms) | 372 | 425 | 409 | 452 | 365 | 382 | 518 | 477 |
| Memory per open connection, this process (KB) | 189 | 229 | 226 | -22 | 38 | 19 | 514 | 514 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 16236 | 376 | 5334 | 0 | 0 |
host VFS: full scan of 4010 pages through the host 20.3 ms, from a warm page cache 3.4 ms (native sqlite3, cache_size 10: 3.6 ms): 4.21 us per page read; a one-row read makes 6 host calls {'lock': 1, 'access': 2, 'read': 1, 'file_size': 1, 'unlock': 1}

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 251 | 519 | 697 | 685 |
| sort_200k | 111 | 256 | 280 | 279 |
| instr_quadratic | 165 | 386 | 362 | 336 |
```
</details>

## SQLite-WASM in a Node.js worker (sqlite-nodeworker)

`nodeworker`: **ok** in 15.0 s, load average (1 min) 1.16 before, 1.05 after

<details><summary>output</summary>

```
Node worker start-up and memory (load average 1.2 1.0 0.5 at start, 1.1 1.0 0.5 at end)

Spawn and wait: /usr/bin/true 0.9 ms; node -e 0 32.0 ms

Raw pipes, median ms: spawn to hello, open, first query (second), total
  python                   hello  115.0  open   15.3  first  0.81  ( 0.36)  total  131.1
  node                     hello   51.9  open  110.3  first  3.47  ( 0.96)  total  166.3
  node, no compile cache   hello   53.3  open  124.2  first  3.76  ( 1.10)  total  181.2
  node, no spare           hello   60.5  open  140.3  first  3.83  ( 1.07)  total  204.9
  c                        hello    1.4  open    0.3  first  0.30  ( 0.12)  total    2.0

Inside a Node worker, ms since its process started (median)
  node                     main loaded  36.3  wasm compiled  51.4  thread created  42.5  thread running 105.6  SQLite JS loaded 108.9  SQLite ready 139.9
  node, no compile cache   main loaded  35.2  wasm compiled  50.6  thread created  41.4  thread running 111.1  SQLite JS loaded 121.7  SQLite ready 152.2
  node, no spare           main loaded  43.4  wasm compiled  56.4  thread created  56.9  thread running 134.4  SQLite JS loaded 137.6  SQLite ready 176.7

Open to first result of a new database, through Database (median ms)
  in-process 0.97; python worker 160.33; node worker 182.11; C worker 2.48

Idle memory with a database open (MB, summed over the worker processes)
  python worker                footprint   19.1  RSS   27.2  (1 process(es))
  node worker                  footprint   84.8  RSS   87.4  (1 process(es))
  C worker                     footprint    1.7  RSS    3.3  (1 process(es))
  node worker, 4 connections   footprint  138.3  RSS  140.9  (1 process(es))

Inside Node, no protocol, in-memory database (SQLite 3.53.4 WASM vs node:sqlite 3.54.1 native)
  one-row read: WASM (oo1 API) 9.48 us, native 1.81 us
  100,000 rows into JS values: WASM raw exports 140.7 ms (oo1 exec 1058.3), native .all() 241.5 ms
  WASM module load and init 102.8 ms; native open 0.47 ms
```
</details>

## DuckDB memory after sleep, in-process against workers

`duckdb_memory`: **ok** in 193.3 s, load average (1 min) 1.05 before, 1.14 after

<details><summary>output</summary>

```
machine: load average [1.05, 0.97, 0.49], 8 CPUs, 0 'yes' processes

in-process n=20 cap=40 load [1.05, 0.97, 0.49] -> [1.05, 0.97, 0.49]
  start: parent 50/34 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 306/277 children 0/0 total 306/277 procs 0 | after sleep parent 128/98 children 0/0 procs 0 | wake median 84.4 ms p95 92.6 ms | wall 0.41 s | fds parent 28 children 0

in-process+gc n=20 cap=40 load [1.05, 0.97, 0.49] -> [1.05, 0.97, 0.49]
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 306/281 children 0/0 total 306/281 procs 0 | after sleep parent 124/95 children 0/0 procs 0 | wake median 77.1 ms p95 88.2 ms | wall 0.39 s | fds parent 28 children 0

in-process+trim n=20 cap=40 load [1.05, 0.97, 0.49] -> [1.05, 0.97, 0.49]
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 306/277 children 0/0 total 306/277 procs 0 | after sleep parent 98/69 children 0/0 procs 0 | wake median 80.6 ms p95 88.7 ms | wall 0.4 s | fds parent 28 children 0 | malloc_trim -> 1

in-process+settings n=20 cap=40 load [1.05, 0.97, 0.49] -> [1.05, 0.97, 0.49]
  start: parent 48/32 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 273/247 children 0/0 total 273/247 procs 0 | after sleep parent 96/67 children 0/0 procs 0 | wake median 82.8 ms p95 97.8 ms | wall 0.52 s | fds parent 28 children 0 | malloc_trim -> 1

worker n=20 cap=40 load [1.05, 0.97, 0.49] -> [1.45, 1.06, 0.52]
  start: parent 30/22 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 33/22 children 1937/950 total 1970/972 procs 20 | after sleep parent 32/24 children 0/0 procs 0 | wake median 344.6 ms p95 411.6 ms | wall 1.38 s | fds parent 68 children 180

worker-pool n=20 cap=40 load [1.45, 1.06, 0.52] -> [1.45, 1.06, 0.52]
  start: parent 30/22 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 32/22 children 882/511 total 914/532 procs 8 | after sleep parent 32/24 children 0/0 procs 0 | wake median 288.7 ms p95 332.5 ms | wall 1.04 s | fds parent 32 children 84

rust-worker n=20 cap=40 load [1.45, 1.06, 0.52] -> [1.41, 1.05, 0.53]
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 33/24 children 1470/576 total 1503/600 procs 20 | after sleep parent 33/24 children 0/0 procs 0 | wake median 92.6 ms p95 109.0 ms | wall 0.58 s | fds parent 68 children 100

cpp-worker n=20 cap=40 load [1.41, 1.05, 0.53] -> [1.41, 1.05, 0.53]
  start: parent 31/23 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 33/24 children 1453/569 total 1486/594 procs 20 | after sleep parent 33/24 children 0/0 procs 0 | wake median 88.7 ms p95 102.4 ms | wall 0.51 s | fds parent 68 children 100

native (load [1.41, 1.05, 0.53] -> [1.27, 1.11, 0.59]):
  floor: {"worker": {"spawn_to_hello_ms": 234.6, "floor_mb": [44.7, 28.5], "open_ms": 47.9, "first_query_ms": 6.5, "spawn_to_first_result_ms": 285.7, "one_db_open_mb": [79.6, 50.6], "after_analytical_mb": [96.2, 67.1]}, "rust-worker": {"spawn_to_hello_ms": 13.7, "floor_mb": [20.4, 17.5], "open_ms": 49.6, "first_query_ms": 7.3, "spawn_to_first_result_ms": 70.6, "one_db_open_mb": [56.5, 53.4], "after_analytical_mb": [73.1, 70.0]}, "cpp-worker": {"spawn_to_hello_ms": 11.4, "floor_mb": [19.7, 16.8], "open_ms": 58.8, "first_query_ms": 7.0, "spawn_to_first_result_ms": 76.1, "one_db_open_mb": [56.6, 53.5], "after_analytical_mb": [72.2, 69.1]}}
  concurrent: {"in-process": {"point_read_quiet_ms": 3.5, "point_read_during_long_query_ms": 3.17, "long_query_still_running": true}, "worker": {"point_read_quiet_ms": 3.59, "point_read_during_long_query_ms": 7811.85, "long_query_still_running": false}, "rust-worker": {"point_read_quiet_ms": 4.29, "point_read_during_long_query_ms": 5.21, "long_query_still_running": true}, "cpp-worker": {"point_read_quiet_ms": 3.24, "point_read_during_long_query_ms": 4.94, "long_query_still_running": true}}
  insert_rows_10k: {"in-process": {"default_ms": 9817.4, "appender_ms": 1233.5, "speedup": 8.0}, "worker": {"default_ms": 11177.1, "appender_ms": 1701.4, "speedup": 6.6}, "rust-worker": {"default_ms": 10930.9, "appender_ms": 1146.7, "speedup": 9.5}, "cpp-worker": {"default_ms": 10099.1, "appender_ms": 1218.0, "speedup": 8.3}}

overhead (load [1.27, 1.11, 0.59] -> [1.14, 1.11, 0.65]):
  in-process: {"one_row_us": 2455.3, "plain_1000_ms": 3.05, "plain_100000_ms": 179.77, "typed_1000_ms": 3.81, "typed_100000_ms": 128.12, "wake_to_first_result_ms": 29.8, "wake_p90_ms": 32.6}
  worker-json: {"one_row_us": 3143.2, "plain_1000_ms": 7.73, "plain_100000_ms": 484.96, "typed_1000_ms": 14.51, "typed_100000_ms": 1023.43, "wake_to_first_result_ms": 253.5, "wake_p90_ms": 279.9, "plain_1000_extra_us_per_row": 4.68, "plain_100000_extra_us_per_row": 3.052, "typed_1000_extra_us_per_row": 10.7, "typed_100000_extra_us_per_row": 8.953}
  worker-msgpack: {"one_row_us": 3234.7, "plain_1000_ms": 4.45, "plain_100000_ms": 293.41, "typed_1000_ms": 13.23, "typed_100000_ms": 1014.07, "wake_to_first_result_ms": 237.6, "wake_p90_ms": 260.5, "plain_1000_extra_us_per_row": 1.4, "plain_100000_extra_us_per_row": 1.136, "typed_1000_extra_us_per_row": 9.42, "typed_100000_extra_us_per_row": 8.86}
  worker+spare: {"one_row_us": 2093.7, "plain_1000_ms": 6.14, "plain_100000_ms": 428.56, "typed_1000_ms": 11.05, "typed_100000_ms": 851.85, "wake_to_first_result_ms": 43.4, "wake_p90_ms": 50.8, "plain_1000_extra_us_per_row": 3.09, "plain_100000_extra_us_per_row": 2.488, "typed_1000_extra_us_per_row": 7.24, "typed_100000_extra_us_per_row": 7.237}
  worker-pool (process already up): {"one_row_us": 2622.9, "plain_1000_ms": 5.6, "plain_100000_ms": 438.02, "typed_1000_ms": 13.47, "typed_100000_ms": 981.34, "wake_to_first_result_ms": 30.9, "wake_p90_ms": 35.2, "plain_1000_extra_us_per_row": 2.55, "plain_100000_extra_us_per_row": 2.583, "typed_1000_extra_us_per_row": 9.66, "typed_100000_extra_us_per_row": 8.532}
  rust-worker-json: {"one_row_us": 2684.9, "plain_1000_ms": 4.31, "plain_100000_ms": 257.41, "typed_1000_ms": 7.41, "typed_100000_ms": 471.5, "wake_to_first_result_ms": 54.2, "wake_p90_ms": 59.2, "plain_1000_extra_us_per_row": 1.26, "plain_100000_extra_us_per_row": 0.776, "typed_1000_extra_us_per_row": 3.6, "typed_100000_extra_us_per_row": 3.434}
  rust-worker-msgpack: {"one_row_us": 2105.8, "plain_1000_ms": 3.78, "plain_100000_ms": 214.89, "typed_1000_ms": 5.94, "typed_100000_ms": 415.48, "wake_to_first_result_ms": 58.4, "wake_p90_ms": 64.5, "plain_1000_extra_us_per_row": 0.73, "plain_100000_extra_us_per_row": 0.351, "typed_1000_extra_us_per_row": 2.13, "typed_100000_extra_us_per_row": 2.874}
  cpp-worker-json: {"one_row_us": 2957.4, "plain_1000_ms": 6.69, "plain_100000_ms": 408.32, "typed_1000_ms": 8.94, "typed_100000_ms": 602.47, "wake_to_first_result_ms": 66.1, "wake_p90_ms": 73.2, "plain_1000_extra_us_per_row": 3.64, "plain_100000_extra_us_per_row": 2.285, "typed_1000_extra_us_per_row": 5.13, "typed_100000_extra_us_per_row": 4.744}
  cpp-worker-msgpack: {"one_row_us": 3390.3, "plain_1000_ms": 4.36, "plain_100000_ms": 200.11, "typed_1000_ms": 7.63, "typed_100000_ms": 383.15, "wake_to_first_result_ms": 68.4, "wake_p90_ms": 73.9, "plain_1000_extra_us_per_row": 1.31, "plain_100000_extra_us_per_row": 0.203, "typed_1000_extra_us_per_row": 3.82, "typed_100000_extra_us_per_row": 2.55}
```
</details>

## PostgreSQL drivers: asyncpg against psycopg 3

`postgres_drivers`: **ok** in 23.8 s, load average (1 min) 1.14 before, 1.23 after

<details><summary>output</summary>

```
connect + close: asyncpg 5.01 ms, psycopg 3.10 ms, psycopg prepared 2.96 ms
  read 1 row: asyncpg 169 us, psycopg 167 us, psycopg prepared 141 us
  read 1 row, max_rows=1000: asyncpg 135 us, psycopg 148 us, psycopg prepared 155 us
  read 1,000 rows, fetch: asyncpg 2.08 ms, psycopg 2.12 ms, psycopg prepared 1.54 ms
  read 1,000 rows, fetch, max_rows=1000: asyncpg 1.90 ms, psycopg 2.01 ms, psycopg prepared 2.01 ms
  read 1,000 rows, stream: asyncpg 2.06 ms, psycopg 2.02 ms, psycopg prepared 2.07 ms
  read 100,000 rows, fetch: asyncpg 218.21 ms, psycopg 235.84 ms, psycopg prepared 212.78 ms
  read 100,000 rows, fetch, max_rows=100000: asyncpg 247.01 ms, psycopg 198.40 ms, psycopg prepared 223.48 ms
  read 100,000 rows, stream: asyncpg 208.86 ms, psycopg 154.46 ms, psycopg prepared 181.31 ms
  insert_rows 100,000, default path: asyncpg 2.23 s, psycopg 3.63 s, psycopg prepared 1.67 s
  insert_rows 100,000, COPY: asyncpg 596.18 ms, psycopg 542.54 ms, psycopg prepared 540.71 ms
  tx statement (1-row update), with savepoints: asyncpg 222 us, psycopg 318 us, psycopg prepared 261 us
  tx statement (1-row update), without savepoints: asyncpg 139 us, psycopg 219 us, psycopg prepared 144 us
  schema(), 50 tables: asyncpg 14.70 ms, psycopg 17.02 ms, psycopg prepared 13.70 ms
  schema_version(): asyncpg 3.23 ms, psycopg 5.12 ms, psycopg prepared 3.60 ms

best of 2 runs (inserts: 1), local socket, load average 1.2, 1.1, 0.7; asyncpg 0.32.0, psycopg 3.3.9 (libpq 180006, binary)

                                                          asyncpg           psycopg  psycopg prepared
-----------------------------------------------------------------------------------------------------
connect + close                                           5.01 ms           3.10 ms           2.96 ms
read 1 row                                                 169 us            167 us            141 us
read 1 row, max_rows=1000                                  135 us            148 us            155 us
read 1,000 rows, fetch                                    2.08 ms           2.12 ms           1.54 ms
read 1,000 rows, fetch, max_rows=1000                     1.90 ms           2.01 ms           2.01 ms
read 1,000 rows, stream                                   2.06 ms           2.02 ms           2.07 ms
read 100,000 rows, fetch                                218.21 ms         235.84 ms         212.78 ms
read 100,000 rows, fetch, max_rows=100000               247.01 ms         198.40 ms         223.48 ms
read 100,000 rows, stream                               208.86 ms         154.46 ms         181.31 ms
insert_rows 100,000, default path                          2.23 s            3.63 s            1.67 s
insert_rows 100,000, COPY                               596.18 ms         542.54 ms         540.71 ms
tx statement (1-row update), with savepoints               222 us            318 us            261 us
tx statement (1-row update), without savepoints            139 us            219 us            144 us
schema(), 50 tables                                      14.70 ms          17.02 ms          13.70 ms
schema_version()                                          3.23 ms           5.12 ms           3.60 ms
```
</details>

## PGlite worker against psycopg and the SQLite workers

`pglite`: **ok** in 68.2 s, load average (1 min) 1.23 before, 1.34 after

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 495.77 | 3548.40 | 457.59 | 175.18 | 2.93 | 7.47 |
| wake_ms | 444.55 | 418.05 | 426.36 | 188.99 | 2.50 | 4.50 |
| read_1_ms | 0.49 | 0.45 | 0.50 | 0.32 | 0.12 | 0.17 |
| read_1k_ms | 11.21 | 11.25 | 10.22 | 3.09 | 1.54 | 1.22 |
| read_100k_ms | 96.77 | 117.68 | 87.93 | 21.23 | 16.18 | 10.82 |
| insert_many_ms | 635.32 | 593.74 | 502.20 | 153.40 | 50.24 | 321.43 |
| insert_rows_ms | 171.82 | 185.46 | 148.78 | 94.61 | 61.21 | 64.65 |
| small_tx_ms | 1.81 | 3.12 | 2.07 | 2.62 | 1.70 | 1.13 |
| sleep_ms | 33.69 | 34.64 | 95.71 | 1.93 | 0.21 | 0.38 |
| idle_floor_mb | 146.10 | 148.73 | 425.61 | 85.23 | 1.79 | n/a |
| awake_footprint_mb | 204.84 | 205.97 | 269.55 | 159.71 | 3.86 | n/a |
| awake_4_footprint_mb | 430.88 | n/a | 942.66 | 289.52 | 4.87 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 41.87 | 41.87 | 41.87 | 0.75 | 0.75 | n/a |
| load | 1.19, 1.12, 0.67 | 1.30, 1.15, 0.69 | 1.33, 1.16, 0.70 | 1.31, 1.16, 0.70 | 1.31, 1.16, 0.70 | 1.31, 1.16, 0.70 |

<details><summary>output</summary>

```
# pglite (load average 1.21, 1.13, 0.67)
  create_ms: 495.77
  idle_floor_mb: 146.1
  empty_disk_mb: 40.08
  wake_ms: 444.55
  wake_breakdown: {'boot_ms': 97.78, 'compile_ms': 29.52, 'wait_compile_ms': 14.19, 'open_ms': 261.17, 'first_query_ms': 21.94, 'spawn_to_hello_ms': 115.93557699995927}
  read_1_ms: 0.49
  insert_many_ms: 635.32
  insert_rows_ms: 171.82
  read_1k_ms: 11.21
  read_100k_ms: 96.77
  small_tx_ms: 1.81
  awake_footprint_mb: 204.84
  disk_mb_200k_rows: 41.87
  sleep_ms: 33.69
  awake_n_footprint_mb: 430.88
  kill: {'bystander_read_ms': 880.1861949999648, 'time_limit_300_raised_after_ms': 981.3364090000505, 'wake_after_kill_ms': 398.56257199994616}
  load: 1.19, 1.12, 0.67
# pglite-initdb (load average 1.19, 1.12, 0.67)
  create_ms: 3548.4
  idle_floor_mb: 148.73
  empty_disk_mb: 40.08
  wake_ms: 418.05
  wake_breakdown: {'boot_ms': 82.88, 'compile_ms': 28.13, 'wait_compile_ms': 11.87, 'open_ms': 255.84, 'first_query_ms': 22.08, 'spawn_to_hello_ms': 100.99847399999362}
  read_1_ms: 0.45
  insert_many_ms: 593.74
  insert_rows_ms: 185.46
  read_1k_ms: 11.25
  read_100k_ms: 117.68
  small_tx_ms: 3.12
  awake_footprint_mb: 205.97
  disk_mb_200k_rows: 41.87
  sleep_ms: 34.64
  load: 1.30, 1.15, 0.69
# pglite-turbofan (load average 1.30, 1.15, 0.69)
  create_ms: 457.59
  idle_floor_mb: 425.61
  empty_disk_mb: 40.08
  wake_ms: 426.36
  wake_breakdown: {'boot_ms': 77.22, 'compile_ms': 29.22, 'wait_compile_ms': 14.05, 'open_ms': 241.2, 'first_query_ms': 26.62, 'spawn_to_hello_ms': 95.33897000005709}
  read_1_ms: 0.5
  insert_many_ms: 502.2
  insert_rows_ms: 148.78
  read_1k_ms: 10.22
  read_100k_ms: 87.93
  small_tx_ms: 2.07
  awake_footprint_mb: 269.55
  disk_mb_200k_rows: 41.87
  sleep_ms: 95.71
  awake_n_footprint_mb: 942.66
  load: 1.33, 1.16, 0.70
# sqlite-nodeworker (load average 1.33, 1.16, 0.70)
  create_ms: 175.18
  idle_floor_mb: 85.23
  empty_disk_mb: 0.0
  wake_ms: 188.99
  wake_breakdown: {}
  read_1_ms: 0.32
  insert_many_ms: 153.4
  insert_rows_ms: 94.61
  read_1k_ms: 3.09
  read_100k_ms: 21.23
  small_tx_ms: 2.62
  awake_footprint_mb: 159.71
  disk_mb_200k_rows: 0.75
  sleep_ms: 1.93
  awake_n_footprint_mb: 289.52
  load: 1.31, 1.16, 0.70
# sqlite-cworker (load average 1.31, 1.16, 0.70)
  create_ms: 2.93
  idle_floor_mb: 1.79
  empty_disk_mb: 0.0
  wake_ms: 2.5
  wake_breakdown: {}
  read_1_ms: 0.12
  insert_many_ms: 50.24
  insert_rows_ms: 61.21
  read_1k_ms: 1.54
  read_100k_ms: 16.18
  small_tx_ms: 1.7
  awake_footprint_mb: 3.86
  disk_mb_200k_rows: 0.75
  sleep_ms: 0.21
  awake_n_footprint_mb: 4.87
  load: 1.31, 1.16, 0.70
# psycopg (load average 1.31, 1.16, 0.70)
  create_ms: 7.47
  idle_floor_mb: None
  wake_ms: 4.5
  wake_breakdown: {}
  read_1_ms: 0.17
  insert_many_ms: 321.43
  insert_rows_ms: 64.65
  read_1k_ms: 1.22
  read_100k_ms: 10.82
  small_tx_ms: 1.13
  awake_footprint_mb: None
  disk_mb_200k_rows: None
  sleep_ms: 0.38
  awake_n_footprint_mb: None
  load: 1.31, 1.16, 0.70

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 495.77 | 3548.40 | 457.59 | 175.18 | 2.93 | 7.47 |
| wake_ms | 444.55 | 418.05 | 426.36 | 188.99 | 2.50 | 4.50 |
| read_1_ms | 0.49 | 0.45 | 0.50 | 0.32 | 0.12 | 0.17 |
| read_1k_ms | 11.21 | 11.25 | 10.22 | 3.09 | 1.54 | 1.22 |
| read_100k_ms | 96.77 | 117.68 | 87.93 | 21.23 | 16.18 | 10.82 |
| insert_many_ms | 635.32 | 593.74 | 502.20 | 153.40 | 50.24 | 321.43 |
| insert_rows_ms | 171.82 | 185.46 | 148.78 | 94.61 | 61.21 | 64.65 |
| small_tx_ms | 1.81 | 3.12 | 2.07 | 2.62 | 1.70 | 1.13 |
| sleep_ms | 33.69 | 34.64 | 95.71 | 1.93 | 0.21 | 0.38 |
| idle_floor_mb | 146.10 | 148.73 | 425.61 | 85.23 | 1.79 | n/a |
| awake_footprint_mb | 204.84 | 205.97 | 269.55 | 159.71 | 3.86 | n/a |
| awake_4_footprint_mb | 430.88 | n/a | 942.66 | 289.52 | 4.87 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 41.87 | 41.87 | 41.87 | 0.75 | 0.75 | n/a |
| load | 1.19, 1.12, 0.67 | 1.30, 1.15, 0.69 | 1.33, 1.16, 0.70 | 1.31, 1.16, 0.70 | 1.31, 1.16, 0.70 | 1.31, 1.16, 0.70 |

pglite wake breakdown (ms): {'boot_ms': 97.78, 'compile_ms': 29.52, 'wait_compile_ms': 14.19, 'open_ms': 261.17, 'first_query_ms': 21.94, 'spawn_to_hello_ms': 115.93557699995927}
pglite time-limit kill: {'bystander_read_ms': 880.1861949999648, 'time_limit_300_raised_after_ms': 981.3364090000505, 'wake_after_kill_ms': 398.56257199994616}

pglite-initdb wake breakdown (ms): {'boot_ms': 82.88, 'compile_ms': 28.13, 'wait_compile_ms': 11.87, 'open_ms': 255.84, 'first_query_ms': 22.08, 'spawn_to_hello_ms': 100.99847399999362}

pglite-turbofan wake breakdown (ms): {'boot_ms': 77.22, 'compile_ms': 29.22, 'wait_compile_ms': 14.05, 'open_ms': 241.2, 'first_query_ms': 26.62, 'spawn_to_hello_ms': 95.33897000005709}

new-database options in one Node process (liftoff_only=True, load 1.31, 1.16, 0.70):
  {'rounds': 3, 'liftoff_only': True, 'compile_ms': 27.947762999999995, 'module_serialize': '2 bytes in 0.1 ms', 'initdb_open_ms': 2701.4972439999997, 'template_copy_ms': 67.5669369999996, 'template_copy_open_ms': 254.51015299999926, 'reopen_existing_ms': 192.4294000000009, 'dump_ms': 181.09611100000075, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 644.1918370000003, 'rss_mb': 491}

new-database options in one Node process (liftoff_only=False, load 1.26, 1.15, 0.71):
  {'rounds': 3, 'liftoff_only': False, 'compile_ms': 28.884394000000015, 'module_serialize': '2 bytes in 0.1 ms', 'initdb_open_ms': 2403.9664759999996, 'template_copy_ms': 62.38651200000004, 'template_copy_open_ms': 255.24651300000005, 'reopen_existing_ms': 180.24614999999903, 'dump_ms': 157.37777099999948, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 564.7056809999995, 'rss_mb': 582}
```
</details>

## ClickHouse (chDB) against DuckDB and SQLite

`clickhouse`: **ok** in 78.2 s, load average (1 min) 1.34 before, 0.94 after

<details><summary>output</summary>

```
## lifecycle and memory   (load average 1.34, 1.18, 0.72)
import chdb 0.25s; first statement with the server down 184 ms; second database while up 9.9 ms; wake after sleep 71 ms
RSS MB: before import 33, after import 264, server up 292, after 1M insert + group by 410, after sleep 412

## analytical reads and point reads, 100,000 rows   (load average 1.34, 1.18, 0.72)
clickhouse fill 0.13s  group by 9 ms  join 13 ms  point read 5.77 ms  disk 1.0 MB
duckdb     fill 0.24s  group by 8 ms  join 7 ms  point read 2.04 ms  disk 2.1 MB
sqlite     fill 0.10s  group by 44 ms  join 47 ms  point read 0.24 ms  disk 2.4 MB

## insert 100,000 rows   (load average 1.31, 1.17, 0.72)
clickhouse native        insert_rows 100,000: 1.89s (52,802 rows/s)
clickhouse default path  insert_rows 100,000: 2.61s (38,268 rows/s)
duckdb                   insert_rows 100,000: 0.49s (202,857 rows/s)
sqlite                   insert_rows 100,000: 0.64s (155,991 rows/s)
clickhouse VALUES batches insert_rows 100,000: 271.37s (368 rows/s; 20,000 rows, scaled)
clickhouse row by row: 5.41 ms a row, 541s for 100,000 (scaled)

## export 100,000 rows   (load average 0.94, 1.10, 0.73)
clickhouse export csv 100,000 engine: 0.03s wall, 0.04s process CPU
clickhouse export csv 100,000 python fallback: 0.30s wall, 0.35s process CPU
clickhouse export json 100,000 engine: 0.06s wall, 0.06s process CPU
clickhouse export json 100,000 python fallback: 0.37s wall, 0.40s process CPU

load average at the end: 0.94, 1.10, 0.73
```
</details>

## db.export(): native serialisation against the Python fallback

`export`: **ok** in 37.2 s, load average (1 min) 0.94 before, 1.05 after

| backend | table | rows | format | mode | transport | cpu_s | wall_s | child_cpu_s | rss_growth_mb | mb_s | rows_s | lag_max_ms | lag_p99_ms | load1 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cworker | wide | 10000 | csv | native | raw | 0.012 | 0.047 | 0.038 | 0.000 | 35.888 | 213449.910 | 5.193 | 5.193 | 0.9 |
| cworker | wide | 10000 | csv | native | frames | 0.010 | 0.044 | 0.040 | 0.000 | 37.885 | 225323.997 | 0.827 | 0.827 | 0.9 |
| cworker | wide | 10000 | csv | native | fifo | 0.009 | 0.039 | 0.037 | 0.000 | 42.808 | 254601.917 | 2.617 | 2.617 | 0.9 |
| cworker | wide | 10000 | csv | fallback |  | 0.083 | 0.097 | 0.020 | 0.000 | 17.289 | 102827.952 | 17.026 | 17.026 | 0.9 |
| cworker | wide | 10000 | csv | datasette |  | 0.071 | 0.087 | 0.021 | 0.000 | 19.352 | 115374.091 | 16.402 | 16.402 | 0.9 |
| cworker | wide | 10000 | csv | handoff |  | 0.008 | 0.040 | 0.035 | 0.000 | 42.386 | 252056.637 | 4.563 | 4.563 | 0.9 |
| cworker | wide | 10000 | json | native | raw | 0.010 | 0.046 | 0.041 | 0.000 | 50.626 | 216614.698 | 0.968 | 0.968 | 0.9 |
| cworker | wide | 10000 | json | native | frames | 0.011 | 0.044 | 0.039 | 0.000 | 53.375 | 228379.240 | 0.926 | 0.926 | 0.9 |
| cworker | wide | 10000 | json | native | fifo | 0.009 | 0.040 | 0.038 | 0.000 | 58.067 | 248452.304 | 1.931 | 1.931 | 0.9 |
| cworker | wide | 10000 | json | fallback |  | 0.062 | 0.075 | 0.020 | 0.000 | 31.280 | 133840.747 | 10.178 | 10.178 | 0.9 |
| cworker | wide | 10000 | json | datasette |  | 0.051 | 0.064 | 0.020 | 1.466 | 38.568 | 156328.597 | 23.070 | 23.070 | 0.9 |
| cworker | wide | 10000 | json | handoff |  | 0.007 | 0.041 | 0.038 | 0.000 | 56.482 | 241638.907 | 4.148 | 4.148 | 0.9 |
| cworker | narrow | 10000 | csv | native | raw | 0.003 | 0.009 | 0.009 | 0.000 | 22.232 | 1052897.796 | 1.773 | 1.773 | 0.9 |
| cworker | narrow | 10000 | csv | fallback |  | 0.026 | 0.033 | 0.012 | 0.000 | 6.449 | 305444.630 | 4.516 | 4.516 | 0.9 |
| cworker | narrow | 10000 | json | native | raw | 0.004 | 0.010 | 0.010 | 0.000 | 40.300 | 1030141.105 | 1.656 | 1.656 | 0.9 |
| cworker | narrow | 10000 | json | fallback |  | 0.024 | 0.031 | 0.010 | 0.000 | 12.702 | 324685.497 | 4.811 | 4.811 | 0.9 |
| duckdb | wide | 10000 | csv | native |  | 0.058 | 0.056 | 0.000 | 19.722 | 30.230 | 179793.359 | 5.498 | 5.498 | 0.9 |
| duckdb | wide | 10000 | csv | fallback |  | 0.108 | 0.107 | 0.000 | 10.080 | 15.662 | 93150.765 | 8.188 | 8.188 | 0.9 |
| duckdb | wide | 10000 | csv | datasette |  | 0.100 | 0.099 | 0.000 | 10.060 | 16.889 | 100690.047 | 8.372 | 8.372 | 1.0 |
| duckdb | wide | 10000 | json | native |  | 0.050 | 0.048 | 0.000 | 21.062 | 48.971 | 209534.753 | 3.793 | 3.793 | 1.0 |
| duckdb | wide | 10000 | json | fallback |  | 0.080 | 0.080 | 0.000 | 10.023 | 29.230 | 125067.025 | 4.543 | 4.543 | 1.0 |
| duckdb | wide | 10000 | json | datasette |  | 0.079 | 0.079 | 0.000 | 20.115 | 31.287 | 126814.162 | 35.394 | 35.394 | 1.0 |
| duckdb | narrow | 10000 | csv | native |  | 0.026 | 0.025 | 0.000 | 5.829 | 8.523 | 403660.539 | 4.706 | 4.706 | 1.0 |
| duckdb | narrow | 10000 | csv | fallback |  | 0.027 | 0.026 | 0.000 | 4.125 | 8.023 | 379989.278 | 2.057 | 2.057 | 1.0 |
| duckdb | narrow | 10000 | json | native |  | 0.024 | 0.023 | 0.000 | 6.746 | 17.286 | 441867.883 | 3.703 | 3.703 | 1.0 |
| duckdb | narrow | 10000 | json | fallback |  | 0.026 | 0.026 | 0.000 | 4.518 | 15.309 | 391336.100 | 1.397 | 1.397 | 1.0 |
| postgres | wide | 10000 | csv | native |  | 0.035 | 0.040 | 0.000 | 1.311 | 41.996 | 249775.240 | 5.707 | 5.707 | 1.0 |
| postgres | wide | 10000 | csv | fallback |  | 0.096 | 0.101 | 0.000 | 2.925 | 16.719 | 99435.422 | 10.974 | 10.974 | 1.0 |
| postgres | wide | 10000 | csv | datasette |  | 0.093 | 0.098 | 0.000 | 3.084 | 17.163 | 102323.154 | 9.774 | 9.774 | 1.0 |
| postgres | wide | 10000 | json | native |  | 0.033 | 0.039 | 0.000 | 0.393 | 60.004 | 256742.290 | 0.996 | 0.996 | 1.0 |
| postgres | wide | 10000 | json | fallback |  | 0.063 | 0.069 | 0.000 | 2.884 | 33.779 | 144529.936 | 7.922 | 7.922 | 1.0 |
| postgres | wide | 10000 | json | datasette |  | 0.074 | 0.077 | 0.000 | 12.481 | 31.990 | 129663.100 | 37.710 | 37.710 | 1.0 |
| postgres | narrow | 10000 | csv | native |  | 0.021 | 0.024 | 0.000 | 0.262 | 8.769 | 415283.254 | 2.543 | 2.543 | 1.0 |
| postgres | narrow | 10000 | csv | fallback |  | 0.025 | 0.028 | 0.000 | 1.049 | 7.659 | 362724.237 | 2.951 | 2.951 | 1.0 |
| postgres | narrow | 10000 | json | native |  | 0.028 | 0.030 | 0.000 | 0.393 | 13.030 | 333078.473 | 2.168 | 2.168 | 1.0 |
| postgres | narrow | 10000 | json | fallback |  | 0.032 | 0.034 | 0.000 | 1.049 | 11.343 | 289944.586 | 3.098 | 3.098 | 1.0 |
| cworker | wide | 100000 | csv | native | raw | 0.058 | 0.365 | 0.322 | 0.000 | 46.909 | 274072.271 | 1.542 | 1.281 | 1.0 |
| cworker | wide | 100000 | csv | native | frames | 0.055 | 0.370 | 0.338 | 0.000 | 46.315 | 270598.500 | 1.980 | 1.056 | 1.0 |
| cworker | wide | 100000 | csv | native | fifo | 0.057 | 0.325 | 0.307 | 0.000 | 52.615 | 307409.593 | 1.205 | 1.051 | 1.0 |
| cworker | wide | 100000 | csv | fallback |  | 0.673 | 0.801 | 0.161 | 0.000 | 21.381 | 124918.312 | 16.839 | 13.466 | 1.0 |
| cworker | wide | 100000 | csv | datasette |  | 0.652 | 0.788 | 0.167 | 0.000 | 21.676 | 126938.272 | 15.110 | 14.843 | 1.0 |
| cworker | wide | 100000 | csv | handoff |  | 0.021 | 0.323 | 0.319 | 0.000 | 52.943 | 309283.624 | 2.953 | 1.021 | 1.0 |
| cworker | wide | 100000 | json | native | raw | 0.062 | 0.376 | 0.334 | 0.000 | 62.907 | 265732.398 | 1.061 | 1.003 | 1.0 |
| cworker | wide | 100000 | json | native | frames | 0.064 | 0.397 | 0.355 | 0.000 | 59.640 | 251933.469 | 1.975 | 1.024 | 1.0 |
| cworker | wide | 100000 | json | native | fifo | 0.070 | 0.341 | 0.317 | 0.000 | 69.517 | 293657.258 | 0.996 | 0.900 | 1.0 |
| cworker | wide | 100000 | json | fallback |  | 0.380 | 0.505 | 0.157 | 0.000 | 46.903 | 198127.638 | 8.940 | 7.521 | 1.0 |
| cworker | wide | 100000 | json | datasette |  | 0.569 | 0.701 | 0.175 | 107.205 | 35.599 | 142552.280 | 299.220 | 6.999 | 1.0 |
| cworker | wide | 100000 | json | handoff |  | 0.027 | 0.373 | 0.366 | 0.000 | 63.539 | 268368.502 | 4.003 | 0.843 | 1.0 |
| cworker | narrow | 100000 | csv | native | raw | 0.015 | 0.081 | 0.074 | 0.000 | 29.624 | 1228560.725 | 1.989 | 1.989 | 1.0 |
| cworker | narrow | 100000 | csv | fallback |  | 0.266 | 0.325 | 0.084 | 0.000 | 7.409 | 307262.054 | 5.665 | 5.602 | 1.0 |
| cworker | narrow | 100000 | json | native | raw | 0.017 | 0.088 | 0.078 | 0.000 | 47.867 | 1136628.897 | 1.554 | 1.554 | 1.0 |
| cworker | narrow | 100000 | json | fallback |  | 0.206 | 0.268 | 0.082 | 0.000 | 15.691 | 372586.829 | 4.776 | 4.586 | 1.0 |
| duckdb | wide | 100000 | csv | native |  | 0.242 | 0.193 | 0.000 | 54.456 | 88.888 | 519338.720 | 3.216 | 1.303 | 1.0 |
| duckdb | wide | 100000 | csv | fallback |  | 1.004 | 0.982 | 0.000 | 51.626 | 17.429 | 101831.326 | 10.764 | 10.048 | 1.0 |
| duckdb | wide | 100000 | csv | datasette |  | 0.913 | 0.893 | 0.000 | 50.704 | 19.131 | 112036.717 | 9.415 | 8.397 | 1.0 |
| duckdb | wide | 100000 | json | native |  | 0.265 | 0.205 | 0.000 | 60.735 | 115.499 | 487894.029 | 4.629 | 1.907 | 1.0 |
| duckdb | wide | 100000 | json | fallback |  | 0.681 | 0.663 | 0.000 | 43.602 | 35.679 | 150717.428 | 8.994 | 6.038 | 1.0 |
| duckdb | wide | 100000 | json | datasette |  | 0.742 | 0.725 | 0.000 | 165.081 | 34.438 | 137901.895 | 361.055 | 3.284 | 1.0 |
| duckdb | narrow | 100000 | csv | native |  | 0.102 | 0.098 | 0.000 | 14.217 | 24.683 | 1023676.072 | 3.735 | 3.735 | 1.0 |
| duckdb | narrow | 100000 | csv | fallback |  | 0.273 | 0.270 | 0.000 | 12.251 | 8.940 | 370752.321 | 4.533 | 2.686 | 1.0 |
| duckdb | narrow | 100000 | json | native |  | 0.104 | 0.101 | 0.000 | 16.314 | 41.589 | 987552.737 | 3.140 | 3.140 | 1.0 |
| duckdb | narrow | 100000 | json | fallback |  | 0.252 | 0.249 | 0.000 | 11.989 | 16.880 | 400820.161 | 3.580 | 3.452 | 1.0 |
| postgres | wide | 100000 | csv | native |  | 0.290 | 0.335 | 0.000 | 0.393 | 51.139 | 298786.217 | 3.178 | 0.979 | 1.0 |
| postgres | wide | 100000 | csv | fallback |  | 0.969 | 1.014 | 0.000 | 3.318 | 16.877 | 98607.107 | 11.886 | 11.691 | 1.0 |
| postgres | wide | 100000 | csv | datasette |  | 0.816 | 0.855 | 0.000 | 3.428 | 19.966 | 116923.715 | 10.382 | 9.755 | 1.0 |
| postgres | wide | 100000 | json | native |  | 0.337 | 0.439 | 0.000 | 0.393 | 53.871 | 227563.969 | 1.046 | 0.515 | 1.0 |
| postgres | wide | 100000 | json | fallback |  | 0.643 | 0.683 | 0.000 | 3.146 | 34.684 | 146515.540 | 8.378 | 8.076 | 1.0 |
| postgres | wide | 100000 | json | datasette |  | 0.726 | 0.762 | 0.000 | 117.965 | 32.777 | 131248.602 | 382.805 | 9.103 | 1.0 |
| postgres | narrow | 100000 | csv | native |  | 0.217 | 0.233 | 0.000 | 0.524 | 10.350 | 429255.065 | 2.565 | 2.565 | 1.0 |
| postgres | narrow | 100000 | csv | fallback |  | 0.224 | 0.244 | 0.000 | 1.180 | 9.899 | 410546.472 | 2.912 | 2.335 | 1.1 |
| postgres | narrow | 100000 | json | native |  | 0.260 | 0.287 | 0.000 | 0.393 | 14.698 | 349004.814 | 2.623 | 2.136 | 1.1 |
| postgres | narrow | 100000 | json | fallback |  | 0.258 | 0.276 | 0.000 | 1.311 | 15.236 | 361788.519 | 2.749 | 2.587 | 1.1 |

<details><summary>output</summary>

```
# load average at start: (0.9462890625, 1.099609375, 0.73583984375)
backend	table	rows	format	mode	transport	cpu_s	wall_s	child_cpu_s	rss_growth_mb	mb_s	rows_s	lag_max_ms	lag_p99_ms	load1
cworker	wide	10000	csv	native	raw	0.012	0.047	0.038	0.000	35.888	213449.910	5.193	5.193	0.9
cworker	wide	10000	csv	native	frames	0.010	0.044	0.040	0.000	37.885	225323.997	0.827	0.827	0.9
cworker	wide	10000	csv	native	fifo	0.009	0.039	0.037	0.000	42.808	254601.917	2.617	2.617	0.9
cworker	wide	10000	csv	fallback		0.083	0.097	0.020	0.000	17.289	102827.952	17.026	17.026	0.9
cworker	wide	10000	csv	datasette		0.071	0.087	0.021	0.000	19.352	115374.091	16.402	16.402	0.9
cworker	wide	10000	csv	handoff		0.008	0.040	0.035	0.000	42.386	252056.637	4.563	4.563	0.9
cworker	wide	10000	json	native	raw	0.010	0.046	0.041	0.000	50.626	216614.698	0.968	0.968	0.9
cworker	wide	10000	json	native	frames	0.011	0.044	0.039	0.000	53.375	228379.240	0.926	0.926	0.9
cworker	wide	10000	json	native	fifo	0.009	0.040	0.038	0.000	58.067	248452.304	1.931	1.931	0.9
cworker	wide	10000	json	fallback		0.062	0.075	0.020	0.000	31.280	133840.747	10.178	10.178	0.9
cworker	wide	10000	json	datasette		0.051	0.064	0.020	1.466	38.568	156328.597	23.070	23.070	0.9
cworker	wide	10000	json	handoff		0.007	0.041	0.038	0.000	56.482	241638.907	4.148	4.148	0.9
cworker	narrow	10000	csv	native	raw	0.003	0.009	0.009	0.000	22.232	1052897.796	1.773	1.773	0.9
cworker	narrow	10000	csv	fallback		0.026	0.033	0.012	0.000	6.449	305444.630	4.516	4.516	0.9
cworker	narrow	10000	json	native	raw	0.004	0.010	0.010	0.000	40.300	1030141.105	1.656	1.656	0.9
cworker	narrow	10000	json	fallback		0.024	0.031	0.010	0.000	12.702	324685.497	4.811	4.811	0.9
duckdb	wide	10000	csv	native		0.058	0.056	0.000	19.722	30.230	179793.359	5.498	5.498	0.9
duckdb	wide	10000	csv	fallback		0.108	0.107	0.000	10.080	15.662	93150.765	8.188	8.188	0.9
duckdb	wide	10000	csv	datasette		0.100	0.099	0.000	10.060	16.889	100690.047	8.372	8.372	1.0
duckdb	wide	10000	json	native		0.050	0.048	0.000	21.062	48.971	209534.753	3.793	3.793	1.0
duckdb	wide	10000	json	fallback		0.080	0.080	0.000	10.023	29.230	125067.025	4.543	4.543	1.0
duckdb	wide	10000	json	datasette		0.079	0.079	0.000	20.115	31.287	126814.162	35.394	35.394	1.0
duckdb	narrow	10000	csv	native		0.026	0.025	0.000	5.829	8.523	403660.539	4.706	4.706	1.0
duckdb	narrow	10000	csv	fallback		0.027	0.026	0.000	4.125	8.023	379989.278	2.057	2.057	1.0
duckdb	narrow	10000	json	native		0.024	0.023	0.000	6.746	17.286	441867.883	3.703	3.703	1.0
duckdb	narrow	10000	json	fallback		0.026	0.026	0.000	4.518	15.309	391336.100	1.397	1.397	1.0
postgres	wide	10000	csv	native		0.035	0.040	0.000	1.311	41.996	249775.240	5.707	5.707	1.0
postgres	wide	10000	csv	fallback		0.096	0.101	0.000	2.925	16.719	99435.422	10.974	10.974	1.0
postgres	wide	10000	csv	datasette		0.093	0.098	0.000	3.084	17.163	102323.154	9.774	9.774	1.0
postgres	wide	10000	json	native		0.033	0.039	0.000	0.393	60.004	256742.290	0.996	0.996	1.0
postgres	wide	10000	json	fallback		0.063	0.069	0.000	2.884	33.779	144529.936	7.922	7.922	1.0
postgres	wide	10000	json	datasette		0.074	0.077	0.000	12.481	31.990	129663.100	37.710	37.710	1.0
postgres	narrow	10000	csv	native		0.021	0.024	0.000	0.262	8.769	415283.254	2.543	2.543	1.0
postgres	narrow	10000	csv	fallback		0.025	0.028	0.000	1.049	7.659	362724.237	2.951	2.951	1.0
postgres	narrow	10000	json	native		0.028	0.030	0.000	0.393	13.030	333078.473	2.168	2.168	1.0
postgres	narrow	10000	json	fallback		0.032	0.034	0.000	1.049	11.343	289944.586	3.098	3.098	1.0
cworker	wide	100000	csv	native	raw	0.058	0.365	0.322	0.000	46.909	274072.271	1.542	1.281	1.0
cworker	wide	100000	csv	native	frames	0.055	0.370	0.338	0.000	46.315	270598.500	1.980	1.056	1.0
cworker	wide	100000	csv	native	fifo	0.057	0.325	0.307	0.000	52.615	307409.593	1.205	1.051	1.0
cworker	wide	100000	csv	fallback		0.673	0.801	0.161	0.000	21.381	124918.312	16.839	13.466	1.0
cworker	wide	100000	csv	datasette		0.652	0.788	0.167	0.000	21.676	126938.272	15.110	14.843	1.0
cworker	wide	100000	csv	handoff		0.021	0.323	0.319	0.000	52.943	309283.624	2.953	1.021	1.0
cworker	wide	100000	json	native	raw	0.062	0.376	0.334	0.000	62.907	265732.398	1.061	1.003	1.0
cworker	wide	100000	json	native	frames	0.064	0.397	0.355	0.000	59.640	251933.469	1.975	1.024	1.0
cworker	wide	100000	json	native	fifo	0.070	0.341	0.317	0.000	69.517	293657.258	0.996	0.900	1.0
cworker	wide	100000	json	fallback		0.380	0.505	0.157	0.000	46.903	198127.638	8.940	7.521	1.0
cworker	wide	100000	json	datasette		0.569	0.701	0.175	107.205	35.599	142552.280	299.220	6.999	1.0
cworker	wide	100000	json	handoff		0.027	0.373	0.366	0.000	63.539	268368.502	4.003	0.843	1.0
cworker	narrow	100000	csv	native	raw	0.015	0.081	0.074	0.000	29.624	1228560.725	1.989	1.989	1.0
cworker	narrow	100000	csv	fallback		0.266	0.325	0.084	0.000	7.409	307262.054	5.665	5.602	1.0
cworker	narrow	100000	json	native	raw	0.017	0.088	0.078	0.000	47.867	1136628.897	1.554	1.554	1.0
cworker	narrow	100000	json	fallback		0.206	0.268	0.082	0.000	15.691	372586.829	4.776	4.586	1.0
duckdb	wide	100000	csv	native		0.242	0.193	0.000	54.456	88.888	519338.720	3.216	1.303	1.0
duckdb	wide	100000	csv	fallback		1.004	0.982	0.000	51.626	17.429	101831.326	10.764	10.048	1.0
duckdb	wide	100000	csv	datasette		0.913	0.893	0.000	50.704	19.131	112036.717	9.415	8.397	1.0
duckdb	wide	100000	json	native		0.265	0.205	0.000	60.735	115.499	487894.029	4.629	1.907	1.0
duckdb	wide	100000	json	fallback		0.681	0.663	0.000	43.602	35.679	150717.428	8.994	6.038	1.0
duckdb	wide	100000	json	datasette		0.742	0.725	0.000	165.081	34.438	137901.895	361.055	3.284	1.0
duckdb	narrow	100000	csv	native		0.102	0.098	0.000	14.217	24.683	1023676.072	3.735	3.735	1.0
duckdb	narrow	100000	csv	fallback		0.273	0.270	0.000	12.251	8.940	370752.321	4.533	2.686	1.0
duckdb	narrow	100000	json	native		0.104	0.101	0.000	16.314	41.589	987552.737	3.140	3.140	1.0
duckdb	narrow	100000	json	fallback		0.252	0.249	0.000	11.989	16.880	400820.161	3.580	3.452	1.0
postgres	wide	100000	csv	native		0.290	0.335	0.000	0.393	51.139	298786.217	3.178	0.979	1.0
postgres	wide	100000	csv	fallback		0.969	1.014	0.000	3.318	16.877	98607.107	11.886	11.691	1.0
postgres	wide	100000	csv	datasette		0.816	0.855	0.000	3.428	19.966	116923.715	10.382	9.755	1.0
postgres	wide	100000	json	native		0.337	0.439	0.000	0.393	53.871	227563.969	1.046	0.515	1.0
postgres	wide	100000	json	fallback		0.643	0.683	0.000	3.146	34.684	146515.540	8.378	8.076	1.0
postgres	wide	100000	json	datasette		0.726	0.762	0.000	117.965	32.777	131248.602	382.805	9.103	1.0
postgres	narrow	100000	csv	native		0.217	0.233	0.000	0.524	10.350	429255.065	2.565	2.565	1.0
postgres	narrow	100000	csv	fallback		0.224	0.244	0.000	1.180	9.899	410546.472	2.912	2.335	1.1
postgres	narrow	100000	json	native		0.260	0.287	0.000	0.393	14.698	349004.814	2.623	2.136	1.1
postgres	narrow	100000	json	fallback		0.258	0.276	0.000	1.311	15.236	361788.519	2.749	2.587	1.1
```
</details>

## Scale backends: wake cost and query cost, separately

`scale_wake_query`: **ok** in 39.1 s, load average (1 min) 1.05 before, 1.11 after

| backend | wake_ms | warm_median_ms | warm_p90_ms | cold_median_ms | cold_p90_ms | sleep_median_ms | load1_before | load1_after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sqlite | 0.435 | 0.200 | 0.270 | 0.635 | 0.802 | 0.237 | 1.05 | 1.05 |
| sqlite-wasm | 1.466 | 0.305 | 0.401 | 1.771 | 2.345 | 0.320 | 1.05 | 1.05 |
| duckdb | 23.027 | 1.237 | 1.889 | 24.265 | 28.321 | 5.272 | 1.05 | 1.05 |
| sqlite-worker | 130.602 | 0.488 | 0.674 | 131.091 | 147.829 | 0.193 | 1.05 | 1.04 |
| sqlite-cworker | 2.150 | 0.173 | 0.241 | 2.323 | 2.708 | 0.153 | 1.04 | 1.04 |
| sqlite-nodeworker | 184.958 | 1.116 | 1.727 | 186.074 | 206.015 | 1.803 | 1.04 | 1.03 |
| sqlite-cpool | 2.038 | 0.164 | 0.259 | 2.203 | 2.769 | 0.258 | 1.03 | 1.11 |
| sqlite-rspool | 2.550 | 0.178 | 0.264 | 2.728 | 3.496 | 0.300 | 1.11 | 1.11 |

## Scale tests (tests/scale, through their harness)

`scale_tests`: **ok** in 487.1 s, load average (1 min) 1.11 before, 39.6 after

```
[scale:sqlite] 300 databases, created in 0.0s
  register: 11.7 us/db; best-of-3 2.73 us/db at 100, 2.67 us/db at 300
  asleep, never used: 464 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +13.8 B/db growth (tracemalloc, 300 dbs)
  load: 20000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 10.9s (1828 req/s)
  fds: baseline 9, peak 76, limit 97 (cap 64 x 1 per place + allowance 24), after sleep 9; 1867 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=6491 sleeps=6442 cap_evictions=10606 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-wasm] 300 databases, created in 0.0s
  register: 110.6 us/db; best-of-3 21.36 us/db at 100, 19.73 us/db at 300
  asleep, never used: 898 B/db (tracemalloc), 14 B/db (RSS delta)
  asleep after a wake/sleep cycle: +176.4 B/db growth (tracemalloc, 300 dbs)
  load: 20000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 41.1s (486 req/s)
  fds: baseline 11, peak 66, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 3258 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=6612 sleeps=6566 cap_evictions=10216 idle_closes=53; after idle timeout: awake=0 open=0

[scale:duckdb] 300 databases, created in 0.1s
  register: 62.0 us/db; best-of-3 10.58 us/db at 100, 12.54 us/db at 300
  asleep, never used: 1278 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +82.7 B/db growth (tracemalloc, 300 dbs)
  load: 3000 requests (10% writes) over 272 databases, Zipf s=1.1, 96 workers, in 11.4s (264 req/s)
  fds: baseline 11, peak 72, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 2038 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=1083 sleeps=1060 cap_evictions=1305 idle_closes=416; after idle timeout: awake=0 open=0

[scale:sqlite-worker] 300 databases, created in 0.0s
  register: 130.7 us/db; best-of-3 17.39 us/db at 100, 17.78 us/db at 300
  asleep, never used: 778 B/db (tracemalloc), 82 B/db (RSS delta)
  asleep after a wake/sleep cycle: +129.1 B/db growth (tracemalloc, 300 dbs)
  load: 2000 requests (10% writes) over 242 databases, Zipf s=1.1, 96 workers, in 30.3s (66 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1637 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=723 sleeps=694 cap_evictions=1202 idle_closes=63; after idle timeout: awake=0 open=0

[scale:sqlite-cworker] 300 databases, created in 0.0s
  register: 170.2 us/db; best-of-3 32.84 us/db at 100, 33.43 us/db at 300
  asleep, never used: 789 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +152.0 B/db growth (tracemalloc, 300 dbs)
  load: 10000 requests (10% writes) over 298 databases, Zipf s=1.1, 96 workers, in 10.4s (960 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 945 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=3408 sleeps=3367 cap_evictions=5105 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-nodeworker] 300 databases, created in 0.0s
  register: 223.4 us/db; best-of-3 47.86 us/db at 100, 49.10 us/db at 300
  asleep, never used: 1484 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +1113.4 B/db growth (tracemalloc, 300 dbs)
  load: 2000 requests (10% writes) over 242 databases, Zipf s=1.1, 96 workers, in 37.9s (53 req/s)
  fds: baseline 11, peak 167, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 2833 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=737 sleeps=720 cap_evictions=1378 idle_closes=179; after idle timeout: awake=0 open=0

[scale:sqlite-cpool] 300 databases, created in 0.0s
  register: 237.4 us/db; best-of-3 47.44 us/db at 100, 43.47 us/db at 300
  asleep, never used: 987 B/db (tracemalloc), 82 B/db (RSS delta)
  asleep after a wake/sleep cycle: +61.7 B/db growth (tracemalloc, 300 dbs)
  load: 50000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 8.5s (5906 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1863 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=16233 sleeps=16189 cap_evictions=25470 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-rspool] 300 databases, created in 0.0s
  register: 213.6 us/db; best-of-3 38.81 us/db at 100, 41.00 us/db at 300
  asleep, never used: 985 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +92.0 B/db growth (tracemalloc, 300 dbs)
  load: 50000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 8.6s (5808 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 1897 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=16240 sleeps=16192 cap_evictions=25597 idle_closes=0; after idle timeout: awake=0 open=0

[threads:sqlite] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-wasm] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:duckdb] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-worker] standalone, 100 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-cworker] standalone, 1000 open: 10 before, 9-10 during, 9 after, new: []

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

pytest: `24 passed, 1 deselected in 486.16s (0:08:06)`
