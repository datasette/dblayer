# dblayer benchmarks: 2026-11-24T1753Z-6e409fa9d0-quick

**TRIAL of the Fly.io mechanism (--quick): not final numbers**

- Mode: quick; started 2026-11-24T17:53:05+00:00
- Commit: `6e409fa9d0830a813da1f457f7e82cc5e462e6fa` (6e409fa 2026-11-24T09:41:04-08:00 fly/: scripts to create the app and volume, build the image, launch, fetch results and tear down (idempotent))
- Machine: AMD EPYC, 8 CPUs, 15.6 GB, kernel 6.12.118-fly, x86_64, Fly performance-8x in sjc
- Data filesystem: `/dev/vdc ext4 rw,relatime`; temporary files: `/dev/vdc ext4`
- Versions: python 3.14.8; sqlite (python sqlite3) 3.53.1; uv uv 0.13.0 (x86_64-unknown-linux-gnu); node v24.22.3; rustc rustc 1.100.0 (0ef1bc386 2026-11-09); cargo cargo 1.100.0 (3acc79c0c 2026-11-05); cc cc (Debian 14.2.0-19) 14.2.0; postgres (psql client) psql (PostgreSQL) 18.7 (Debian 18.7-1.pgdg13+5); duckdb 2.0.1; asyncpg 0.32.0; psycopg 3.3.9; psycopg-binary 3.3.9; apsw 3.54.1.0; wasmtime 51.0.0; chdb 4.5.1; msgpack 1.2.4; pytest 9.2.0; starlette 1.10.0; uvicorn 0.54.0; postgres server 18.7 (Debian 18.7-1.pgdg13+5); libpq 180006; sqlite (C worker) dblayer-cworker protocol 1, SQLite 3.53.1; sqlite-rsworker dblayer-sqlite-rsworker protocol 1, SQLite 3.53.1 (-O2 -g -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_DQS=3); duckdb-rsworker dblayer-duckdb-rsworker protocol 1, DuckDB v2.0.1; duckdb-cppworker dblayer-duckdb-worker protocol 1, DuckDB v2.0.1

Every topic ran alone, one after another, in this one session. Compare figures within this file only.

| topic | status | seconds | load before | load after | reason |
| --- | --- | --- | --- | --- | --- |
| worker_overhead | ok | 83.9 | 1.41 | 1.42 |  |
| cpool | ok | 19.5 | 1.42 | 1.33 |  |
| rspool | ok | 36.7 | 1.33 | 1.29 |  |
| wasm | ok | 130.5 | 1.29 | 1.03 |  |
| nodeworker | ok | 19.0 | 1.03 | 1.09 |  |
| duckdb_memory | failed | 98.6 | 1.09 | 1.05 | AssertionError |
| postgres_drivers | ok | 24.8 | 1.05 | 0.98 |  |
| pglite | ok | 69.8 | 0.98 | 1.23 |  |
| clickhouse | ok | 74.4 | 1.23 | 1.06 |  |
| export | ok | 89.0 | 1.06 | 1.01 |  |
| scale_wake_query | ok | 94.4 | 1.01 | 1.15 |  |
| scale_tests | ok | 1047.3 | 1.15 | 58.65 |  |

## Worker process boundary against in-process SQLite

`worker_overhead`: **ok** in 83.9 s, load average (1 min) 1.41 before, 1.42 after

| | in-process_ms | dedicated_ms | shared_first_ms | shared_warm_ms | c_dedicated_ms | c_shared_first_ms | c_shared_warm_ms | spawn_true_ms | python_bare_ms | python_site_ms | c_version_ms | python_idle_rss_kb | c_idle_rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| round_trip |  |  |  |  |  |  |  |  |  |  |  |  |  |
| rows_1000 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| rows_100000 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| insert_with_alter |  |  |  |  |  |  |  |  |  |  |  |  |  |
| apsw_extras |  |  |  |  |  |  |  |  |  |  |  |  |  |
| start | 0.7473 | 660.8 | 670.7 | 0.9603 | 2.042 | 2.16 | 0.3536 |  |  |  |  |  |  |
| startup_breakdown |  |  |  |  |  |  |  | 0.6815 | 19.88 | 138.5 | 0.827 | 36176 | 3364 |
| cpool |  |  |  |  |  |  |  |  |  |  |  |  |  |

<details><summary>output</summary>

```
SQLite worker overhead (python 3.14.8)

One-row read by primary key, round trip (median / p90 us); one-row update (median us)
  in-process           read    235.1 /    292.2   write   1352.3
  apsw                 read    229.0 /    297.1   write   1201.4
  apsw-inline          read     53.2 /     87.8   write   1121.8
  worker-json          read    472.7 /    622.5   write   1842.9
  worker-msgpack       read    385.1 /    463.6   write   1222.3
  cworker-json         read    149.4 /    193.7   write   1088.4
  cworker-msgpack      read     96.7 /    136.4   write    966.0
  cpool-json           read    163.2 /    235.7   write   1377.9
  cpool-msgpack        read    119.8 /    156.6   write   1027.8
  nodeworker-json      read    583.3 /    789.4   write   2037.9
  nodeworker-msgpack   read    491.4 /    644.4   write   1597.0

Read of 1,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute      1.73 ms   1.73 us/row   stream      2.51 ms   2.51 us/row
  apsw                 execute      2.06 ms   2.06 us/row   stream      2.65 ms   2.65 us/row
  apsw-inline          execute      1.38 ms   1.38 us/row   stream      1.64 ms   1.64 us/row
  worker-json          execute      6.73 ms   6.73 us/row   stream      5.90 ms   5.90 us/row
  worker-msgpack       execute      3.39 ms   3.39 us/row   stream      2.93 ms   2.93 us/row
  cworker-json         execute      3.33 ms   3.33 us/row   stream      3.09 ms   3.09 us/row
  cworker-msgpack      execute      1.28 ms   1.28 us/row   stream      1.42 ms   1.42 us/row
  cpool-json           execute      3.37 ms   3.37 us/row   stream      3.63 ms   3.63 us/row
  cpool-msgpack        execute      1.37 ms   1.37 us/row   stream      1.66 ms   1.66 us/row
  nodeworker-json      execute      7.02 ms   7.02 us/row   stream      6.09 ms   6.09 us/row
  nodeworker-msgpack   execute      3.62 ms   3.62 us/row   stream      2.90 ms   2.90 us/row

Read of 100,000 rows (4 columns: int, text, real, 16-byte blob): total ms, us per row
  in-process           execute    198.37 ms   1.98 us/row   stream    178.04 ms   1.78 us/row
  apsw                 execute    158.73 ms   1.59 us/row   stream    140.57 ms   1.41 us/row
  apsw-inline          execute    134.33 ms   1.34 us/row   stream    159.60 ms   1.60 us/row
  worker-json          execute    641.18 ms   6.41 us/row   stream    487.69 ms   4.88 us/row
  worker-msgpack       execute    367.23 ms   3.67 us/row   stream    209.81 ms   2.10 us/row
  cworker-json         execute    365.78 ms   3.66 us/row   stream    240.74 ms   2.41 us/row
  cworker-msgpack      execute    229.81 ms   2.30 us/row   stream    165.30 ms   1.65 us/row
  cpool-json           execute    375.11 ms   3.75 us/row   stream    297.20 ms   2.97 us/row
  cpool-msgpack        execute    228.28 ms   2.28 us/row   stream     94.74 ms   0.95 us/row
  nodeworker-json      execute    531.36 ms   5.31 us/row   stream    365.42 ms   3.65 us/row
  nodeworker-msgpack   execute    356.98 ms   3.57 us/row   stream    187.48 ms   1.87 us/row

Insert with alter (2 rows, add one column): median us
  in-process           interactive transaction   2812.0   one named operation   2817.8
  apsw                 interactive transaction   4145.0   one named operation   4523.0
  apsw-inline          interactive transaction   4104.6   one named operation   3794.5
  worker-json          interactive transaction   5026.5   one named operation   4074.3
  worker-msgpack       interactive transaction   4307.4   one named operation   3882.8
  cworker-json         interactive transaction   2425.8   one named operation   2482.9
  cworker-msgpack      interactive transaction   3410.1   one named operation   3905.1
  cpool-json           interactive transaction   2421.5   one named operation   2522.6
  cpool-msgpack        interactive transaction   2499.3   one named operation   2853.7
  nodeworker-json      interactive transaction   6969.6   one named operation   6430.7
  nodeworker-msgpack   interactive transaction   8063.8   one named operation   6989.5

SQLite versions: in-process sqlite3 3.53.1; apsw 3.54.1.0 bundles SQLite 3.54.1
Connection open (median us), execute_many of 100,000 rows in a transaction (ms), memory per open connection (KB of process RSS; SQLite's own bytes)
  in-process           open    231.9   execute_many    367.7 ms   rss  169.1 KB/conn
  apsw                 open    195.3   execute_many    382.0 ms   rss  186.3 KB/conn (db_status 21.4 KB)
  cworker-json         open   2116.8   execute_many    395.7 ms
  cworker-msgpack      open   1525.4   execute_many    342.1 ms

Multi-tenant C pool (load average 1.2 0.8 0.4 at start, 1.4 after; provisional)
  new database on a warm pool: first result 0.98 ms; bare open round trip 0.193 ms
  memory per open database inside the process: 170.4 KB footprint, 170.4 KB RSS (100 databases; process base 1.7 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.3 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    4585 (0.99; 0.00; 0p; 9fd)   4:    3376 (1.34; 0.00; 0p; 12fd)  16:    2990 (1.49; 0.00; 0p; 24fd)  64:    3143 (1.49; 0.00; 0p; 72fd)
    cworker     1:    7543 (0.76; 0.00; 1p; 11fd)   4:   10549 (0.97; 0.00; 4p; 20fd)  16:   14021 (0.99; 0.00; 16p; 56fd)  64:   12498 (0.99; 0.00; 64p; 200fd)
    cpool       1:    6967 (0.74; 0.00; 1p; 11fd)   4:   11215 (0.98; 0.00; 2p; 14fd)  16:   13923 (0.99; 0.00; 2p; 14fd)  64:   13012 (0.99; 0.00; 2p; 14fd)

Open to first result of a new Database (ms)
  in-process 0.75; worker dedicated 660.8 (a process per connection); shared: first 670.7, then 0.96 on the running process
  C worker dedicated 2.0; shared: first 2.2, then 0.35 on the running process

Start-up breakdown, raw pipes (median ms)
  spawn and wait for /usr/bin/true 0.68; python -S -c pass 19.9; python -c pass 138.5; dblayer-cworker --version 0.83
  python  spawn to hello  618.41  open  99.48  first query   0.82  (second  0.42)  total  723.62
  c       spawn to hello    1.43  open   0.30  first query   0.31  (second  0.14)  total    2.06

Resident memory of one idle worker with a database open (KB)
  python 36176; c 3364
```
</details>

## Multi-tenant C pool (sqlite-cpool)

`cpool`: **ok** in 19.5 s, load average (1 min) 1.42 before, 1.33 after

<details><summary>output</summary>

```
Multi-tenant C pool (load average 1.4 0.9 0.4 at start, 1.3 after; provisional)
  new database on a warm pool: first result 1.14 ms; bare open round trip 0.379 ms
  memory per open database inside the process: 170.4 KB footprint, 170.4 KB RSS (100 databases; process base 1.7 MB footprint); parent descriptors with all open: 11
  the same with lookaside off (CPool(lookaside=(0, 0))): 131.3 KB footprint
  one-row reads, each client on its own database: requests/s (parent CPU share of wall; worker CPU in cores; worker processes; parent descriptors)
    in-process  1:    4095 (1.00; 0.00; 0p; 9fd)   4:    3229 (1.31; 0.00; 0p; 12fd)  16:    2819 (1.47; 0.00; 0p; 24fd)  64:    2950 (1.48; 0.00; 0p; 72fd)
    cworker     1:    6706 (0.74; 0.00; 1p; 11fd)   4:   11242 (0.98; 0.00; 4p; 20fd)  16:   12306 (0.98; 0.00; 16p; 56fd)  64:    9188 (0.93; 0.00; 64p; 200fd)
    cpool       1:    6113 (0.71; 0.00; 1p; 11fd)   4:   11573 (0.99; 0.00; 2p; 14fd)  16:   13454 (0.99; 0.00; 2p; 14fd)  64:   12534 (0.99; 0.00; 2p; 14fd)
```
</details>

## Multi-tenant C worker against its Rust port (sqlite-cpool, sqlite-rspool)

`rspool`: **ok** in 36.7 s, load average (1 min) 1.33 before, 1.29 after

| Measure (median over rounds) | C worker | Rust worker | Rust / C |
| --- | --- | --- | --- |
| Warm open to first result (ms) | 1.14 | 1.05 | 0.92 |
| Bare open round trip (ms) | 0.302 | 0.32 | 1.06 |
| One-row read (µs) | 303 | 285 | 0.94 |
| One-row update (µs) | 1.4e+03 | 1.3e+03 | 0.92 |
| execute 1,000 rows, msgpack (µs/row) | 1.31 | 1.34 | 1.02 |
| execute 100,000 rows, msgpack (µs/row) | 2.23 | 1.96 | 0.88 |
| stream 1,000 rows, msgpack (µs/row) | 1.34 | 1.44 | 1.07 |
| stream 100,000 rows, msgpack (µs/row) | 1.05 | 1.22 | 1.16 |
| execute 1,000 rows, JSON (µs/row) | 2.3 | 2 | 0.87 |
| execute 100,000 rows, JSON (µs/row) | 2.92 | 2.84 | 0.97 |
| stream 1,000 rows, JSON (µs/row) | 2.27 | 2.07 | 0.91 |
| stream 100,000 rows, JSON (µs/row) | 2.05 | 1.78 | 0.87 |
| Process base footprint (KB) | 840 | 1.46e+03 | 1.74 |
| Process base RSS (KB) | 2.54e+03 | 3.26e+03 | 1.28 |
| Footprint per open database (KB) | 171 | 169 | 0.99 |
| 1 clients: requests/s | 7.24e+03 | 6.51e+03 | 0.90 |
| 1 clients: parent CPU (cores) | 0.755 | 0.74 | 0.98 |
| 1 clients: worker CPU (cores) | 0 | 0 | nan |
| 4 clients: requests/s | 1.23e+04 | 1.09e+04 | 0.89 |
| 4 clients: parent CPU (cores) | 0.99 | 0.983 | 0.99 |
| 4 clients: worker CPU (cores) | 0 | 0 | nan |
| 16 clients: requests/s | 1.38e+04 | 1.33e+04 | 0.97 |
| 16 clients: parent CPU (cores) | 0.988 | 0.987 | 1.00 |
| 16 clients: worker CPU (cores) | 0 | 0 | nan |
| 64 clients: requests/s | 1.39e+04 | 1.41e+04 | 1.01 |
| 64 clients: parent CPU (cores) | 0.989 | 0.991 | 1.00 |
| 64 clients: worker CPU (cores) | 0 | 0 | nan |

## SQLite in WebAssembly (sqlite-wasm)

`wasm`: **ok** in 130.5 s, load average (1 min) 1.29 before, 1.03 after

| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.58 | 0.90 | 0.49 | 685.63 | 2.22 | 1.87 | 1.46 | 1.30 |
| One-row read, median (us) | 254 | 222 | 42 | 363 | 123 | 136 | 291 | 166 |
| One-row read, p90 (us) | 322 | 275 | 67 | 457 | 183 | 180 | 360 | 196 |
| One-row update (us) | 1393 | 964 | 966 | 1228 | 969 | 911 | 1339 | 1326 |
| Per row, 1,000-row read (us) | 1.86 | 1.29 | 1.27 | 2.99 | 1.69 | 0.98 | 1.60 | 1.45 |
| Per row, 100,000-row read (us) | 1.81 | 1.38 | 1.11 | 3.46 | 2.12 | 2.27 | 2.47 | 2.32 |
| Per row, 100,000-row stream (us) | 1.76 | 1.12 | 1.05 | 2.38 | 1.30 | 0.91 | 1.64 | 1.44 |
| execute_many, 100,000 rows (ms) | 374 | 394 | 385 | 464 | 348 | 353 | 427 | 465 |
| Memory per open connection, this process (KB) | 195 | 231 | 193 | -36 | 26 | 8 | 531 | 538 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 24261 | 376 | 5324 | 0 | 0 |

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 263 | 581 | 622 | 762 |
| sort_200k | 117 | 234 | 258 | 283 |
| instr_quadratic | 167 | 303 | 366 | 356 |

<details><summary>output</summary>

```
load average at start 1.3/1.0/0.5, at end 1.0/1.0/0.5 (provisional if high)
module: 1740 KB; compile 623 ms; load cached compilation 90.0 ms; import wasmtime 522 ms
| Measure | in-process | apsw | apsw-inline | worker-msgpack | cworker-msgpack | cpool-msgpack | wasm | wasm-inline |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| New database to first result (ms) | 0.58 | 0.90 | 0.49 | 685.63 | 2.22 | 1.87 | 1.46 | 1.30 |
| One-row read, median (us) | 254 | 222 | 42 | 363 | 123 | 136 | 291 | 166 |
| One-row read, p90 (us) | 322 | 275 | 67 | 457 | 183 | 180 | 360 | 196 |
| One-row update (us) | 1393 | 964 | 966 | 1228 | 969 | 911 | 1339 | 1326 |
| Per row, 1,000-row read (us) | 1.86 | 1.29 | 1.27 | 2.99 | 1.69 | 0.98 | 1.60 | 1.45 |
| Per row, 100,000-row read (us) | 1.81 | 1.38 | 1.11 | 3.46 | 2.12 | 2.27 | 2.47 | 2.32 |
| Per row, 100,000-row stream (us) | 1.76 | 1.12 | 1.05 | 2.38 | 1.30 | 0.91 | 1.64 | 1.44 |
| execute_many, 100,000 rows (ms) | 374 | 394 | 385 | 464 | 348 | 353 | 427 | 465 |
| Memory per open connection, this process (KB) | 195 | 231 | 193 | -36 | 26 | 8 | 531 | 538 |
| Memory per open connection, worker process (KB) | 0 | 0 | 0 | 24261 | 376 | 5324 | 0 | 0 |
host VFS: full scan of 4010 pages through the host 24.1 ms, from a warm page cache 3.5 ms (native sqlite3, cache_size 10: 3.9 ms): 5.13 us per page read; a one-row read makes 6 host calls {'lock': 1, 'access': 2, 'read': 1, 'file_size': 1, 'unlock': 1}

| CPU-bound statement | native sqlite3 (ms) | wasm, no interruption | wasm, epoch | wasm, fuel |
| --- | --- | --- | --- | --- |
| recursive_cte_1m | 263 | 581 | 622 | 762 |
| sort_200k | 117 | 234 | 258 | 283 |
| instr_quadratic | 167 | 303 | 366 | 356 |
```
</details>

## SQLite-WASM in a Node.js worker (sqlite-nodeworker)

`nodeworker`: **ok** in 19.0 s, load average (1 min) 1.03 before, 1.09 after

<details><summary>output</summary>

```
Node worker start-up and memory (load average 1.0 1.0 0.5 at start, 1.1 1.0 0.6 at end)

Spawn and wait: /usr/bin/true 0.7 ms; node -e 0 31.1 ms

Raw pipes, median ms: spawn to hello, open, first query (second), total
  python                   hello  599.0  open   95.0  first  0.91  ( 0.44)  total  694.1
  node                     hello   51.6  open  106.3  first  3.28  ( 0.98)  total  164.6
  node, no compile cache   hello   51.7  open  126.3  first  3.70  ( 1.00)  total  184.6
  node, no spare           hello   52.3  open  123.6  first  4.01  ( 0.85)  total  175.5
  c                        hello    1.2  open    0.3  first  0.32  ( 0.10)  total    1.8

Inside a Node worker, ms since its process started (median)
  node                     main loaded  35.0  wasm compiled  51.6  thread created  42.4  thread running 103.5  SQLite JS loaded 106.8  SQLite ready 137.2
  node, no compile cache   main loaded  36.1  wasm compiled  48.9  thread created  42.8  thread running 107.3  SQLite JS loaded 117.3  SQLite ready 158.3
  node, no spare           main loaded  36.2  wasm compiled  54.1  thread created  50.2  thread running 110.6  SQLite JS loaded 114.3  SQLite ready 148.7

Open to first result of a new database, through Database (median ms)
  in-process 0.92; python worker 733.29; node worker 188.22; C worker 2.54

Idle memory with a database open (MB, summed over the worker processes)
  python worker                footprint   27.1  RSS   35.4  (1 process(es))
  node worker                  footprint   84.3  RSS   86.9  (1 process(es))
  C worker                     footprint    1.7  RSS    3.4  (1 process(es))
  node worker, 4 connections   footprint  137.2  RSS  139.8  (1 process(es))

Inside Node, no protocol, in-memory database (SQLite 3.53.4 WASM vs node:sqlite 3.54.1 native)
  one-row read: WASM (oo1 API) 8.64 us, native 2.01 us
  100,000 rows into JS values: WASM raw exports 117.8 ms (oo1 exec 929.8), native .all() 223.9 ms
  WASM module load and init 83.5 ms; native open 0.31 ms
```
</details>

## DuckDB memory after sleep, in-process against workers

`duckdb_memory`: **failed** in 98.6 s, load average (1 min) 1.09 before, 1.05 after

Reason:

    Traceback (most recent call last):
      File "/data/src/benchmarks/duckdb_memory.py", line 746, in <module>
        main()
        ~~~~^^
      File "/data/src/benchmarks/duckdb_memory.py", line 733, in main
        out["overhead"] = asyncio.run(overhead(template, root))
                          ~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/opt/uv-python/cpython-3.14.8-linux-x86_64-gnu/lib/python3.14/asyncio/runners.py", line 205, in run
        return runner.run(main)
               ~~~~~~~~~~^^^^^^
      File "/opt/uv-python/cpython-3.14.8-linux-x86_64-gnu/lib/python3.14/asyncio/runners.py", line 128, in run
        return self._loop.run_until_complete(task)
               ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^
      File "/opt/uv-python/cpython-3.14.8-linux-x86_64-gnu/lib/python3.14/asyncio/base_events.py", line 720, in run_until_complete
        return future.result()
               ~~~~~~~~~~~~~^^
      File "/data/src/benchmarks/duckdb_memory.py", line 465, in overhead
        assert len(res.rows) == n
               ^^^^^^^^^^^^^^^^^^
    AssertionError

<details><summary>output</summary>

```
machine: load average [1.09, 1.0, 0.55], 8 CPUs, 0 'yes' processes

in-process n=20 cap=40 load [1.09, 1.0, 0.55] -> [1.09, 1.0, 0.55]
  start: parent 54/38 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 227/198 children 0/0 total 227/198 procs 0 | after sleep parent 143/113 children 0/0 procs 0 | wake median 70.4 ms p95 84.6 ms | wall 0.27 s | fds parent 28 children 0

in-process+gc n=20 cap=40 load [1.09, 1.0, 0.55] -> [1.09, 1.0, 0.55]
  start: parent 54/38 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 231/202 children 0/0 total 231/202 procs 0 | after sleep parent 105/76 children 0/0 procs 0 | wake median 65.9 ms p95 88.3 ms | wall 0.26 s | fds parent 28 children 0

in-process+trim n=20 cap=40 load [1.09, 1.0, 0.55] -> [1.09, 1.0, 0.55]
  start: parent 54/38 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 229/200 children 0/0 total 229/200 procs 0 | after sleep parent 100/71 children 0/0 procs 0 | wake median 67.1 ms p95 75.6 ms | wall 0.26 s | fds parent 28 children 0 | malloc_trim -> 1

in-process+settings n=20 cap=40 load [1.09, 1.0, 0.55] -> [1.08, 1.0, 0.56]
  start: parent 54/38 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 206/177 children 0/0 total 206/177 procs 0 | after sleep parent 98/69 children 0/0 procs 0 | wake median 84.4 ms p95 116.3 ms | wall 0.37 s | fds parent 28 children 0 | malloc_trim -> 1

worker n=20 cap=40 load [1.08, 1.0, 0.56] -> [1.55, 1.1, 0.59]
  start: parent 36/28 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 37/28 children 1776/782 total 1813/808 procs 20 | after sleep parent 36/28 children 0/0 procs 0 | wake median 974.5 ms p95 1064.4 ms | wall 3.01 s | fds parent 68 children 180

worker-pool n=20 cap=40 load [1.55, 1.1, 0.59] -> [1.51, 1.1, 0.59]
  start: parent 36/28 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 36/28 children 787/413 total 823/439 procs 8 | after sleep parent 36/28 children 0/0 procs 0 | wake median 220.7 ms p95 1161.9 ms | wall 1.61 s | fds parent 32 children 84

rust-worker n=20 cap=40 load [1.51, 1.1, 0.59] -> [1.51, 1.1, 0.59]
  start: parent 36/28 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 38/30 children 1238/344 total 1276/373 procs 20 | after sleep parent 38/30 children 0/0 procs 0 | wake median 84.3 ms p95 143.2 ms | wall 0.42 s | fds parent 68 children 100

cpp-worker n=20 cap=40 load [1.51, 1.1, 0.59] -> [1.47, 1.09, 0.6]
  start: parent 36/28 MB (RSS/footprint), children 0/0
  cycle 1: peak parent 38/30 children 1226/340 total 1265/370 procs 20 | after sleep parent 38/29 children 0/0 procs 0 | wake median 85.6 ms p95 126.2 ms | wall 0.36 s | fds parent 68 children 100

native (load [1.47, 1.09, 0.6] -> [1.05, 1.03, 0.61]):
  floor: {"worker": {"spawn_to_hello_ms": 870.8, "floor_mb": [51.2, 34.8], "open_ms": 40.8, "first_query_ms": 5.5, "spawn_to_first_result_ms": 917.5, "one_db_open_mb": [81.9, 52.7], "after_analytical_mb": [87.6, 58.2]}, "rust-worker": {"spawn_to_hello_ms": 11.7, "floor_mb": [20.3, 17.5], "open_ms": 46.3, "first_query_ms": 5.7, "spawn_to_first_result_ms": 65.4, "one_db_open_mb": [55.9, 52.9], "after_analytical_mb": [61.8, 58.7]}, "cpp-worker": {"spawn_to_hello_ms": 11.9, "floor_mb": [19.7, 16.8], "open_ms": 55.4, "first_query_ms": 5.9, "spawn_to_first_result_ms": 71.8, "one_db_open_mb": [56.2, 53.1], "after_analytical_mb": [60.7, 57.6]}}
  concurrent: {"in-process": {"point_read_quiet_ms": 1.79, "point_read_during_long_query_ms": 1.37, "long_query_still_running": true}, "worker": {"point_read_quiet_ms": 3.81, "point_read_during_long_query_ms": 7809.78, "long_query_still_running": false}, "rust-worker": {"point_read_quiet_ms": 2.36, "point_read_during_long_query_ms": 3.09, "long_query_still_running": true}, "cpp-worker": {"point_read_quiet_ms": 2.04, "point_read_during_long_query_ms": 2.69, "long_query_still_running": true}}
  insert_rows_10k: {"in-process": {"default_ms": 9767.4, "appender_ms": 1139.2, "speedup": 8.6}, "worker": {"default_ms": 9315.5, "appender_ms": 1395.5, "speedup": 6.7}, "rust-worker": {"default_ms": 8573.4, "appender_ms": 777.0, "speedup": 11.0}, "cpp-worker": {"default_ms": 7995.9, "appender_ms": 815.1, "speedup": 9.8}}
```
</details>

## PostgreSQL drivers: asyncpg against psycopg 3

`postgres_drivers`: **ok** in 24.8 s, load average (1 min) 1.05 before, 0.98 after

<details><summary>output</summary>

```
connect + close: asyncpg 5.13 ms, psycopg 3.10 ms, psycopg prepared 3.30 ms
  read 1 row: asyncpg 147 us, psycopg 172 us, psycopg prepared 152 us
  read 1 row, max_rows=1000: asyncpg 142 us, psycopg 185 us, psycopg prepared 178 us
  read 1,000 rows, fetch: asyncpg 2.45 ms, psycopg 2.21 ms, psycopg prepared 2.00 ms
  read 1,000 rows, fetch, max_rows=1000: asyncpg 2.12 ms, psycopg 2.23 ms, psycopg prepared 3.12 ms
  read 1,000 rows, stream: asyncpg 2.75 ms, psycopg 2.58 ms, psycopg prepared 2.85 ms
  read 100,000 rows, fetch: asyncpg 259.77 ms, psycopg 240.03 ms, psycopg prepared 243.66 ms
  read 100,000 rows, fetch, max_rows=100000: asyncpg 275.89 ms, psycopg 248.00 ms, psycopg prepared 255.48 ms
  read 100,000 rows, stream: asyncpg 211.28 ms, psycopg 170.44 ms, psycopg prepared 165.11 ms
  insert_rows 100,000, default path: asyncpg 2.08 s, psycopg 3.29 s, psycopg prepared 1.68 s
  insert_rows 100,000, COPY: asyncpg 545.33 ms, psycopg 539.44 ms, psycopg prepared 562.89 ms
  tx statement (1-row update), with savepoints: asyncpg 234 us, psycopg 300 us, psycopg prepared 279 us
  tx statement (1-row update), without savepoints: asyncpg 134 us, psycopg 183 us, psycopg prepared 141 us
  schema(), 50 tables: asyncpg 14.06 ms, psycopg 18.06 ms, psycopg prepared 12.24 ms
  schema_version(): asyncpg 3.11 ms, psycopg 5.00 ms, psycopg prepared 3.26 ms

best of 2 runs (inserts: 1), local socket, load average 1.0, 1.0, 0.6; asyncpg 0.32.0, psycopg 3.3.9 (libpq 180006, binary)

                                                          asyncpg           psycopg  psycopg prepared
-----------------------------------------------------------------------------------------------------
connect + close                                           5.13 ms           3.10 ms           3.30 ms
read 1 row                                                 147 us            172 us            152 us
read 1 row, max_rows=1000                                  142 us            185 us            178 us
read 1,000 rows, fetch                                    2.45 ms           2.21 ms           2.00 ms
read 1,000 rows, fetch, max_rows=1000                     2.12 ms           2.23 ms           3.12 ms
read 1,000 rows, stream                                   2.75 ms           2.58 ms           2.85 ms
read 100,000 rows, fetch                                259.77 ms         240.03 ms         243.66 ms
read 100,000 rows, fetch, max_rows=100000               275.89 ms         248.00 ms         255.48 ms
read 100,000 rows, stream                               211.28 ms         170.44 ms         165.11 ms
insert_rows 100,000, default path                          2.08 s            3.29 s            1.68 s
insert_rows 100,000, COPY                               545.33 ms         539.44 ms         562.89 ms
tx statement (1-row update), with savepoints               234 us            300 us            279 us
tx statement (1-row update), without savepoints            134 us            183 us            141 us
schema(), 50 tables                                      14.06 ms          18.06 ms          12.24 ms
schema_version()                                          3.11 ms           5.00 ms           3.26 ms
```
</details>

## PGlite worker against psycopg and the SQLite workers

`pglite`: **ok** in 69.8 s, load average (1 min) 0.98 before, 1.23 after

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 508.63 | 3692.84 | 505.88 | 193.43 | 2.75 | 6.27 |
| wake_ms | 445.54 | 446.43 | 454.87 | 237.75 | 2.52 | 5.81 |
| read_1_ms | 0.56 | 0.46 | 0.50 | 0.46 | 0.14 | 0.17 |
| read_1k_ms | 10.61 | 11.29 | 10.33 | 3.49 | 1.45 | 1.82 |
| read_100k_ms | 95.57 | 123.52 | 88.69 | 25.18 | 13.73 | 11.02 |
| insert_many_ms | 587.98 | 599.09 | 536.69 | 190.94 | 51.38 | 374.43 |
| insert_rows_ms | 171.07 | 179.90 | 145.37 | 122.75 | 66.27 | 75.49 |
| small_tx_ms | 2.12 | 2.40 | 2.24 | 4.20 | 3.55 | 1.12 |
| sleep_ms | 55.44 | 35.02 | 116.23 | 2.84 | 0.33 | 0.40 |
| idle_floor_mb | 148.16 | 150.44 | 429.67 | 85.25 | 1.85 | n/a |
| awake_footprint_mb | 206.82 | 205.94 | 253.11 | 166.42 | 3.91 | n/a |
| awake_4_footprint_mb | 433.72 | n/a | 936.15 | 287.55 | 4.88 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 41.87 | 41.87 | 41.87 | 0.75 | 0.75 | n/a |
| load | 0.91, 0.99, 0.62 | 0.93, 0.99, 0.63 | 1.11, 1.03, 0.65 | 1.02, 1.01, 0.64 | 1.02, 1.01, 0.64 | 1.02, 1.01, 0.64 |

<details><summary>output</summary>

```
# pglite (load average 0.98, 1.01, 0.62)
  create_ms: 508.63
  idle_floor_mb: 148.16
  empty_disk_mb: 40.08
  wake_ms: 445.54
  wake_breakdown: {'boot_ms': 89.84, 'compile_ms': 27.25, 'wait_compile_ms': 13.03, 'open_ms': 289.78, 'first_query_ms': 20.31, 'spawn_to_hello_ms': 106.68923899993388}
  read_1_ms: 0.56
  insert_many_ms: 587.98
  insert_rows_ms: 171.07
  read_1k_ms: 10.61
  read_100k_ms: 95.57
  small_tx_ms: 2.12
  awake_footprint_mb: 206.82
  disk_mb_200k_rows: 41.87
  sleep_ms: 55.44
  awake_n_footprint_mb: 433.72
  kill: {'bystander_read_ms': 983.5150510000403, 'time_limit_300_raised_after_ms': 1084.6088950000876, 'wake_after_kill_ms': 514.7922699999299}
  load: 0.91, 0.99, 0.62
# pglite-initdb (load average 0.91, 0.99, 0.62)
  create_ms: 3692.84
  idle_floor_mb: 150.44
  empty_disk_mb: 40.08
  wake_ms: 446.43
  wake_breakdown: {'boot_ms': 93.39, 'compile_ms': 30.97, 'wait_compile_ms': 14.32, 'open_ms': 285.87, 'first_query_ms': 21.75, 'spawn_to_hello_ms': 112.6257240000541}
  read_1_ms: 0.46
  insert_many_ms: 599.09
  insert_rows_ms: 179.9
  read_1k_ms: 11.29
  read_100k_ms: 123.52
  small_tx_ms: 2.4
  awake_footprint_mb: 205.94
  disk_mb_200k_rows: 41.87
  sleep_ms: 35.02
  load: 0.93, 0.99, 0.63
# pglite-turbofan (load average 0.93, 0.99, 0.63)
  create_ms: 505.88
  idle_floor_mb: 429.67
  empty_disk_mb: 40.08
  wake_ms: 454.87
  wake_breakdown: {'boot_ms': 69.9, 'compile_ms': 30.44, 'wait_compile_ms': 15.24, 'open_ms': 314.75, 'first_query_ms': 28.58, 'spawn_to_hello_ms': 87.29150300007404}
  read_1_ms: 0.5
  insert_many_ms: 536.69
  insert_rows_ms: 145.37
  read_1k_ms: 10.33
  read_100k_ms: 88.69
  small_tx_ms: 2.24
  awake_footprint_mb: 253.11
  disk_mb_200k_rows: 41.87
  sleep_ms: 116.23
  awake_n_footprint_mb: 936.15
  load: 1.11, 1.03, 0.65
# sqlite-nodeworker (load average 1.11, 1.03, 0.65)
  create_ms: 193.43
  idle_floor_mb: 85.25
  empty_disk_mb: 0.0
  wake_ms: 237.75
  wake_breakdown: {}
  read_1_ms: 0.46
  insert_many_ms: 190.94
  insert_rows_ms: 122.75
  read_1k_ms: 3.49
  read_100k_ms: 25.18
  small_tx_ms: 4.2
  awake_footprint_mb: 166.42
  disk_mb_200k_rows: 0.75
  sleep_ms: 2.84
  awake_n_footprint_mb: 287.55
  load: 1.02, 1.01, 0.64
# sqlite-cworker (load average 1.02, 1.01, 0.64)
  create_ms: 2.75
  idle_floor_mb: 1.85
  empty_disk_mb: 0.0
  wake_ms: 2.52
  wake_breakdown: {}
  read_1_ms: 0.14
  insert_many_ms: 51.38
  insert_rows_ms: 66.27
  read_1k_ms: 1.45
  read_100k_ms: 13.73
  small_tx_ms: 3.55
  awake_footprint_mb: 3.91
  disk_mb_200k_rows: 0.75
  sleep_ms: 0.33
  awake_n_footprint_mb: 4.88
  load: 1.02, 1.01, 0.64
# psycopg (load average 1.02, 1.01, 0.64)
  create_ms: 6.27
  idle_floor_mb: None
  wake_ms: 5.81
  wake_breakdown: {}
  read_1_ms: 0.17
  insert_many_ms: 374.43
  insert_rows_ms: 75.49
  read_1k_ms: 1.82
  read_100k_ms: 11.02
  small_tx_ms: 1.12
  awake_footprint_mb: None
  disk_mb_200k_rows: None
  sleep_ms: 0.4
  awake_n_footprint_mb: None
  load: 1.02, 1.01, 0.64

| measurement | pglite | pglite-initdb | pglite-turbofan | sqlite-nodeworker | sqlite-cworker | psycopg |
| --- | --- | --- | --- | --- | --- | --- |
| create_ms | 508.63 | 3692.84 | 505.88 | 193.43 | 2.75 | 6.27 |
| wake_ms | 445.54 | 446.43 | 454.87 | 237.75 | 2.52 | 5.81 |
| read_1_ms | 0.56 | 0.46 | 0.50 | 0.46 | 0.14 | 0.17 |
| read_1k_ms | 10.61 | 11.29 | 10.33 | 3.49 | 1.45 | 1.82 |
| read_100k_ms | 95.57 | 123.52 | 88.69 | 25.18 | 13.73 | 11.02 |
| insert_many_ms | 587.98 | 599.09 | 536.69 | 190.94 | 51.38 | 374.43 |
| insert_rows_ms | 171.07 | 179.90 | 145.37 | 122.75 | 66.27 | 75.49 |
| small_tx_ms | 2.12 | 2.40 | 2.24 | 4.20 | 3.55 | 1.12 |
| sleep_ms | 55.44 | 35.02 | 116.23 | 2.84 | 0.33 | 0.40 |
| idle_floor_mb | 148.16 | 150.44 | 429.67 | 85.25 | 1.85 | n/a |
| awake_footprint_mb | 206.82 | 205.94 | 253.11 | 166.42 | 3.91 | n/a |
| awake_4_footprint_mb | 433.72 | n/a | 936.15 | 287.55 | 4.88 | n/a |
| empty_disk_mb | 40.08 | 40.08 | 40.08 | 0.00 | 0.00 | n/a |
| disk_mb_200k_rows | 41.87 | 41.87 | 41.87 | 0.75 | 0.75 | n/a |
| load | 0.91, 0.99, 0.62 | 0.93, 0.99, 0.63 | 1.11, 1.03, 0.65 | 1.02, 1.01, 0.64 | 1.02, 1.01, 0.64 | 1.02, 1.01, 0.64 |

pglite wake breakdown (ms): {'boot_ms': 89.84, 'compile_ms': 27.25, 'wait_compile_ms': 13.03, 'open_ms': 289.78, 'first_query_ms': 20.31, 'spawn_to_hello_ms': 106.68923899993388}
pglite time-limit kill: {'bystander_read_ms': 983.5150510000403, 'time_limit_300_raised_after_ms': 1084.6088950000876, 'wake_after_kill_ms': 514.7922699999299}

pglite-initdb wake breakdown (ms): {'boot_ms': 93.39, 'compile_ms': 30.97, 'wait_compile_ms': 14.32, 'open_ms': 285.87, 'first_query_ms': 21.75, 'spawn_to_hello_ms': 112.6257240000541}

pglite-turbofan wake breakdown (ms): {'boot_ms': 69.9, 'compile_ms': 30.44, 'wait_compile_ms': 15.24, 'open_ms': 314.75, 'first_query_ms': 28.58, 'spawn_to_hello_ms': 87.29150300007404}

new-database options in one Node process (liftoff_only=True, load 1.02, 1.01, 0.64):
  {'rounds': 3, 'liftoff_only': True, 'compile_ms': 30.580385000000007, 'module_serialize': '2 bytes in 0.3 ms', 'initdb_open_ms': 2394.293847, 'template_copy_ms': 68.62117800000033, 'template_copy_open_ms': 254.9667330000002, 'reopen_existing_ms': 155.90245999999934, 'dump_ms': 141.6962679999997, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 495.43413299999884, 'rss_mb': 504}

new-database options in one Node process (liftoff_only=False, load 1.09, 1.03, 0.65):
  {'rounds': 3, 'liftoff_only': False, 'compile_ms': 26.405022000000017, 'module_serialize': '2 bytes in 0.1 ms', 'initdb_open_ms': 2121.375489, 'template_copy_ms': 63.48941300000024, 'template_copy_open_ms': 251.13772999999946, 'reopen_existing_ms': 183.3056030000007, 'dump_ms': 158.50927299999967, 'dump_bytes': 40594432, 'load_data_dir_open_ms': 547.8799870000003, 'rss_mb': 605}
```
</details>

## ClickHouse (chDB) against DuckDB and SQLite

`clickhouse`: **ok** in 74.4 s, load average (1 min) 1.23 before, 1.06 after

<details><summary>output</summary>

```
## lifecycle and memory   (load average 1.21, 1.06, 0.67)
import chdb 0.21s; first statement with the server down 592 ms; second database while up 8.1 ms; wake after sleep 73 ms
RSS MB: before import 35, after import 234, server up 306, after 1M insert + group by 426, after sleep 427

## analytical reads and point reads, 100,000 rows   (load average 1.21, 1.06, 0.67)
clickhouse fill 0.12s  group by 8 ms  join 13 ms  point read 5.44 ms  disk 1.0 MB
duckdb     fill 0.25s  group by 6 ms  join 6 ms  point read 1.93 ms  disk 2.1 MB
sqlite     fill 0.13s  group by 45 ms  join 51 ms  point read 0.21 ms  disk 2.4 MB

## insert 100,000 rows   (load average 1.21, 1.06, 0.67)
clickhouse native        insert_rows 100,000: 3.07s (32,545 rows/s)
clickhouse default path  insert_rows 100,000: 2.31s (43,293 rows/s)
duckdb                   insert_rows 100,000: 0.45s (223,343 rows/s)
sqlite                   insert_rows 100,000: 0.53s (186,989 rows/s)
clickhouse VALUES batches insert_rows 100,000: 247.43s (404 rows/s; 20,000 rows, scaled)
clickhouse row by row: 5.35 ms a row, 535s for 100,000 (scaled)

## export 100,000 rows   (load average 1.06, 1.04, 0.70)
clickhouse export csv 100,000 engine: 0.03s wall, 0.03s process CPU
clickhouse export csv 100,000 python fallback: 0.28s wall, 0.33s process CPU
clickhouse export json 100,000 engine: 0.04s wall, 0.05s process CPU
clickhouse export json 100,000 python fallback: 0.32s wall, 0.35s process CPU

load average at the end: 1.06, 1.04, 0.70
```
</details>

## db.export(): native serialisation against the Python fallback

`export`: **ok** in 89.0 s, load average (1 min) 1.06 before, 1.01 after

| backend | table | rows | format | mode | transport | cpu_s | wall_s | child_cpu_s | rss_growth_mb | mb_s | rows_s | lag_max_ms | lag_p99_ms | load1 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cworker | wide | 10000 | csv | native | raw | 0.015 | 0.044 | 0.032 | 0.000 | 38.494 | 228945.874 | 10.759 | 10.759 | 1.1 |
| cworker | wide | 10000 | csv | native | frames | 0.013 | 0.047 | 0.038 | 0.000 | 36.080 | 214586.984 | 7.934 | 7.934 | 1.1 |
| cworker | wide | 10000 | csv | native | fifo | 0.014 | 0.046 | 0.037 | 0.000 | 36.926 | 219623.163 | 7.696 | 7.696 | 1.1 |
| cworker | wide | 10000 | csv | fallback |  | 0.076 | 0.091 | 0.021 | 0.000 | 18.402 | 109448.065 | 15.745 | 15.745 | 1.1 |
| cworker | wide | 10000 | csv | datasette |  | 0.080 | 0.096 | 0.022 | 0.000 | 17.531 | 104515.378 | 15.842 | 15.842 | 1.1 |
| cworker | wide | 10000 | csv | handoff |  | 0.017 | 0.052 | 0.039 | 0.000 | 32.522 | 193399.122 | 13.850 | 13.850 | 1.1 |
| cworker | wide | 10000 | json | native | raw | 0.016 | 0.050 | 0.038 | 0.000 | 46.395 | 198514.390 | 10.359 | 10.359 | 1.1 |
| cworker | wide | 10000 | json | native | frames | 0.016 | 0.055 | 0.041 | 0.000 | 42.402 | 181426.018 | 7.059 | 7.059 | 1.1 |
| cworker | wide | 10000 | json | native | fifo | 0.015 | 0.045 | 0.036 | 0.000 | 51.486 | 220296.595 | 10.185 | 10.185 | 1.1 |
| cworker | wide | 10000 | json | fallback |  | 0.048 | 0.060 | 0.018 | 0.000 | 39.038 | 167033.444 | 9.500 | 9.500 | 1.1 |
| cworker | wide | 10000 | json | datasette |  | 0.063 | 0.079 | 0.022 | 0.000 | 31.357 | 127099.905 | 29.441 | 29.441 | 1.1 |
| cworker | wide | 10000 | json | handoff |  | 0.015 | 0.058 | 0.047 | 0.000 | 40.287 | 172354.099 | 11.041 | 11.041 | 1.0 |
| cworker | narrow | 10000 | csv | native | raw | 0.011 | 0.018 | 0.009 | 0.000 | 11.913 | 564219.300 | 10.506 | 10.506 | 1.0 |
| cworker | narrow | 10000 | csv | fallback |  | 0.024 | 0.031 | 0.011 | 0.000 | 6.881 | 325901.625 | 4.512 | 4.512 | 1.0 |
| cworker | narrow | 10000 | json | native | raw | 0.012 | 0.021 | 0.011 | 0.000 | 18.444 | 471466.205 | 11.283 | 11.283 | 1.0 |
| cworker | narrow | 10000 | json | fallback |  | 0.021 | 0.028 | 0.011 | 0.000 | 14.065 | 359523.377 | 3.446 | 3.446 | 1.0 |
| duckdb | wide | 10000 | csv | native |  | 0.061 | 0.059 | 0.000 | 0.000 | 28.503 | 169521.209 | 6.358 | 6.358 | 1.0 |
| duckdb | wide | 10000 | csv | fallback |  | 0.118 | 0.118 | 0.000 | 0.000 | 14.266 | 84851.051 | 8.717 | 8.717 | 1.0 |
| duckdb | wide | 10000 | csv | datasette |  | 0.127 | 0.130 | 0.000 | 0.000 | 12.948 | 77195.412 | 9.343 | 9.343 | 1.0 |
| duckdb | wide | 10000 | json | native |  | 0.065 | 0.062 | 0.000 | 0.000 | 37.637 | 161040.004 | 3.572 | 3.572 | 1.0 |
| duckdb | wide | 10000 | json | fallback |  | 0.089 | 0.088 | 0.000 | 0.000 | 26.494 | 113361.393 | 5.564 | 5.564 | 1.0 |
| duckdb | wide | 10000 | json | datasette |  | 0.097 | 0.097 | 0.000 | 0.000 | 25.466 | 103221.543 | 44.006 | 44.006 | 1.0 |
| duckdb | narrow | 10000 | csv | native |  | 0.027 | 0.026 | 0.000 | 0.000 | 8.185 | 387647.804 | 3.730 | 3.730 | 1.0 |
| duckdb | narrow | 10000 | csv | fallback |  | 0.034 | 0.033 | 0.000 | 0.000 | 6.333 | 299971.260 | 2.640 | 2.640 | 1.0 |
| duckdb | narrow | 10000 | json | native |  | 0.025 | 0.023 | 0.000 | 0.000 | 16.846 | 430634.661 | 3.679 | 3.679 | 1.0 |
| duckdb | narrow | 10000 | json | fallback |  | 0.040 | 0.040 | 0.000 | 0.000 | 9.897 | 252983.990 | 2.343 | 2.343 | 1.0 |
| postgres | wide | 10000 | csv | native |  | 0.036 | 0.042 | 0.000 | 0.000 | 40.283 | 239583.996 | 4.981 | 4.981 | 1.0 |
| postgres | wide | 10000 | csv | fallback |  | 0.110 | 0.115 | 0.000 | 0.000 | 14.678 | 87297.380 | 12.142 | 12.142 | 1.0 |
| postgres | wide | 10000 | csv | datasette |  | 0.086 | 0.091 | 0.000 | 0.000 | 18.534 | 110496.169 | 8.743 | 8.743 | 1.0 |
| postgres | wide | 10000 | json | native |  | 0.040 | 0.046 | 0.000 | 0.000 | 50.949 | 217998.772 | 1.203 | 1.203 | 1.0 |
| postgres | wide | 10000 | json | fallback |  | 0.089 | 0.094 | 0.000 | 0.000 | 24.796 | 106095.721 | 8.692 | 8.692 | 1.0 |
| postgres | wide | 10000 | json | datasette |  | 0.097 | 0.103 | 0.000 | 0.000 | 23.846 | 96655.342 | 56.897 | 56.897 | 1.0 |
| postgres | narrow | 10000 | csv | native |  | 0.025 | 0.030 | 0.000 | 0.000 | 7.118 | 337114.920 | 3.048 | 3.048 | 1.0 |
| postgres | narrow | 10000 | csv | fallback |  | 0.036 | 0.038 | 0.000 | 0.000 | 5.522 | 261523.770 | 3.191 | 3.191 | 1.0 |
| postgres | narrow | 10000 | json | native |  | 0.026 | 0.029 | 0.000 | 0.000 | 13.584 | 347239.295 | 1.990 | 1.990 | 1.0 |
| postgres | narrow | 10000 | json | fallback |  | 0.030 | 0.033 | 0.000 | 0.000 | 11.757 | 300531.809 | 3.138 | 3.138 | 1.0 |
| cworker | wide | 100000 | csv | native | raw | 0.083 | 0.476 | 0.418 | 0.000 | 35.934 | 209949.297 | 12.146 | 1.142 | 1.0 |
| cworker | wide | 100000 | csv | native | frames | 0.088 | 0.463 | 0.401 | 0.000 | 36.947 | 215867.418 | 9.984 | 1.131 | 1.0 |
| cworker | wide | 100000 | csv | native | fifo | 0.062 | 0.371 | 0.346 | 0.000 | 46.076 | 269205.062 | 12.785 | 1.315 | 1.0 |
| cworker | wide | 100000 | csv | fallback |  | 0.883 | 1.057 | 0.200 | 0.000 | 16.189 | 94584.517 | 19.146 | 18.921 | 1.0 |
| cworker | wide | 100000 | csv | datasette |  | 0.782 | 0.937 | 0.187 | 0.000 | 18.223 | 106720.001 | 19.911 | 17.199 | 1.0 |
| cworker | wide | 100000 | csv | handoff |  | 0.033 | 0.355 | 0.339 | 0.000 | 48.249 | 281858.982 | 10.514 | 1.012 | 1.0 |
| cworker | wide | 100000 | json | native | raw | 0.077 | 0.438 | 0.378 | 0.000 | 54.013 | 228162.975 | 9.448 | 1.044 | 1.0 |
| cworker | wide | 100000 | json | native | frames | 0.085 | 0.460 | 0.396 | 0.000 | 51.482 | 217471.763 | 7.457 | 1.103 | 1.0 |
| cworker | wide | 100000 | json | native | fifo | 0.084 | 0.467 | 0.427 | 0.000 | 50.649 | 213952.936 | 10.531 | 1.347 | 1.0 |
| cworker | wide | 100000 | json | fallback |  | 0.424 | 0.567 | 0.175 | 0.000 | 41.744 | 176336.927 | 13.646 | 8.725 | 1.0 |
| cworker | wide | 100000 | json | datasette |  | 0.605 | 0.771 | 0.201 | 18.670 | 32.378 | 129654.475 | 302.749 | 9.782 | 1.0 |
| cworker | wide | 100000 | json | handoff |  | 0.034 | 0.361 | 0.347 | 0.000 | 65.645 | 277260.932 | 11.342 | 0.590 | 1.0 |
| cworker | narrow | 100000 | csv | native | raw | 0.019 | 0.086 | 0.074 | 0.000 | 28.064 | 1163884.194 | 10.045 | 10.045 | 1.0 |
| cworker | narrow | 100000 | csv | fallback |  | 0.245 | 0.303 | 0.084 | 0.000 | 7.957 | 329998.650 | 5.114 | 4.905 | 1.0 |
| cworker | narrow | 100000 | json | native | raw | 0.021 | 0.103 | 0.088 | 0.000 | 40.897 | 971134.899 | 7.794 | 7.794 | 1.0 |
| cworker | narrow | 100000 | json | fallback |  | 0.235 | 0.298 | 0.085 | 0.000 | 14.143 | 335831.068 | 5.170 | 4.699 | 1.0 |
| duckdb | wide | 100000 | csv | native |  | 0.245 | 0.192 | 0.000 | 2.785 | 88.924 | 519546.644 | 4.230 | 1.393 | 1.0 |
| duckdb | wide | 100000 | csv | fallback |  | 1.125 | 1.100 | 0.000 | 0.000 | 15.566 | 90946.122 | 10.531 | 10.109 | 1.0 |
| duckdb | wide | 100000 | csv | datasette |  | 0.988 | 0.968 | 0.000 | 0.000 | 17.638 | 103294.718 | 8.971 | 8.734 | 1.0 |
| duckdb | wide | 100000 | json | native |  | 0.279 | 0.224 | 0.000 | 8.499 | 105.531 | 445787.440 | 4.391 | 1.234 | 1.0 |
| duckdb | wide | 100000 | json | fallback |  | 0.770 | 0.749 | 0.000 | 0.000 | 31.594 | 133462.041 | 10.618 | 6.628 | 1.0 |
| duckdb | wide | 100000 | json | datasette |  | 0.755 | 0.740 | 0.000 | 114.618 | 33.739 | 135100.763 | 371.765 | 3.634 | 1.0 |
| duckdb | narrow | 100000 | csv | native |  | 0.105 | 0.101 | 0.000 | 0.000 | 23.832 | 988369.715 | 3.229 | 3.229 | 1.0 |
| duckdb | narrow | 100000 | csv | fallback |  | 0.302 | 0.302 | 0.000 | 0.000 | 7.986 | 331191.559 | 3.105 | 2.741 | 1.0 |
| duckdb | narrow | 100000 | json | native |  | 0.120 | 0.115 | 0.000 | 0.000 | 36.565 | 868268.801 | 4.586 | 4.586 | 1.0 |
| duckdb | narrow | 100000 | json | fallback |  | 0.330 | 0.332 | 0.000 | 0.000 | 12.685 | 301202.146 | 3.977 | 3.329 | 1.0 |
| postgres | wide | 100000 | csv | native |  | 0.276 | 0.313 | 0.000 | 0.000 | 54.603 | 319021.739 | 1.615 | 0.700 | 1.0 |
| postgres | wide | 100000 | csv | fallback |  | 1.108 | 1.150 | 0.000 | 0.000 | 14.877 | 86921.046 | 12.961 | 12.849 | 1.0 |
| postgres | wide | 100000 | csv | datasette |  | 1.086 | 1.127 | 0.000 | 0.000 | 15.155 | 88753.441 | 14.736 | 12.729 | 1.0 |
| postgres | wide | 100000 | json | native |  | 0.346 | 0.411 | 0.000 | 0.000 | 57.622 | 243408.312 | 1.259 | 0.664 | 1.0 |
| postgres | wide | 100000 | json | fallback |  | 0.671 | 0.705 | 0.000 | 0.000 | 33.574 | 141824.802 | 8.310 | 8.051 | 1.0 |
| postgres | wide | 100000 | json | datasette |  | 0.708 | 0.742 | 0.000 | 28.672 | 33.634 | 134681.666 | 360.910 | 6.574 | 1.0 |
| postgres | narrow | 100000 | csv | native |  | 0.209 | 0.228 | 0.000 | 0.000 | 10.577 | 438665.002 | 3.141 | 3.141 | 1.0 |
| postgres | narrow | 100000 | csv | fallback |  | 0.224 | 0.245 | 0.000 | 0.000 | 9.834 | 407834.309 | 2.349 | 2.145 | 1.0 |
| postgres | narrow | 100000 | json | native |  | 0.205 | 0.226 | 0.000 | 0.000 | 18.646 | 442771.990 | 2.021 | 1.920 | 1.0 |
| postgres | narrow | 100000 | json | fallback |  | 0.248 | 0.265 | 0.000 | 0.000 | 15.875 | 376970.990 | 2.511 | 2.451 | 1.0 |

<details><summary>output</summary>

```
# load average at start: (1.05615234375, 1.04296875, 0.697265625)
backend	table	rows	format	mode	transport	cpu_s	wall_s	child_cpu_s	rss_growth_mb	mb_s	rows_s	lag_max_ms	lag_p99_ms	load1
cworker	wide	10000	csv	native	raw	0.015	0.044	0.032	0.000	38.494	228945.874	10.759	10.759	1.1
cworker	wide	10000	csv	native	frames	0.013	0.047	0.038	0.000	36.080	214586.984	7.934	7.934	1.1
cworker	wide	10000	csv	native	fifo	0.014	0.046	0.037	0.000	36.926	219623.163	7.696	7.696	1.1
cworker	wide	10000	csv	fallback		0.076	0.091	0.021	0.000	18.402	109448.065	15.745	15.745	1.1
cworker	wide	10000	csv	datasette		0.080	0.096	0.022	0.000	17.531	104515.378	15.842	15.842	1.1
cworker	wide	10000	csv	handoff		0.017	0.052	0.039	0.000	32.522	193399.122	13.850	13.850	1.1
cworker	wide	10000	json	native	raw	0.016	0.050	0.038	0.000	46.395	198514.390	10.359	10.359	1.1
cworker	wide	10000	json	native	frames	0.016	0.055	0.041	0.000	42.402	181426.018	7.059	7.059	1.1
cworker	wide	10000	json	native	fifo	0.015	0.045	0.036	0.000	51.486	220296.595	10.185	10.185	1.1
cworker	wide	10000	json	fallback		0.048	0.060	0.018	0.000	39.038	167033.444	9.500	9.500	1.1
cworker	wide	10000	json	datasette		0.063	0.079	0.022	0.000	31.357	127099.905	29.441	29.441	1.1
cworker	wide	10000	json	handoff		0.015	0.058	0.047	0.000	40.287	172354.099	11.041	11.041	1.0
cworker	narrow	10000	csv	native	raw	0.011	0.018	0.009	0.000	11.913	564219.300	10.506	10.506	1.0
cworker	narrow	10000	csv	fallback		0.024	0.031	0.011	0.000	6.881	325901.625	4.512	4.512	1.0
cworker	narrow	10000	json	native	raw	0.012	0.021	0.011	0.000	18.444	471466.205	11.283	11.283	1.0
cworker	narrow	10000	json	fallback		0.021	0.028	0.011	0.000	14.065	359523.377	3.446	3.446	1.0
duckdb	wide	10000	csv	native		0.061	0.059	0.000	0.000	28.503	169521.209	6.358	6.358	1.0
duckdb	wide	10000	csv	fallback		0.118	0.118	0.000	0.000	14.266	84851.051	8.717	8.717	1.0
duckdb	wide	10000	csv	datasette		0.127	0.130	0.000	0.000	12.948	77195.412	9.343	9.343	1.0
duckdb	wide	10000	json	native		0.065	0.062	0.000	0.000	37.637	161040.004	3.572	3.572	1.0
duckdb	wide	10000	json	fallback		0.089	0.088	0.000	0.000	26.494	113361.393	5.564	5.564	1.0
duckdb	wide	10000	json	datasette		0.097	0.097	0.000	0.000	25.466	103221.543	44.006	44.006	1.0
duckdb	narrow	10000	csv	native		0.027	0.026	0.000	0.000	8.185	387647.804	3.730	3.730	1.0
duckdb	narrow	10000	csv	fallback		0.034	0.033	0.000	0.000	6.333	299971.260	2.640	2.640	1.0
duckdb	narrow	10000	json	native		0.025	0.023	0.000	0.000	16.846	430634.661	3.679	3.679	1.0
duckdb	narrow	10000	json	fallback		0.040	0.040	0.000	0.000	9.897	252983.990	2.343	2.343	1.0
postgres	wide	10000	csv	native		0.036	0.042	0.000	0.000	40.283	239583.996	4.981	4.981	1.0
postgres	wide	10000	csv	fallback		0.110	0.115	0.000	0.000	14.678	87297.380	12.142	12.142	1.0
postgres	wide	10000	csv	datasette		0.086	0.091	0.000	0.000	18.534	110496.169	8.743	8.743	1.0
postgres	wide	10000	json	native		0.040	0.046	0.000	0.000	50.949	217998.772	1.203	1.203	1.0
postgres	wide	10000	json	fallback		0.089	0.094	0.000	0.000	24.796	106095.721	8.692	8.692	1.0
postgres	wide	10000	json	datasette		0.097	0.103	0.000	0.000	23.846	96655.342	56.897	56.897	1.0
postgres	narrow	10000	csv	native		0.025	0.030	0.000	0.000	7.118	337114.920	3.048	3.048	1.0
postgres	narrow	10000	csv	fallback		0.036	0.038	0.000	0.000	5.522	261523.770	3.191	3.191	1.0
postgres	narrow	10000	json	native		0.026	0.029	0.000	0.000	13.584	347239.295	1.990	1.990	1.0
postgres	narrow	10000	json	fallback		0.030	0.033	0.000	0.000	11.757	300531.809	3.138	3.138	1.0
cworker	wide	100000	csv	native	raw	0.083	0.476	0.418	0.000	35.934	209949.297	12.146	1.142	1.0
cworker	wide	100000	csv	native	frames	0.088	0.463	0.401	0.000	36.947	215867.418	9.984	1.131	1.0
cworker	wide	100000	csv	native	fifo	0.062	0.371	0.346	0.000	46.076	269205.062	12.785	1.315	1.0
cworker	wide	100000	csv	fallback		0.883	1.057	0.200	0.000	16.189	94584.517	19.146	18.921	1.0
cworker	wide	100000	csv	datasette		0.782	0.937	0.187	0.000	18.223	106720.001	19.911	17.199	1.0
cworker	wide	100000	csv	handoff		0.033	0.355	0.339	0.000	48.249	281858.982	10.514	1.012	1.0
cworker	wide	100000	json	native	raw	0.077	0.438	0.378	0.000	54.013	228162.975	9.448	1.044	1.0
cworker	wide	100000	json	native	frames	0.085	0.460	0.396	0.000	51.482	217471.763	7.457	1.103	1.0
cworker	wide	100000	json	native	fifo	0.084	0.467	0.427	0.000	50.649	213952.936	10.531	1.347	1.0
cworker	wide	100000	json	fallback		0.424	0.567	0.175	0.000	41.744	176336.927	13.646	8.725	1.0
cworker	wide	100000	json	datasette		0.605	0.771	0.201	18.670	32.378	129654.475	302.749	9.782	1.0
cworker	wide	100000	json	handoff		0.034	0.361	0.347	0.000	65.645	277260.932	11.342	0.590	1.0
cworker	narrow	100000	csv	native	raw	0.019	0.086	0.074	0.000	28.064	1163884.194	10.045	10.045	1.0
cworker	narrow	100000	csv	fallback		0.245	0.303	0.084	0.000	7.957	329998.650	5.114	4.905	1.0
cworker	narrow	100000	json	native	raw	0.021	0.103	0.088	0.000	40.897	971134.899	7.794	7.794	1.0
cworker	narrow	100000	json	fallback		0.235	0.298	0.085	0.000	14.143	335831.068	5.170	4.699	1.0
duckdb	wide	100000	csv	native		0.245	0.192	0.000	2.785	88.924	519546.644	4.230	1.393	1.0
duckdb	wide	100000	csv	fallback		1.125	1.100	0.000	0.000	15.566	90946.122	10.531	10.109	1.0
duckdb	wide	100000	csv	datasette		0.988	0.968	0.000	0.000	17.638	103294.718	8.971	8.734	1.0
duckdb	wide	100000	json	native		0.279	0.224	0.000	8.499	105.531	445787.440	4.391	1.234	1.0
duckdb	wide	100000	json	fallback		0.770	0.749	0.000	0.000	31.594	133462.041	10.618	6.628	1.0
duckdb	wide	100000	json	datasette		0.755	0.740	0.000	114.618	33.739	135100.763	371.765	3.634	1.0
duckdb	narrow	100000	csv	native		0.105	0.101	0.000	0.000	23.832	988369.715	3.229	3.229	1.0
duckdb	narrow	100000	csv	fallback		0.302	0.302	0.000	0.000	7.986	331191.559	3.105	2.741	1.0
duckdb	narrow	100000	json	native		0.120	0.115	0.000	0.000	36.565	868268.801	4.586	4.586	1.0
duckdb	narrow	100000	json	fallback		0.330	0.332	0.000	0.000	12.685	301202.146	3.977	3.329	1.0
postgres	wide	100000	csv	native		0.276	0.313	0.000	0.000	54.603	319021.739	1.615	0.700	1.0
postgres	wide	100000	csv	fallback		1.108	1.150	0.000	0.000	14.877	86921.046	12.961	12.849	1.0
postgres	wide	100000	csv	datasette		1.086	1.127	0.000	0.000	15.155	88753.441	14.736	12.729	1.0
postgres	wide	100000	json	native		0.346	0.411	0.000	0.000	57.622	243408.312	1.259	0.664	1.0
postgres	wide	100000	json	fallback		0.671	0.705	0.000	0.000	33.574	141824.802	8.310	8.051	1.0
postgres	wide	100000	json	datasette		0.708	0.742	0.000	28.672	33.634	134681.666	360.910	6.574	1.0
postgres	narrow	100000	csv	native		0.209	0.228	0.000	0.000	10.577	438665.002	3.141	3.141	1.0
postgres	narrow	100000	csv	fallback		0.224	0.245	0.000	0.000	9.834	407834.309	2.349	2.145	1.0
postgres	narrow	100000	json	native		0.205	0.226	0.000	0.000	18.646	442771.990	2.021	1.920	1.0
postgres	narrow	100000	json	fallback		0.248	0.265	0.000	0.000	15.875	376970.990	2.511	2.451	1.0
```
</details>

## Scale backends: wake cost and query cost, separately

`scale_wake_query`: **ok** in 94.4 s, load average (1 min) 1.01 before, 1.15 after

| backend | wake_ms | warm_median_ms | warm_p90_ms | cold_median_ms | cold_p90_ms | sleep_median_ms | load1_before | load1_after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sqlite | 0.311 | 0.186 | 0.225 | 0.497 | 0.633 | 0.212 | 1.01 | 1.01 |
| sqlite-wasm | 1.439 | 0.293 | 0.389 | 1.731 | 2.103 | 0.312 | 1.01 | 1.01 |
| duckdb | 21.107 | 1.067 | 1.459 | 22.174 | 26.910 | 4.441 | 1.01 | 1.01 |
| sqlite-worker | 698.432 | 0.474 | 0.678 | 698.906 | 787.259 | 0.205 | 1.01 | 1.00 |
| sqlite-cworker | 2.316 | 0.185 | 0.270 | 2.502 | 3.062 | 0.167 | 1.00 | 1.00 |
| sqlite-nodeworker | 161.720 | 0.903 | 1.309 | 162.623 | 174.354 | 1.484 | 1.00 | 1.15 |
| sqlite-cpool | 1.671 | 0.133 | 0.184 | 1.804 | 2.218 | 0.206 | 1.15 | 1.15 |
| sqlite-rspool | 1.644 | 0.128 | 0.180 | 1.772 | 2.117 | 0.195 | 1.15 | 1.15 |

## Scale tests (tests/scale, through their harness)

`scale_tests`: **ok** in 1047.3 s, load average (1 min) 1.15 before, 58.65 after

```
[scale:sqlite] 300 databases, created in 0.0s
  register: 9.4 us/db; best-of-3 3.09 us/db at 100, 2.71 us/db at 300
  asleep, never used: 464 B/db (tracemalloc), 14 B/db (RSS delta)
  asleep after a wake/sleep cycle: +14.7 B/db growth (tracemalloc, 300 dbs)
  load: 20000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 10.4s (1931 req/s)
  fds: baseline 9, peak 77, limit 97 (cap 64 x 1 per place + allowance 24), after sleep 9; 1794 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=6508 sleeps=6459 cap_evictions=10579 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-wasm] 300 databases, created in 0.0s
  register: 105.9 us/db; best-of-3 19.58 us/db at 100, 17.52 us/db at 300
  asleep, never used: 899 B/db (tracemalloc), 55 B/db (RSS delta)
  asleep after a wake/sleep cycle: +173.5 B/db growth (tracemalloc, 300 dbs)
  load: 20000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 32.6s (614 req/s)
  fds: baseline 11, peak 67, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 2967 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=6599 sleeps=6553 cap_evictions=10339 idle_closes=0; after idle timeout: awake=0 open=0

[scale:duckdb] 300 databases, created in 0.1s
  register: 49.5 us/db; best-of-3 11.72 us/db at 100, 10.23 us/db at 300
  asleep, never used: 1278 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +84.1 B/db growth (tracemalloc, 300 dbs)
  load: 3000 requests (10% writes) over 272 databases, Zipf s=1.1, 96 workers, in 10.9s (276 req/s)
  fds: baseline 11, peak 75, limit 99 (cap 64 x 1 per place + allowance 24), after sleep 11; 1949 samples
  threads: 10 before, 10-10 during
  registry after load: wakes=1062 sleeps=1044 cap_evictions=1358 idle_closes=357; after idle timeout: awake=0 open=0

[scale:sqlite-worker] 300 databases, created in 0.0s
  register: 97.8 us/db; best-of-3 20.47 us/db at 100, 18.12 us/db at 300
  asleep, never used: 778 B/db (tracemalloc), 150 B/db (RSS delta)
  asleep after a wake/sleep cycle: +133.0 B/db growth (tracemalloc, 300 dbs)
  load: 2000 requests (10% writes) over 242 databases, Zipf s=1.1, 96 workers, in 148.9s (13 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 14581 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=719 sleeps=705 cap_evictions=1056 idle_closes=268; after idle timeout: awake=0 open=0

[scale:sqlite-cworker] 300 databases, created in 0.0s
  register: 186.1 us/db; best-of-3 36.25 us/db at 100, 39.02 us/db at 300
  asleep, never used: 787 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +161.0 B/db growth (tracemalloc, 300 dbs)
  load: 10000 requests (10% writes) over 298 databases, Zipf s=1.1, 96 workers, in 11.7s (853 req/s)
  fds: baseline 11, peak 203, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 986 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=3436 sleeps=3396 cap_evictions=5102 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-nodeworker] 300 databases, created in 0.0s
  register: 220.7 us/db; best-of-3 51.38 us/db at 100, 50.81 us/db at 300
  asleep, never used: 1485 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +1098.0 B/db growth (tracemalloc, 300 dbs)
  load: 2000 requests (10% writes) over 242 databases, Zipf s=1.1, 96 workers, in 44.1s (45 req/s)
  fds: baseline 11, peak 164, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 3194 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=756 sleeps=739 cap_evictions=1132 idle_closes=304; after idle timeout: awake=0 open=0

[scale:sqlite-cpool] 300 databases, created in 0.0s
  register: 209.0 us/db; best-of-3 40.23 us/db at 100, 43.27 us/db at 300
  asleep, never used: 995 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +93.8 B/db growth (tracemalloc, 300 dbs)
  load: 50000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 9.6s (5213 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 2133 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=16165 sleeps=16121 cap_evictions=25080 idle_closes=0; after idle timeout: awake=0 open=0

[scale:sqlite-rspool] 300 databases, created in 0.0s
  register: 203.6 us/db; best-of-3 36.97 us/db at 100, 43.10 us/db at 300
  asleep, never used: 985 B/db (tracemalloc), 0 B/db (RSS delta)
  asleep after a wake/sleep cycle: +58.2 B/db growth (tracemalloc, 300 dbs)
  load: 50000 requests (10% writes) over 300 databases, Zipf s=1.1, 96 workers, in 10.2s (4882 req/s)
  fds: baseline 11, peak 17, limit 227 (cap 64 x 3 per place + allowance 24), after sleep 11; 2230 samples
  threads: 9 before, 9-9 during
  registry after load: wakes=16189 sleeps=16143 cap_evictions=25544 idle_closes=0; after idle timeout: awake=0 open=0

[threads:sqlite] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

[threads:sqlite-wasm] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:duckdb] standalone, 1000 open: 10 before, 10-10 during, 10 after, new: []

[threads:sqlite-worker] standalone, 100 open: 10 before, 9-9 during, 9 after, new: []

[threads:sqlite-cworker] standalone, 1000 open: 9 before, 9-9 during, 9 after, new: []

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

pytest: `24 passed, 1 deselected in 1045.23s (0:17:25)`
