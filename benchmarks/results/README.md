# Benchmark results

One directory per run of `benchmarks/run_all.py`, named
`<UTC start>-<commit>[-quick]`. Compare figures within one directory only:
each was one session on one machine under one set of settings.

| Directory | What it is |
| --- | --- |
| `2026-11-24T1753Z-6e409fa9d0-quick` | **Trial 1 of the Fly.io mechanism (`fly/`), not final numbers.** `--quick`, performance-8x in sjc. Python process start-up is inflated here: the image's standard library had no `.pyc` files and the benchmark user could not write them (`python -c pass` 138 ms); fixed in `fly/Dockerfile` afterwards. `duckdb_memory` failed on a bad quick argument (fixed). The test-suite step was stopped by hand at 2% (it was too slow with the `.pyc` problem). |
| `2026-11-24T1948Z-tests-only` | Not a benchmark: the test suite alone at `d65fc99` (after the CI-hang fix a474e36), every backend built, `-o faulthandler_timeout=240 --timeout=300`. Stopped by hand at 56% (27.7 minutes) to free the machine for the full run: `5704 passed, 77 skipped, 4 xfailed`, no failures, no hang; peak memory 3.7 GB used of 15.6 GB (`machine-logs/memory.log`). |
| `2026-11-24T1841Z-a10d1fd909-quick` | **Trial 2 of the Fly.io mechanism, not final numbers.** `--quick`, same machine size, standard library precompiled. All 12 topics ok. `cpool`/`rspool` worker CPU still reads 0 here (procps `ps` has whole-second resolution; fixed in `cpool_bench.py` afterwards). The test-suite step (`machine-logs/tests.log`, `memory.log`) was stopped by hand at 26% to rerun it alone with a longer cap; one failure by then: `test_sleep_leaks_no_file_descriptors[clickhouse]` (220 descriptors after sleep against 217 before). |
