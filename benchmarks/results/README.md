# Benchmark results

One directory per run of `benchmarks/run_all.py`, named
`<UTC start>-<commit>[-quick]`. Compare figures within one directory only:
each was one session on one machine under one set of settings.

| Directory | What it is |
| --- | --- |
| `2026-11-24T1753Z-6e409fa9d0-quick` | **Trial 1 of the Fly.io mechanism (`fly/`), not final numbers.** `--quick`, performance-8x in sjc. Python process start-up is inflated here: the image's standard library had no `.pyc` files and the benchmark user could not write them (`python -c pass` 138 ms); fixed in `fly/Dockerfile` afterwards. `duckdb_memory` failed on a bad quick argument (fixed). The test-suite step was stopped by hand at 2% (it was too slow with the `.pyc` problem). |
