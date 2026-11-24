# Benchmarks on a dedicated Fly.io machine

Everything needed to run dblayer's benchmarks (`benchmarks/run_all.py`) on
one dedicated-CPU Linux machine, fetch the results and destroy everything
afterwards. A laptop running other work gives noisy figures; this machine
runs nothing but the benchmarks, one at a time.

Nothing in this folder is secret. flyctl's own login (kept in `~/.fly`, never
in this repository) authorises the commands; the machine clones this public
repository without credentials, and PostgreSQL inside it accepts only local
Unix-socket connections (trust auth, no TCP, no password).

## What gets created

| Resource | Setting (`fly/config.sh`, all overridable from the environment) |
| --- | --- |
| App | `dblayer-bench` in org `datasette-cloud-dev`; no IPs, certificates or services |
| Volume | `bench_data`, 30 GB, region `sjc`, no scheduled snapshots, mounted at `/data` |
| Machine | `performance-8x` (8 dedicated CPUs, 16 GB), region `sjc`, `--rm` (auto_destroy), restart policy `no` |
| Image | `registry.fly.io/dblayer-bench:bench-<hash of Dockerfile and machine/>`, built on Fly's Depot builder |

The image (`Dockerfile`) holds toolchains only: Debian 13, PostgreSQL 18
(apt.postgresql.org), uv with CPython 3.12, 3.13 and 3.14, Node.js 24 (newest
24.x at build time), Rust stable with clippy, gcc/g++ and make. The code is
cloned at run time at the git ref you give, and every backend is built from
that fresh clone (`machine/run.sh`): the C worker and its WebAssembly module,
the Rust SQLite and DuckDB workers, the C++ DuckDB worker, the Node and
PGlite workers' npm packages, and `uv sync --all-extras --group demo` on
Python 3.14. A build that fails is recorded and the run carries on; the
runner then records those backends as skipped, with the reason.

## Running it

From the repository root, with `fly` logged in:

    fly/bench.sh main quick          # trial: ~25 minutes end to end
    fly/bench.sh main full           # final numbers
    fly/bench.sh <ref> quick tests   # also run the test suite (hang hunting)

`bench.sh` is the steps below in order, with `teardown.sh` on exit (also on
Ctrl-C or an error). The steps can be run one by one:

    fly/create.sh                    # app and volume (idempotent)
    fly/build-image.sh               # build and push the image (~3 min first time, seconds when cached)
    fly/launch.sh main quick         # start the machine; returns at once
    fly/fetch.sh                     # wait, copy the results into benchmarks/results/, release the machine
    fly/teardown.sh                  # destroy machine, volume and app; verify (idempotent)

Follow a run with `fly logs -a dblayer-bench`, or
`fly ssh console -a dblayer-bench -C "cat /data/out/STATUS"`.

## Results

`benchmarks/run_all.py` writes `results.md` (one section per topic),
`results.jsonl` (one line per measurement, each carrying the machine
description, commit, tool versions and load average before and after its
topic), `context.json`, and every script's raw output under `logs/` and
`raw/`. `fetch.sh` copies them to `benchmarks/results/<UTC date>-<commit>[-quick]/`,
adding `stages.jsonl` (how long each build stage took) and `machine-logs/`
(build and test logs).

Results leave the machine two ways, so a lost connection does not lose them:

1. `fly ssh sftp get` of `/data/out/results.tar.gz` (the primary path, run by
   `fetch.sh`). The machine waits up to `HOLD_S` (30 minutes) after the run
   for this, and exits as soon as `fetch.sh` touches `/data/out/RELEASE`.
2. A copy in the machine's log: `results.md` between
   `===== BEGIN/END DBLAYER RESULTS MARKDOWN =====` lines, and the tarball as
   base64 between `===== BEGIN/END DBLAYER RESULTS TARBALL BASE64 =====` lines
   when it is under 3 MB. `fetch.sh` falls back to this when sftp fails.

The volume also keeps everything until `teardown.sh` runs.

## Cost, and why it cannot run away

Fly bills a machine by the second while it runs. Fly's pricing page
(docs.fly.io/about/pricing, read 2026-11-24) lists `performance-1x` at
$33.00/month and `performance-2x` at $66.00/month in its cheapest regions
(`iad`, `ewr`), and says prices vary by region; `fly platform vm-sizes` lists
sizes but no prices. At that rate a `performance-8x` is about $264/month,
**about $0.36 an hour**, a little more in `sjc`. The volume is $0.15/GB/month
(30 GB: under a cent an hour). A quick run is well under $1; a full run a
dollar or two.

The machine bounds its own life, so a run nobody watches still ends:

* `machine/entry.sh` runs everything under `timeout $MAX_LIFETIME_S`
  (3 hours), then exits; a watchdog powers the VM off five minutes after that
  in case the timeout fails.
* When the main process exits the machine stops; with auto_destroy and
  restart policy `no` it is then destroyed, never restarted (a stopped
  machine would still bill its root filesystem).
* What is left after that is the app and the volume (cents a day):
  `fly/teardown.sh` removes them, and can be run at any time.

Check that nothing is left:

    fly apps list --org datasette-cloud-dev | grep dblayer-bench   # no output
    fly volumes list -a dblayer-bench                              # error: app not found

## Notes

* The benchmarks run as an unprivileged user `bench` (some tests check file
  permissions, which root ignores). `TMPDIR`, the uv and npm caches, the
  clone and PostgreSQL's data directory are all on the volume (ext4 on a
  local NVMe-backed block device), not the root filesystem.
* `DBLAYER_TEST_POSTGRES=postgresql:///dblayer_test?host=/tmp` inside the
  machine: the server is in the same machine, so the driver comparison does
  not measure a network hop.
* The first trial's volume landed on a host with no room for a
  `performance-8x` ("insufficient resources ... with existing volume");
  `create.sh` now passes `--vm-size` so the volume is placed where the
  machine fits. If it still happens, `fly/teardown.sh` and try again, or pick
  another region with `FLY_REGION=...`.
