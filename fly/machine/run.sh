#!/bin/bash
# Runs as the unprivileged user "bench" inside the machine (fly/machine/entry.sh).
#
#   1. starts PostgreSQL (data directory on the volume, Unix socket in /tmp,
#      trust auth for local connections only, no TCP) with a database
#      dblayer_test;
#   2. clones the public repository at $BENCH_REF onto the volume;
#   3. builds every backend, each step with its own timeout; a failed build
#      is recorded and the run carries on (the runner then records the
#      backend as skipped, with the reason);
#   4. runs benchmarks/run_all.py ($BENCH_MODE: quick or full);
#   5. optionally ($BENCH_RUN_TESTS=1) runs the test suite with per-test
#      timeouts and stack dumps, to catch a hang;
#   6. packs everything into /data/out/results.tar.gz, prints results.md and
#      the tarball (base64) to the log between marker lines, then waits up
#      to $HOLD_S seconds for fly/fetch.sh to copy it and touch
#      /data/out/RELEASE.
#
# /data/out/STATUS says what it is doing; /data/out/stages.jsonl how long
# each stage took and how it ended.
set -u
REF="${BENCH_REF:-main}"
MODE="${BENCH_MODE:-quick}"
RUN_TESTS="${BENCH_RUN_TESTS:-0}"
HOLD="${HOLD_S:-1800}"
MAX="${MAX_LIFETIME_S:-10800}"
LABEL="${BENCH_LABEL:-}"
REPO_URL="${REPO_URL:-https://github.com/datasette/dblayer.git}"
PY="${BENCH_PYTHON:-3.14}"
STARTED=$(date +%s)

OUT=/data/out
SRC=/data/src
rm -rf "$OUT" /data/tmp
mkdir -p "$OUT/logs" /data/tmp /data/cache
export TMPDIR=/data/tmp
export UV_CACHE_DIR=/data/cache/uv
export npm_config_cache=/data/cache/npm
export DBLAYER_BENCH_CACHE=/data/cache/export
export DBLAYER_TEST_POSTGRES="postgresql:///dblayer_test?host=/tmp"
export BENCH_VM_SIZE="${FLY_VM_SIZE:-}"

status() { echo "$*" > "$OUT/STATUS"; echo "##### $(date -u +%H:%M:%SZ) $*"; }

# stage NAME TIMEOUT_S COMMAND...: run with a timeout, log to logs/NAME.log,
# record the outcome in stages.jsonl, print the log's tail on failure.
stage() {
    local name=$1 limit=$2; shift 2
    status "running $name"
    local t0=$(date +%s)
    timeout -k 30 "$limit" bash -c '"$@"' _ "$@" > "$OUT/logs/$name.log" 2>&1
    local rc=$?
    local t1=$(date +%s)
    local outcome=ok
    [ $rc != 0 ] && outcome=failed
    [ $rc = 124 ] && outcome=timeout
    printf '{"stage": "%s", "status": "%s", "returncode": %d, "seconds": %d, "load1_after": %s}\n' \
        "$name" "$outcome" "$rc" $((t1 - t0)) "$(cut -d' ' -f1 /proc/loadavg)" >> "$OUT/stages.jsonl"
    echo "##### $name: $outcome in $((t1 - t0)) s"
    if [ $outcome != ok ]; then tail -n 25 "$OUT/logs/$name.log" | sed "s/^/  [$name] /"; fi
    return $rc
}

start_postgres() {
    rm -rf /data/pg
    initdb -D /data/pg -A trust -U bench --encoding UTF8 --locale C.UTF-8 >/dev/null &&
    pg_ctl -D /data/pg -l "$OUT/logs/postgres-server.log" -w \
        -o "-c listen_addresses='' -c unix_socket_directories=/tmp -c max_connections=300" start &&
    createdb -h /tmp dblayer_test &&
    psql -h /tmp -d dblayer_test -Atc "select version()"
}

clone() {
    rm -rf "$SRC"
    git clone --quiet "$REPO_URL" "$SRC" && git -C "$SRC" checkout --quiet "$REF" &&
    git -C "$SRC" log -1 --format='%H %s'
}

machine_info() {
    nproc; grep -m1 'model name' /proc/cpuinfo; grep MemTotal /proc/meminfo; uname -a
    findmnt -T /data; df -h /data /; cat /proc/loadavg
    node --version; rustc --version; cargo clippy --version; uv --version; uv python list --only-installed
    postgres --version; cc --version | head -1
}

export -f start_postgres clone machine_info
status "starting: ref $REF, mode $MODE, tests $RUN_TESTS"
stage machine-info 60 machine_info; cat "$OUT/logs/machine-info.log"
stage postgres 120 start_postgres
stage clone 300 clone || { status "clone failed"; }
cd "$SRC" 2>/dev/null || cd /data

stage build-cworker 900 make -C cworker
stage build-wasm 900 make -C cworker wasm
stage build-sqlite-rs-worker 1200 make -C sqlite-rs-worker
stage build-duckdb-rs-worker 1200 make -C duckdb-rs-worker
stage build-duckdb-cpp-worker 900 make -C duckdb-cpp-worker
stage build-nodeworker 600 npm ci --prefix nodeworker
stage build-pglite 600 npm ci --prefix pglite-worker
stage python-env 900 uv sync --python "$PY" --all-extras --group demo

mode_flag=""; [ "$MODE" = quick ] && mode_flag="--quick"
status "running benchmarks"
t0=$(date +%s)
timeout -k 60 $((MAX - HOLD - 1200)) \
    uv run --python "$PY" --all-extras --group demo python benchmarks/run_all.py $mode_flag \
        --out "$OUT/results" --label "$LABEL" 2>&1 | tee "$OUT/logs/run_all.log" | grep -E '^(===|RESULTS_DIR)'
printf '{"stage": "benchmarks", "status": "%s", "seconds": %d}\n' "${PIPESTATUS[0]}" $(( $(date +%s) - t0 )) >> "$OUT/stages.jsonl"

if [ "$RUN_TESTS" = 1 ]; then
    # What is left of the lifetime, keeping the hold and a margin: never past it.
    left=$(( MAX - HOLD - 600 - ($(date +%s) - STARTED) ))
    [ $left -gt 2400 ] && left=2400
    if [ $left -gt 300 ]; then
        export DBLAYER_HANG_DUMP_S=240 DBLAYER_HANG_DUMP_FILE="$OUT/hang-dump.txt"
        stage tests "$left" uv run --python "$PY" --all-extras --group demo pytest -v -p no:cacheprovider \
            -o faulthandler_timeout=240 --timeout=300 --timeout-method=thread --durations=30 -ra
        grep -E "Timeout|FAILED|ERROR|passed|failed" "$OUT/logs/tests.log" | tail -n 40
    else
        echo "##### tests: skipped, only ${left}s of lifetime left"
    fi
fi

status "packing results"
pg_ctl -D /data/pg -m fast stop >/dev/null 2>&1
results_dir=$(ls -d "$OUT"/results/*/ 2>/dev/null | head -1)
tar -czf /data/results.tar.gz -C "$OUT" . && mv /data/results.tar.gz "$OUT/results.tar.gz"
echo "===== BEGIN DBLAYER RESULTS MARKDOWN ====="
[ -n "$results_dir" ] && cat "$results_dir/results.md"
echo "===== END DBLAYER RESULTS MARKDOWN ====="
size=$(stat -c %s "$OUT/results.tar.gz")
if [ "$size" -lt 3000000 ]; then
    echo "===== BEGIN DBLAYER RESULTS TARBALL BASE64 ($size bytes) ====="
    base64 -w 120 "$OUT/results.tar.gz"
    echo "===== END DBLAYER RESULTS TARBALL BASE64 ====="
fi
status "READY $(basename "${results_dir:-none}")"

waited=0
while [ ! -e "$OUT/RELEASE" ] && [ $waited -lt "$HOLD" ]; do sleep 5; waited=$((waited + 5)); done
echo "##### released after ${waited}s (RELEASE file: $([ -e "$OUT/RELEASE" ] && echo yes || echo no))"
