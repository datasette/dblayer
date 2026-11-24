#!/bin/bash
# Start the benchmark machine:
#
#   fly/launch.sh [REF] [quick|full] [tests]
#
# REF: git ref of the public repository to clone (default main). "tests"
# also runs the test suite after the benchmarks (hang hunting).
#
# The machine is created with --rm (auto_destroy: destroyed when it stops)
# and --restart no (never restarted, so a stopped machine costs nothing),
# and bounds its own life to MAX_LIFETIME_S (fly/machine/entry.sh).
set -euo pipefail
source "$(dirname "$0")/config.sh"
REF="${1:-main}"
MODE="${2:-quick}"
TESTS=0; [ "${3:-}" = tests ] && TESTS=1
LABEL="${BENCH_LABEL:-}"

if [ "$(fly machines list -a "$FLY_APP" --json | jq length)" != 0 ]; then
    log "a machine already exists in $FLY_APP: run fly/teardown.sh or wait for it"; exit 1
fi
log "starting $FLY_VM_SIZE in $FLY_REGION: ref $REF, mode $MODE, tests $TESTS, max lifetime ${MAX_LIFETIME_S}s"
fly machine run "$IMAGE" \
    --app "$FLY_APP" --region "$FLY_REGION" --name "$FLY_MACHINE_NAME" \
    --vm-size "$FLY_VM_SIZE" \
    --volume "$FLY_VOLUME:/data" \
    --restart no --rm \
    --env BENCH_REF="$REF" --env BENCH_MODE="$MODE" --env BENCH_RUN_TESTS="$TESTS" \
    --env BENCH_LABEL="$LABEL" --env MAX_LIFETIME_S="$MAX_LIFETIME_S" --env HOLD_S="$HOLD_S" \
    --env REPO_URL="$REPO_URL" --env FLY_VM_SIZE="$FLY_VM_SIZE" \
    --detach
fly machines list -a "$FLY_APP"
