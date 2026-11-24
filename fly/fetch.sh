#!/bin/bash
# Wait for the run to finish, copy the results off the machine and release
# it (it then exits and is destroyed):
#
#   fly/fetch.sh [DEST]     # default DEST: benchmarks/results/
#
# Copies /data/out/results.tar.gz with `fly ssh sftp get`. If that fails, it
# falls back to the base64 copy of the tarball the machine printed to its
# log between "BEGIN/END DBLAYER RESULTS TARBALL BASE64" lines.
set -uo pipefail
source "$(dirname "$0")/config.sh"
DEST="${1:-$REPO_DIR/benchmarks/results}"
POLL_S="${POLL_S:-60}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

remote() { fly ssh console -a "$FLY_APP" --quiet -C "$*" 2>/dev/null | tr -d '\r'; }

last=""
while :; do
    if [ "$(fly machines list -a "$FLY_APP" --json 2>/dev/null | jq length)" = 0 ]; then
        log "no machine left in $FLY_APP: trying the log copy"; break
    fi
    s="$(remote cat /data/out/STATUS)"
    [ "$s" != "$last" ] && log "machine: ${s:-(no status yet)}" && last="$s"
    case "$s" in READY*) break ;; esac
    sleep "$POLL_S"
done

got=""
if [[ "$last" == READY* ]]; then
    if (cd "$WORK" && fly ssh sftp get -a "$FLY_APP" /data/out/results.tar.gz results.tar.gz >/dev/null); then
        got="$WORK/results.tar.gz"; log "copied the tarball with sftp"
    fi
fi
if [ -z "$got" ]; then
    fly logs -a "$FLY_APP" --no-tail 2>/dev/null \
        | sed -n '/BEGIN DBLAYER RESULTS TARBALL BASE64/,/END DBLAYER RESULTS TARBALL BASE64/p' \
        | grep -v 'DBLAYER RESULTS' | sed -E 's/^.*\] *//; s/^.* (app|stdout)\[[^]]*\] *//' \
        | awk '{print $NF}' | base64 -d > "$WORK/results.tar.gz" 2>/dev/null \
        && tar -tzf "$WORK/results.tar.gz" >/dev/null 2>&1 && got="$WORK/results.tar.gz" && log "decoded the tarball from the log"
fi
if [ -z "$got" ]; then log "could not fetch the results"; exit 1; fi

mkdir -p "$WORK/x"
tar -xzf "$got" -C "$WORK/x"
if ! ls -d "$WORK"/x/results/*/ >/dev/null 2>&1; then
    # A tests-only run has no benchmark results: keep its logs.
    mkdir -p "$WORK/x/results/$(date -u +%Y-%m-%dT%H%MZ)-tests-only"
fi
for d in "$WORK"/x/results/*/; do
    name="$(basename "$d")"
    mkdir -p "$DEST/$name"
    cp -R "$d". "$DEST/$name/"
    cp "$WORK"/x/stages.jsonl "$DEST/$name/stages.jsonl" 2>/dev/null
    mkdir -p "$DEST/$name/machine-logs"
    cp "$WORK"/x/logs/*.log "$DEST/$name/machine-logs/" 2>/dev/null
    cp "$WORK"/x/hang-dump.txt "$DEST/$name/machine-logs/" 2>/dev/null
    log "results in $DEST/$name"
done
remote touch /data/out/RELEASE >/dev/null && log "released the machine (it exits and is destroyed)"
