#!/bin/bash
# Destroy everything this folder creates: the machine(s), the volume(s) and
# the app (with its registry images). Idempotent: safe to run at any time,
# any number of times; it only ever touches $FLY_APP in $FLY_ORG.
set -uo pipefail
source "$(dirname "$0")/config.sh"

if ! fly apps list --org "$FLY_ORG" --json | jq -e --arg a "$FLY_APP" '.[] | select(.Name == $a)' >/dev/null; then
    log "app $FLY_APP does not exist in $FLY_ORG: nothing to tear down"
else
    for m in $(fly machines list -a "$FLY_APP" --json | jq -r '.[].id'); do
        log "destroying machine $m"; fly machine destroy "$m" -a "$FLY_APP" --force
    done
    for v in $(fly volumes list -a "$FLY_APP" --json | jq -r '.[].id'); do
        log "destroying volume $v"; fly volumes destroy "$v" -a "$FLY_APP" --yes
    done
    log "destroying app $FLY_APP"
    fly apps destroy "$FLY_APP" --yes
fi

log "verifying"
fly apps list --org "$FLY_ORG" | grep -w "$FLY_APP" && { log "app $FLY_APP still listed"; exit 1; }
log "app $FLY_APP is not in $FLY_ORG's app list: nothing left"
