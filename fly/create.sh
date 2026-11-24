#!/bin/bash
# Create the app and its volume (idempotent: reuses them if they exist).
# No IPs, certificates or services are allocated: the machine has no ports.
set -euo pipefail
source "$(dirname "$0")/config.sh"

if fly apps list --org "$FLY_ORG" --json | jq -e --arg a "$FLY_APP" '.[] | select(.Name == $a)' >/dev/null; then
    log "app $FLY_APP exists in $FLY_ORG"
else
    log "creating app $FLY_APP in $FLY_ORG"
    fly apps create --name "$FLY_APP" --org "$FLY_ORG"
fi

if fly volumes list -a "$FLY_APP" --json | jq -e --arg v "$FLY_VOLUME" '.[] | select(.name == $v)' >/dev/null; then
    log "volume $FLY_VOLUME exists"
else
    log "creating volume $FLY_VOLUME (${FLY_VOLUME_GB} GB) in $FLY_REGION"
    fly volumes create "$FLY_VOLUME" -a "$FLY_APP" --region "$FLY_REGION" --size "$FLY_VOLUME_GB" \
        --require-unique-zone=false --yes
fi
