#!/bin/bash
# Build fly/Dockerfile on Fly's Depot builder (no builder app is created in
# the org, and no local Docker is needed) and push it to the app's registry
# as registry.fly.io/$FLY_APP:$IMAGE_LABEL. Needs the app (fly/create.sh).
set -euo pipefail
source "$(dirname "$0")/config.sh"
cd "$FLY_DIR"
log "building $IMAGE"
fly deploy --build-only --push --depot=true --depot-scope=app \
    --app "$FLY_APP" --config fly.toml --image-label "$IMAGE_LABEL"
log "pushed $IMAGE"
