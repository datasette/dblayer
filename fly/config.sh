# Settings shared by the fly/*.sh scripts. Source it; override any value
# from the environment. Nothing here is secret: flyctl's own login (in
# ~/.fly, never in this repository) is what authorises the commands.

FLY_ORG="${FLY_ORG:-datasette-cloud-dev}"
FLY_APP="${FLY_APP:-dblayer-bench}"
FLY_REGION="${FLY_REGION:-sjc}"
# 8 dedicated CPUs, 16 GB: dedicated, so the figures are not shared-host noise.
FLY_VM_SIZE="${FLY_VM_SIZE:-performance-8x}"
FLY_VOLUME="${FLY_VOLUME:-bench_data}"
# Quick runs peaked at 2.5 GB (clone and Rust targets 1.4 GB, uv/npm caches
# 1 GB); full runs add 10M-row ClickHouse/DuckDB/SQLite files and 1M-row
# export tables. 15 GB leaves room; it costs about $0.003 an hour.
FLY_VOLUME_GB="${FLY_VOLUME_GB:-15}"
FLY_MACHINE_NAME="${FLY_MACHINE_NAME:-bench}"
# The machine shuts itself down after this many seconds whatever happens,
# and is then destroyed (auto_destroy, restart policy "no").
MAX_LIFETIME_S="${MAX_LIFETIME_S:-10800}"
# After the run the machine waits up to this long for fly/fetch.sh to copy
# the results off (fetch.sh releases it early), then exits.
HOLD_S="${HOLD_S:-1800}"
REPO_URL="${REPO_URL:-https://github.com/datasette/dblayer.git}"
FLY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$FLY_DIR/.." && pwd)"
# The image tag is a hash of what goes into the image, so a changed script
# always gets a new tag (re-pushing one tag was not picked up reliably).
IMAGE_LABEL="${IMAGE_LABEL:-bench-$(cat "$FLY_DIR/Dockerfile" "$FLY_DIR"/machine/* | (sha256sum 2>/dev/null || shasum -a 256) | cut -c1-12)}"
IMAGE="registry.fly.io/${FLY_APP}:${IMAGE_LABEL}"

log() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }
