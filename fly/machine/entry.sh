#!/bin/bash
# The machine's main process (runs as root). When it exits the machine
# stops, and because it was created with auto_destroy (--rm) and restart
# policy "no", Fly then destroys it rather than restarting it.
#
# Two independent limits make sure it cannot run up a bill:
#   1. `timeout` ends the run after MAX_LIFETIME_S, then this script exits;
#   2. a watchdog powers the VM off (sysrq) and kills this script 5 minutes
#      after that, in case the first limit fails.
set -u
MAX="${MAX_LIFETIME_S:-10800}"
echo "dblayer-bench: machine ${FLY_MACHINE_ID:-?} (${FLY_VM_SIZE:-?}) in ${FLY_REGION:-?}," \
     "max lifetime ${MAX}s, started $(date -u +%FT%TZ)"

(
    sleep $((MAX + 300))
    echo "dblayer-bench: WATCHDOG: max lifetime exceeded, powering off"
    sync
    echo 1 > /proc/sys/kernel/sysrq 2>/dev/null
    echo o > /proc/sysrq-trigger 2>/dev/null
    kill -9 $$
) &

mkdir -p /data
chown bench:bench /data
timeout -k 60 "$MAX" runuser -u bench -- /fly/machine/run.sh
rc=$?
[ "$rc" = 124 ] && echo "dblayer-bench: max lifetime reached, run stopped"
echo "dblayer-bench: run ended with status $rc at $(date -u +%FT%TZ); exiting, the machine is destroyed"
exit 0
