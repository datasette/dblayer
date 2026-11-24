#!/usr/bin/env bash
# CI diagnostics. `ci-diag.sh snapshot` prints CPUs, memory, load and the
# worker processes alive now. `ci-diag.sh monitor [seconds]` prints a
# one-line summary every interval until killed (start it in the background
# of the test step, so the live log shows memory before a runner dies).
set -u

count() { pgrep -f "$1" 2>/dev/null | wc -l | tr -d ' '; }

workers() {
    printf 'python=%s node=%s cworker=%s rsworker=%s duckdb-cpp=%s duckdb-rs=%s pglite=%s' \
        "$(count '[p]ython')" "$(count '[n]ode ')" "$(count '[d]blayer-cworker')" \
        "$(count '[d]blayer-sqlite-rs')" "$(count '[d]blayer-duckdb-worker')" \
        "$(count '[d]blayer-duckdb-rsworker')" "$(count '[p]glite-worker')"
}

mem() {
    if command -v free >/dev/null; then
        free -m | awk '/^Mem:/ {printf "mem used=%sMB avail=%sMB", $3, $7} /^Swap:/ {printf " swap used=%sMB", $3}'
    else
        vm_stat | awk '/Pages free/ {gsub("\\.","",$3); printf "mem free=%dMB", $3*16384/1048576}'
    fi
}

case "${1:-snapshot}" in
snapshot)
    echo "== ci-diag snapshot $(date -u +%H:%M:%S) =="
    echo "cpus: $(getconf _NPROCESSORS_ONLN)  load: $(uptime | sed 's/.*load averages*: //')"
    if command -v free >/dev/null; then free -m; else vm_stat | head -5; fi
    echo "disk: $(df -h . | awk 'NR==2 {print $4 " free of " $2}')  tmp: $(du -sh "${TMPDIR:-/tmp}" 2>/dev/null | cut -f1)"
    echo "workers: $(workers)"
    echo "processes: $(ps -A | wc -l | tr -d ' ')"
    echo "top 15 by RSS:"
    ps -A -o pid=,ppid=,rss=,etime=,command= | sort -k3 -n -r | head -15 | cut -c1-200
    ;;
monitor)
    interval="${2:-60}"
    while sleep "$interval"; do
        echo "[ci-diag $(date -u +%H:%M:%S)] load $(uptime | sed 's/.*load averages*: //') | $(mem) | disk free $(df -h . | awk 'NR==2 {print $4}') | $(workers)"
    done
    ;;
esac
