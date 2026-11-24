// dblayer DuckDB worker: one process owning DuckDB databases on behalf of
// a parent (dblayer.backends.duckdb_cppworker), speaking the protocol in
// docs/worker-protocol.md over stdin (requests) and stdout (replies).
//
// Threads
// -------
// * The main thread reads and decodes frames, answers hello, routes
//   requests to connections and acts on control messages (cancel, more)
//   at once.
// * Each protocol connection (a duckdb_connection) has a thread of its
//   own with a FIFO queue, so requests on different connections run
//   concurrently: a long analytical query does not hold up a point read
//   on another connection of the same database. Requests on one
//   connection run one at a time, in arrival order.
// * One timer thread enforces time limits and transaction deadlines by
//   calling duckdb_interrupt() again every 5 ms until the statement
//   returns (DuckDB drops an interrupt that arrives before a statement has
//   really started, or while the connection is idle).
//
// Every reply frame is written whole under one output lock.
#include "worker.hpp"

#include <chrono>
#include <cinttypes>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dbw {
void set_output_fd(int fd);
void run_server(int in_fd, bool debug, long memory_limit_mb);
}  // namespace dbw

using namespace dbw;

static const char *VERSION_TEXT = "dblayer-duckdb-worker protocol 1, DuckDB ";

static void usage() {
    fprintf(stderr, "usage: dblayer-duckdb-worker [--memory-limit-mb N] [--debug] [--version]\n");
    exit(2);
}

// RLIMIT_AS on Linux: what is mapped now plus the limit (as the Python
// worker does it), before any database is opened.
static bool set_rlimit_as(long mb) {
#ifdef __linux__
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return false;
    char line[256];
    long vm_kb = -1;
    while (fgets(line, sizeof line, f)) {
        if (strncmp(line, "VmSize:", 7) == 0) {
            vm_kb = strtol(line + 7, nullptr, 10);
            break;
        }
    }
    fclose(f);
    if (vm_kb < 0) return false;
    struct rlimit rl;
    if (getrlimit(RLIMIT_AS, &rl) != 0) return false;
    rlim_t limit = (rlim_t)vm_kb * 1024 + (rlim_t)mb * 1024 * 1024;
    if (rl.rlim_max != RLIM_INFINITY && limit > rl.rlim_max) limit = rl.rlim_max;
    rl.rlim_cur = limit;
    return setrlimit(RLIMIT_AS, &rl) == 0;
#else
    (void)mb;
    return false;
#endif
}

namespace dbw {
bool g_rlimit_as = false;
}

int main(int argc, char **argv) {
    bool debug = false;
    long memory_limit_mb = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--debug") == 0) {
            debug = true;
        } else if (strcmp(argv[i], "--memory-limit-mb") == 0 && i + 1 < argc) {
            memory_limit_mb = strtol(argv[++i], nullptr, 10);
        } else if (strcmp(argv[i], "--version") == 0) {
            printf("%s%s\n", VERSION_TEXT, duckdb_library_version());
            return 0;
        } else {
            usage();
        }
    }
    // The parent's Ctrl-C is not ours: we exit when stdin closes.
    signal(SIGINT, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    // Replies go to a private copy of stdout; anything else that writes
    // to fd 1 (DuckDB, a library) lands on stderr instead.
    int out_fd = dup(1);
    if (out_fd < 0 || dup2(2, 1) < 0) {
        perror("dblayer-duckdb-worker: dup");
        return 1;
    }
    fcntl(out_fd, F_SETFD, FD_CLOEXEC);
    set_output_fd(out_fd);
    if (memory_limit_mb > 0)
        g_rlimit_as = set_rlimit_as(memory_limit_mb);
    run_server(0, debug, memory_limit_mb);
    return 0;
}
