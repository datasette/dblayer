//! `dblayer-duckdb-rsworker [--memory-limit-mb N] [--debug] [--version]`
//!
//! An experimental DuckDB worker process for dblayer, speaking the protocol
//! in `docs/worker-protocol.md` (requests on stdin, replies on stdout) as
//! the Python worker does with the DuckDB engine (`DuckDBWorker`). The
//! parent side is `dblayer.backends.duckdb_rsworker.DuckDBRustWorker`.

mod convert;
mod errors;
mod ffi;
mod json;
mod msgpack;
mod schema;
mod sql;
mod timer;
mod types;
mod value;
mod worker;

fn vm_size_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmSize:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

fn main() {
    let mut memory_limit_mb: Option<u64> = None;
    let mut debug = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--memory-limit-mb" => memory_limit_mb = args.next().and_then(|v| v.parse().ok()),
            "--debug" => debug = true,
            "--preload" => {
                args.next(); // a Python-worker option; nothing to preload here
            }
            "--version" => {
                println!(
                    "dblayer-duckdb-rsworker protocol {}, DuckDB {}",
                    worker::PROTOCOL_VERSION,
                    ffi::library_version()
                );
                return;
            }
            other => {
                eprintln!("dblayer-duckdb-rsworker: unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }
    // Replies go to a private copy of stdout; fd 1 then points at stderr.
    let Some(out_fd) = ffi::private_stdout() else {
        eprintln!("dblayer-duckdb-rsworker: cannot set up stdout");
        std::process::exit(1);
    };
    let out = ffi::file_from_fd(out_fd);
    let mut rlimit_as = false;
    let server_out = out;
    // The timer thread starts in Server::new, before RLIMIT_AS, so its stack
    // is already mapped (as the Python worker starts its executor first).
    let mut server = worker::Server::new(server_out, debug, false);
    if let Some(mb) = memory_limit_mb.filter(|m| *m > 0)
        && let Some(vm) = vm_size_bytes()
    {
        rlimit_as = ffi::set_rlimit_as(vm + mb * 1024 * 1024);
    }
    server.set_rlimit_as(rlimit_as);
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    server.serve(&mut input);
}
