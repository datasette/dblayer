//! `dblayer-sqlite-rsworker [--memory-limit-mb N] [--debug]`
//! `dblayer-sqlite-rsworker --multi [--max-connections N] [--heap-limit-mb N]
//!                          [--soft-heap-limit-mb N] [--cache-kb N] [--lookaside SIZE,COUNT]`
//!
//! A drop-in replacement for the C worker (`cworker/build/dblayer-cworker`),
//! in Rust: SQLite (the same pinned amalgamation, the same compile options)
//! behind docs/worker-protocol.md on stdin/stdout. Used by
//! `dblayer.backends.sqlite_rspool` (`--multi`).

// `unsafe` is confined to the FFI layer (ffi.rs); everything else is safe Rust.
#![deny(unsafe_code)]

mod codec;
mod errors;
mod export;
#[allow(unsafe_code)]
mod ffi;
#[cfg(test)]
mod fuzz;
mod schema;
mod sqlutil;
mod worker;

use std::sync::Arc;

const USAGE: &str = "usage: dblayer-sqlite-rsworker [--memory-limit-mb N] [--debug] [--multi [--max-connections N] \
[--heap-limit-mb N] [--soft-heap-limit-mb N] [--cache-kb N] [--lookaside SIZE,COUNT]]";

fn arg_i64(args: &mut impl Iterator<Item = String>, name: &str) -> i64 {
    let Some(v) = args.next() else {
        eprintln!("dblayer-sqlite-rsworker: {name} needs a value");
        std::process::exit(2);
    };
    // strtol-like: leading digits, else 0
    let t = v.trim();
    let end = t.char_indices().find(|&(i, c)| !(c.is_ascii_digit() || (i == 0 && c == '-'))).map(|(i, _)| i).unwrap_or(t.len());
    t[..end].parse().unwrap_or(0)
}

fn main() {
    let mut o = worker::Opts { max_conns: 256, ..Default::default() };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--debug" => o.debug = true,
            "--memory-limit-mb" => o.memory_limit_mb = arg_i64(&mut args, &a),
            "--multi" => o.multi = true,
            "--max-connections" => o.max_conns = arg_i64(&mut args, &a).max(1) as usize,
            "--heap-limit-mb" => o.heap_limit_mb = arg_i64(&mut args, &a),
            "--soft-heap-limit-mb" => o.soft_heap_limit_mb = arg_i64(&mut args, &a),
            "--cache-kb" => o.cache_kb = arg_i64(&mut args, &a),
            "--lookaside" => {
                // SIZE,COUNT for each connection's lookaside slab; 0,0 turns it off
                let parsed = args.next().and_then(|v| {
                    let (a, b) = v.split_once(',')?;
                    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
                });
                match parsed {
                    Some(p) => o.lookaside = Some(p),
                    None => {
                        eprintln!("dblayer-sqlite-rsworker: --lookaside needs SIZE,COUNT");
                        std::process::exit(2);
                    }
                }
            }
            "--version" => {
                println!(
                    "dblayer-sqlite-rsworker protocol {}, SQLite {} ({})",
                    worker::PROTOCOL_VERSION,
                    ffi::libversion(),
                    env!("DBL_SQLITE_CFLAGS")
                );
                return;
            }
            _ => {
                eprintln!("{USAGE}");
                std::process::exit(2);
            }
        }
    }
    worker::now_ns(); // the monotonic epoch
    ffi::ignore_signals();
    // Anything printed (an extension, say) goes to stderr, never into the
    // protocol stream.
    let Some(out) = ffi::private_stdout() else {
        eprintln!("dblayer-sqlite-rsworker: cannot set up stdout");
        std::process::exit(1);
    };
    // Multi-thread mode: each connection is used by one thread only (its
    // lane; `Db` is not `Send`), so SQLite's per-connection mutexes are off.
    // Memory statistics cost a global mutex on every allocation, so in
    // multi-tenant mode they are on only when a heap limit needs them.
    let memstatus = !o.multi || o.heap_limit_mb > 0 || o.soft_heap_limit_mb > 0;
    if !ffi::configure(o.multi, memstatus) {
        eprintln!("dblayer-sqlite-rsworker: this SQLite cannot run multi-threaded");
        std::process::exit(1);
    }
    // Process-wide: every connection in this process shares them.
    if o.heap_limit_mb > 0 {
        ffi::hard_heap_limit(o.heap_limit_mb * 1024 * 1024);
    }
    if o.soft_heap_limit_mb > 0 {
        ffi::soft_heap_limit(o.soft_heap_limit_mb * 1024 * 1024);
    }
    let memory_limit_mb = o.memory_limit_mb;
    let sh = worker::Shared::new(o, out);
    let (w, r) = (sh.clone(), sh.clone());
    let spawned = std::thread::Builder::new().name("watchdog".into()).spawn(move || worker::watchdog_main(w)).and_then(|_| {
        std::thread::Builder::new().name("reader".into()).spawn(move || {
            let stdin = std::io::stdin();
            worker::reader_main(r, stdin.lock())
        })
    });
    if spawned.is_err() {
        eprintln!("dblayer-sqlite-rsworker: cannot start threads");
        std::process::exit(1);
    }
    // After the threads exist, so their stacks are already mapped.
    if memory_limit_mb > 0 {
        sh.set_rlimit_as(ffi::set_rlimit_as(memory_limit_mb));
    }
    sh.start();
    worker::main_lane(Arc::clone(&sh));
}
