//! Embed an rpath to the libduckdb directory, so the binary finds the library
//! at run time without DYLD_LIBRARY_PATH / LD_LIBRARY_PATH: the absolute
//! directory it was linked against, then the same place relative to the
//! binary (target/<profile>/ -> ../../vendor/libduckdb) in case the checkout
//! moved.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");
    let Ok(dir) = env::var("DUCKDB_LIB_DIR") else {
        println!("cargo:warning=DUCKDB_LIB_DIR is not set: run ./fetch-libduckdb.sh, then build from this directory");
        return;
    };
    let dir = PathBuf::from(dir);
    let abs = dir.canonicalize().unwrap_or(dir);
    println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{}", abs.display());
    let origin = if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        "@executable_path"
    } else {
        "$ORIGIN"
    };
    println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{origin}/../../vendor/libduckdb");
}
