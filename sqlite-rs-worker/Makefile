# dblayer SQLite worker in Rust: a drop-in replacement for
# `cworker/build/dblayer-cworker` (single-tenant and --multi), speaking
# docs/worker-protocol.md.
#
#   make -C sqlite-rs-worker            # target/release/dblayer-sqlite-rsworker
#   make -C sqlite-rs-worker debug      # target/debug/dblayer-sqlite-rsworker
#   make -C sqlite-rs-worker test       # cargo test (unit tests and the fuzz)
#
# SQLite is the C worker's pinned, checksummed 3.53.1 amalgamation
# (downloaded once into cworker/vendor/ by cworker/Makefile), compiled by
# build.rs with the C worker's own options (SQLITE_DEFS, read from
# cworker/Makefile, and -O2 -g). Python finds the binary through an explicit
# path, then $DBLAYER_RSWORKER, then target/release/.

CARGO ?= cargo
AMALGAMATION := ../cworker/vendor/sqlite-amalgamation-3530100/sqlite3.c

release: $(AMALGAMATION)
	$(CARGO) build --release --locked

debug: $(AMALGAMATION)
	$(CARGO) build --locked

test: $(AMALGAMATION)
	$(CARGO) test --release --locked

clippy: $(AMALGAMATION)
	$(CARGO) clippy --release --locked --all-targets -- -D warnings

# Nightly-only checks (rustup's nightly toolchain; not run in CI). TSan and
# ASan instrument SQLite too (build.rs); the TSan binary is for the Python
# tests: DBLAYER_RSWORKER=$(TSAN_BIN) TSAN_OPTIONS="halt_on_error=1
# suppressions=$(CURDIR)/../cworker/tsan.supp" uv run pytest -k rspool ...
TARGET ?= $(shell rustc -vV | sed -n 's/^host: //p')
TSAN_BIN := target/tsan/$(TARGET)/release/dblayer-sqlite-rsworker

tsan: $(AMALGAMATION)
	RUSTFLAGS="-Zsanitizer=thread" $(CARGO) +nightly test -Zbuild-std --target $(TARGET) --target-dir target/tsan
	RUSTFLAGS="-Zsanitizer=thread" $(CARGO) +nightly build --release -Zbuild-std=std,panic_abort --target $(TARGET) --target-dir target/tsan

asan: $(AMALGAMATION)
	RUSTFLAGS="-Zsanitizer=address" $(CARGO) +nightly test -Zbuild-std --target $(TARGET) --target-dir target/asan
	RUSTFLAGS="-Zsanitizer=address" $(CARGO) +nightly build --release -Zbuild-std=std,panic_abort --target $(TARGET) --target-dir target/asan

# Miri runs every unit test (all of them are pure Rust: codecs, framing,
# SQL text helpers, error names, float formatting) at a reduced fuzz size.
miri: $(AMALGAMATION)
	DBLAYER_RS_FUZZ_ITERS=300 DBLAYER_RS_FUZZ_SCALE=50 \
	MIRIFLAGS="-Zmiri-env-forward=DBLAYER_RS_FUZZ_ITERS -Zmiri-env-forward=DBLAYER_RS_FUZZ_SCALE -Zmiri-disable-isolation" \
	$(CARGO) +nightly miri test

$(AMALGAMATION):
	$(MAKE) -C ../cworker vendor/sqlite-amalgamation-3530100/sqlite3.c

clean:
	$(CARGO) clean

.PHONY: release debug test clean clippy tsan asan miri
