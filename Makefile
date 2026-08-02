# Makefile for threefoldtech/tfstor project

.PHONY: all build test test-respcas-integration test-s3cas-integration clippy clean fmt run-respcas run-s3cas realtest realtest-tb realtest-smoke realtest-selftest

# Default target
all: build test

# Build the project
build:
	cargo build --workspace

# Build with release profile
release:
	cargo build --workspace --release

# Run tests
test:
	cargo test --workspace

# Run clippy lints with warnings as errors
clippy:
	cargo clippy --workspace --all-features -- -Dwarnings

# Run clippy without treating warnings as errors
clippy-check:
	cargo clippy --workspace --all-features

# Format code
fmt:
	cargo fmt --all

# Check formatting without modifying files
fmt-check:
	cargo fmt --all -- --check

# Run respcas integration tests
test-respcas-integration:
	cargo test --test integration_test -p respcas -- --test-threads=1

# Run s3cas integration tests
test-s3cas-integration:
	cargo test --test it_s3 -p s3cas -- --test-threads=1

# Clean build artifacts
clean:
	cargo clean

# The real-hardware validation campaign (ADR 0009). Consumes the designated
# disk, kills daemons on purpose, takes hours. See docs/realtest.md.
# Flags live in one place: tests/real/run.sh --help.
realtest:
	tests/real/run.sh

# The campaign plus phase 10, the terabyte: an overnight session that owns
# the disk and replaces phases 5, 8 and 9 at scale.
realtest-tb:
	tests/real/run.sh --tb

# The harness exercised against a tempdir at a thousandth of the size. The
# verdict is stamped NOT CAMPAIGN GRADE.
realtest-smoke:
	QSSRT_UNSAFE_ALLOW_ANY_PATH=1 \
	QSSRT_MOUNT=$${QSSRT_MOUNT:-/tmp/qss-realtest-smoke} \
	QSSRT_STORE_ROOT=$${QSSRT_STORE_ROOT:-/tmp/qss-realtest-smoke/store} \
	tests/real/run.sh --scale 4096 --fresh

# The harness's own tests: no store, no daemon, no disk.
realtest-selftest:
	tests/real/run.sh --selftest

# Run the application (respcas)
run-respcas:
	cargo run -p respcas

# Run the application (s3cas)
run-s3cas:
	cargo run -p s3cas
