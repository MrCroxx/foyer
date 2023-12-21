SHELL := /bin/bash
.PHONY: deps check test test-ignored test-all all fast monitor clear madsim

deps:
	./scripts/install-deps.sh

check:
	typos
	shellcheck ./scripts/*
	./.github/template/generate.sh
	./scripts/minimize-dashboards.sh
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets
	cargo udeps --workspace --exclude foyer-workspace-hack

check-all:
	shellcheck ./scripts/*
	./.github/template/generate.sh
	./scripts/minimize-dashboards.sh
	cargo hakari generate
	cargo hakari manage-deps
	cargo sort -w
	cargo fmt --all
	cargo clippy --all-targets --features deadlock
	cargo clippy --all-targets --features tokio-console
	cargo clippy --all-targets --features trace
	cargo clippy --all-targets
	cargo udeps --workspace --exclude foyer-workspace-hack

test:
	RUST_BACKTRACE=1 cargo nextest run --all
	RUST_BACKTRACE=1 cargo test --doc

test-ignored:
	RUST_BACKTRACE=1 cargo nextest run --run-ignored ignored-only --no-capture --workspace

test-all: test test-ignored

madsim:
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" cargo clippy --all-targets
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo nextest run --all
	RUSTFLAGS="--cfg madsim --cfg tokio_unstable" RUST_BACKTRACE=1 cargo test --doc

all: check-all test-all

fast: check test

monitor:
	./scripts/monitor.sh

clear:
	rm -rf .tmp
