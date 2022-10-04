SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.PHONY: build
build: pre
	cargo build --all-targets
	cargo doc

.PHONY: pre
pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

.PHONY: release
release: pre
	cargo build --release

.PHONY: flamegraph
flamegraph:
	cargo flamegraph --bench lmdb_benchmark
	firefox ./flamegraph.svg

.PHONY: publish_py
publish_py: test_py
	docker pull quay.io/pypa/manylinux2014_x86_64
	docker run -it --rm -v $(shell pwd):/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/py_publish.sh

.PHONY: test_py
test_py: install_py
	python3 -m unittest discover

.PHONY: install_py
install_py: pre
	maturin develop

.PHONY: test
test: pre
	RUST_BACKTRACE=1 cargo test

.PHONY: bench
bench: pre
	cargo bench --bench lmdb_benchmark

# Nightly version selected from: https://rust-lang.github.io/rustup-components-history/
NIGHTLY := "nightly-2022-09-10"
.PHONY: fuzz
fuzz: pre
	rustup toolchain install $(NIGHTLY)
	cargo +$(NIGHTLY) fuzz run fuzz_redb -- -max_len=1000000 -timeout=60

.PHONY: fuzz_coverage
fuzz_coverage: pre
	rustup toolchain install $(NIGHTLY)
	$(eval RUST_SYSROOT := $(shell cargo +$(NIGHTLY) rustc -- --print sysroot 2>/dev/null))
	$(eval LLVM_COV := $(shell find $(RUST_SYSROOT) -name llvm-cov))
	echo $(LLVM_COV)
	rustup component add llvm-tools-preview --toolchain $(NIGHTLY)
	cargo +$(NIGHTLY) fuzz coverage fuzz_redb
	$(LLVM_COV) show fuzz/target/*/release/fuzz_redb --format html \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*' > fuzz/coverage/coverage_report.html
	$(LLVM_COV) report fuzz/target/*/release/fuzz_redb \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*'
	firefox ./fuzz/coverage/coverage_report.html
