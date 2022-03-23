build: pre
	cargo build --all-targets

pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

release: pre
	cargo build --release

flamegraph:
	cargo flamegraph --bench lmdb_benchmark
	firefox ./flamegraph.svg

release_native: pre
	RUSTFLAGS='-C target-cpu=native' cargo build --release

publish_py: test_py
	docker pull quay.io/pypa/manylinux2014_x86_64
	docker run -it --rm -v $(shell pwd):/redb quay.io/pypa/manylinux2014_x86_64 /redb/py_publish.sh

test_py: install_py
	python3 -m unittest discover

install_py: pre
	maturin develop

test: pre
	RUST_BACKTRACE=1 cargo test

bench: pre
	cargo bench

# Nightly version selected from: https://rust-lang.github.io/rustup-components-history/
NIGHTLY := "nightly-2022-03-21"
fuzz: pre
	rustup toolchain install $(NIGHTLY)
	cargo +$(NIGHTLY) fuzz run fuzz_redb -- -max_len=1000000

RUST_SYSROOT := $(shell cargo +$(NIGHTLY) rustc -- --print sysroot 2>/dev/null)
LLVM_COV := $(shell find $(RUST_SYSROOT) -name llvm-cov)
fuzz_coverage: pre
	echo $(LLVM_COV)
	rustup component add llvm-tools-preview --toolchain $(NIGHTLY)
	cargo +$(NIGHTLY) fuzz coverage fuzz_redb
	$(LLVM_COV) show fuzz/target/*/release/fuzz_redb \
					--format html \
                    -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
                    -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*' \
                    > fuzz/coverage/coverage_report.html
	$(LLVM_COV) report fuzz/target/*/release/fuzz_redb \
                    -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
                    -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*'
	firefox ./fuzz/coverage/coverage_report.html
