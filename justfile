build: pre
	cargo build --all-targets
	cargo doc

pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

release: pre
	cargo build --release

flamegraph:
	cargo flamegraph --bench lmdb_benchmark
	firefox ./flamegraph.svg

publish_py: test_py
	docker pull quay.io/pypa/manylinux2014_x86_64
	docker run -it --rm -v $(shell pwd):/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/py_publish.sh

test_py: install_py
	python3 -m unittest discover

install_py: pre
	maturin develop

test: pre
	RUST_BACKTRACE=1 cargo test

bench: pre
	cargo bench --bench lmdb_benchmark

# Nightly version selected from: https://rust-lang.github.io/rustup-components-history/
NIGHTLY := "nightly-2022-11-01"
fuzz: pre
	rustup toolchain install {{NIGHTLY}}
	cargo +{{NIGHTLY}} fuzz run fuzz_redb -- -max_len=1000000

fuzz_ci: pre
	rustup toolchain install {{NIGHTLY}}
	cargo +{{NIGHTLY}} fuzz run fuzz_redb -- -max_len=1000000 -max_total_time=60

fuzz_coverage: pre
	rustup toolchain install {{NIGHTLY}}
	$(eval RUST_SYSROOT := $(shell cargo +{{NIGHTLY}} rustc -- --print sysroot 2>/dev/null))
	$(eval LLVM_COV := $(shell find $(RUST_SYSROOT) -name llvm-cov))
	echo $(LLVM_COV)
	rustup component add llvm-tools-preview --toolchain {{NIGHTLY}}
	cargo +{{NIGHTLY}} fuzz coverage fuzz_redb
	$(LLVM_COV) show fuzz/target/*/release/fuzz_redb --format html \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*' > fuzz/coverage/coverage_report.html
	$(LLVM_COV) report fuzz/target/*/release/fuzz_redb \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*'
	firefox ./fuzz/coverage/coverage_report.html
