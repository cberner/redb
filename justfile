build: pre
    cargo build --all-targets --all-features
    cargo doc

build_all: pre_all
    cargo build --all --all-targets --all-features
    cargo doc --all

pre:
    cargo deny --workspace --all-features check licenses
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features

pre_all:
    cargo deny --workspace --all-features check licenses
    cargo fmt --all -- --check
    cargo clippy --all --all-targets --all-features

release: pre
    cargo build --release

flamegraph:
    cargo flamegraph -p redb-bench --bench redb_benchmark
    firefox ./flamegraph.svg

publish_py: test_py
    docker pull quay.io/pypa/manylinux2014_x86_64
    MATURIN_PYPI_TOKEN=$(cat ~/.pypi/redb_token) docker run -it --rm -e "MATURIN_PYPI_TOKEN" -v `pwd`:/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/crates/redb-python/py_publish.sh

test_py: install_py
    python3 -m unittest discover --start-directory=./crates/redb-python

install_py: pre
    maturin develop --manifest-path=./crates/redb-python/Cargo.toml

test: pre
    RUST_BACKTRACE=1 cargo test --all-features

test_all: build_all
    RUST_BACKTRACE=1 cargo test --all --all-features

test_wasi:
    rustup install nightly-2025-05-04 --target wasm32-wasip1-threads
    # Uses cargo pkgid because "redb" is ambiguous with the test dependency on an old version of redb
    cargo +nightly-2025-05-04 test -p $(cargo pkgid) --target=wasm32-wasip1-threads -- --nocapture
    cargo +nightly-2025-05-04 test -p redb-derive --target=wasm32-wasip1-threads -- --nocapture

bench bench='redb_benchmark': pre
    cargo bench -p redb-bench --bench {{bench}}

watch +args='test':
    cargo watch --clear --exec "{{args}}"

fuzz: pre
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_cmin:
    cargo fuzz cmin --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_ci: pre_all
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=10000 -max_total_time=60

fuzz_coverage: pre
    #!/usr/bin/env bash
    set -euxo pipefail
    RUST_SYSROOT=`cargo rustc -- --print sysroot 2>/dev/null`
    LLVM_COV=`find $RUST_SYSROOT -name llvm-cov`
    echo $LLVM_COV
    rustup component add llvm-tools-preview
    cargo fuzz coverage --sanitizer=none fuzz_redb
    $LLVM_COV show fuzz/target/*/release/fuzz_redb --format html \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*' > fuzz/coverage/coverage_report.html
    $LLVM_COV report fuzz/target/*/release/fuzz_redb \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*'
    firefox ./fuzz/coverage/coverage_report.html
