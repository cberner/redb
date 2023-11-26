build: pre
    cargo build --all-targets
    cargo doc

pre:
    cargo deny --all-features check licenses
    cargo fmt --all -- --check
    cargo clippy --all --all-targets

release: pre
    cargo build --release

flamegraph:
    cargo flamegraph --bench lmdb_benchmark
    firefox ./flamegraph.svg

publish_py: test_py
    docker pull quay.io/pypa/manylinux2014_x86_64
    MATURIN_PYPI_TOKEN=$(cat ~/.pypi/redb_token) docker run -it --rm -e "MATURIN_PYPI_TOKEN" -v `pwd`:/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/publish_py.sh

test_py: install_py
    python3 -m unittest discover

install_py: pre
    maturin develop

test: pre
    RUST_BACKTRACE=1 cargo test

test_wasi: pre
    CARGO_TARGET_WASM32_WASI_RUNNER="wasmtime --mapdir=/::$TMPDIR" cargo +nightly wasi test -- --nocapture

bench bench='lmdb_benchmark': pre
    cargo bench --bench {{bench}}

watch +args='test':
    cargo watch --clear --exec "{{args}}"

fuzz: pre
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_cmin:
    cargo fuzz cmin --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_ci: pre
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
