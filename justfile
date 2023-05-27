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
    docker run -it --rm -v `pwd`:/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/py_publish.sh

test_py: install_py
    python3 -m unittest discover

install_py: pre
    maturin develop

test: pre
    RUST_BACKTRACE=1 cargo test

test_wasi: pre
    CARGO_TARGET_WASM32_WASI_RUNNER="wasmtime --dir=." cargo +nightly wasi test --target=wasm32-wasi -- --nocapture

bench bench='lmdb_benchmark': pre
    cargo bench --bench {{bench}}

watch +args='test':
    cargo watch --clear --exec "{{args}}"

fuzz: pre
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=100000

fuzz_ci: pre
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=100000 -max_total_time=60

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
