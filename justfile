build: pre
    ./scripts/podman-sandbox.sh 'cargo build --frozen --all-targets --all-features'
    ./scripts/podman-sandbox.sh 'cargo doc --frozen'

build_all: pre_all
    ./scripts/podman-sandbox.sh 'cargo build --frozen --all --all-targets --all-features'
    ./scripts/podman-sandbox.sh 'cargo doc --frozen --all --no-deps'

pre: _audit _checks

pre_no_sandbox: _audit _checks_no_sandbox

_audit:
    cargo deny --workspace --all-features check licenses advisories
    cargo fmt --all -- --check

_checks:
    ./scripts/podman-sandbox.sh 'just _checks_no_sandbox'

_checks_no_sandbox:
    cargo clippy --all-targets --all-features

pre_all: _audit _checks_all

pre_all_no_sandbox: _audit _checks_all_no_sandbox

_checks_all:
    ./scripts/podman-sandbox.sh 'just _checks_all_no_sandbox'

_checks_all_no_sandbox:
    cargo clippy --all --all-targets --all-features

flamegraph:
    cargo flamegraph -p redb-bench --bench redb_benchmark
    firefox ./flamegraph.svg

publish_py: test_py
    docker pull quay.io/pypa/manylinux2014_x86_64
    MATURIN_PYPI_TOKEN=$(cat ~/.pypi/redb_token) docker run -it --rm -e "MATURIN_PYPI_TOKEN" -v `pwd`:/redb-ro:ro quay.io/pypa/manylinux2014_x86_64 /redb-ro/crates/redb-python/py_publish.sh

test_py: install_py
    python3 -m pytest ./crates/redb-python/test

install_py: pre_no_sandbox
    maturin develop --manifest-path=./crates/redb-python/Cargo.toml
    python3 -m pip install pytest hypothesis

test: pre
    ./scripts/podman-sandbox.sh 'RUST_BACKTRACE=1 cargo test --frozen --all-features'

test_all: build_all
    ./scripts/podman-sandbox.sh 'RUST_BACKTRACE=1 cargo test --frozen --all --all-features'

test_all_no_sandbox: pre_all_no_sandbox
    cargo build --frozen --all --all-targets --all-features
    cargo doc --frozen --all --no-deps
    RUST_BACKTRACE=1 cargo test --frozen --all --all-features

clear_podman_cache:
    podman volume rm --force redb-sandbox-target

test_wasi:
    rustup install nightly-2025-07-26 --target wasm32-wasip1-threads
    # Uses cargo pkgid because "redb" is ambiguous with the test dependency on an old version of redb
    cargo +nightly-2025-07-26 test -p $(cargo pkgid) --target=wasm32-wasip1-threads -- --nocapture
    cargo +nightly-2025-07-26 test -p redb-derive --target=wasm32-wasip1-threads -- --nocapture

coverage:
    #!/usr/bin/env bash
    set -euxo pipefail
    cargo install --locked cargo-llvm-cov
    rustup component add llvm-tools-preview
    RUST_BACKTRACE=1 cargo llvm-cov --all-features

bench bench='redb_benchmark': pre
    ./scripts/podman-sandbox.sh 'REDB_BENCHMARK_DIR=/scratch cargo bench --frozen -p redb-bench --bench {{bench}}'

build_bench_container:
    docker build -t redb-bench:latest -f Dockerfile.bench .

bench_containerized bench='lmdb_benchmark': build_bench_container
    # Exec the binary directly, because at low memory limits there may not be enough to invoke cargo & rustc
    docker run --rm -it --memory=4g redb-bench:latest bash -c "cd /code/redb && ./target/release/deps/{{bench}}-*"

watch +args='test':
    cargo watch --clear --exec "{{args}}"

fuzz: pre
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_cmin:
    cargo fuzz cmin --sanitizer=none fuzz_redb -- -max_len=10000

fuzz_ci: pre_all_no_sandbox
    cargo fuzz run --sanitizer=none fuzz_redb -- -max_len=10000 -max_total_time=60

fuzz_coverage: pre
    #!/usr/bin/env bash
    set -euxo pipefail
    rustup component add llvm-tools-preview
    RUST_SYSROOT=`cargo rustc -- --print sysroot 2>/dev/null`
    LLVM_COV=`find $RUST_SYSROOT -name llvm-cov`
    echo $LLVM_COV
    cargo fuzz coverage --sanitizer=none fuzz_redb
    $LLVM_COV show target/*/coverage/*/release/fuzz_redb --format html \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*' > fuzz/coverage/coverage_report.html
    $LLVM_COV report target/*/coverage/*/release/fuzz_redb \
          -instr-profile=fuzz/coverage/fuzz_redb/coverage.profdata \
          -ignore-filename-regex='.*(cargo/registry|redb/fuzz|rustc).*'
    firefox ./fuzz/coverage/coverage_report.html
