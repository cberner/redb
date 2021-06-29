pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all

release: pre
	cargo build --release

release_native: pre
	RUSTFLAGS='-C target-cpu=native' cargo build --release
