release:
	cargo build --release

release_native:
	RUSTFLAGS='-C target-cpu=native' cargo build --release
