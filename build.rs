fn main() {
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");

    if std::env::var("CARGO_CFG_FUZZING").is_ok()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
