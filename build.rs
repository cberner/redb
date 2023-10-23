fn main() {
    if std::env::var("CARGO_CFG_FUZZING").is_ok()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }

    #[cfg(feature = "python")]
    pyo3_build_config::add_extension_module_link_args();
}
