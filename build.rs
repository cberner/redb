fn main() {
    println!("cargo:rustc-check-cfg=cfg(fuzzing)");
    println!("cargo:rustc-check-cfg=cfg(redb_no_std)");

    // Building without the standard library is only offered under the redb 5 API preview. Cargo
    // features cannot express "experimental-api-5 and not std", so it is computed here and used as
    // `cfg(redb_no_std)` throughout the crate. Turning "std" off without "experimental-api-5" keeps
    // the std build, so that 4.x dependents who set `default-features = false` are unaffected.
    if std::env::var_os("CARGO_FEATURE_EXPERIMENTAL_API_5").is_some()
        && std::env::var_os("CARGO_FEATURE_STD").is_none()
    {
        println!("cargo:rustc-cfg=redb_no_std");
    }

    println!("cargo:rustc-check-cfg=cfg(redb_multiprocess)");
    // The multi-process modes need the standard library and a platform with byte-range file
    // locks: Linux, the Apple platforms, or Windows. Cargo features cannot express that
    // conjunction, so it is computed here and used as `cfg(redb_multiprocess)` throughout.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_vendor = std::env::var("CARGO_CFG_TARGET_VENDOR").unwrap_or_default();
    let range_lock_platform =
        target_os == "linux" || target_os == "windows" || target_vendor == "apple";
    if std::env::var_os("CARGO_FEATURE_EXPERIMENTAL_MULTIPROCESS").is_some()
        && std::env::var_os("CARGO_FEATURE_STD").is_some()
        && range_lock_platform
    {
        println!("cargo:rustc-cfg=redb_multiprocess");
    }

    if std::env::var("CARGO_CFG_FUZZING").is_ok()
        && std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
    {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
