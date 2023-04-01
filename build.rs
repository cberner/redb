#[cfg(feature = "python")]
fn main() {
    pyo3_build_config::add_extension_module_link_args();
}

#[cfg(not(feature = "python"))]
fn main() {}
