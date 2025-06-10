use pyo3::prelude::*;

#[pymodule]
pub fn redb(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
