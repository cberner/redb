use pyo3::prelude::*;

#[pymodule]
pub fn redbx(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
