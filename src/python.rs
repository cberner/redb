use pyo3::prelude::*;
use pyo3::types::*;

#[pymodule]
pub fn redb(_py: Python, _m: &PyModule) -> PyResult<()> {
    Ok(())
}
