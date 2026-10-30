use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyType};

/// The key and value types a table may declare.
///
/// Each is the redb type of the same name, so a table a Rust program wrote with that type
/// opens from Python with this one, and sorts the same way.
#[pyclass(module = "redb", name = "Type", eq, hash, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Type {
    #[pyo3(name = "bytes")]
    Bytes,
    #[pyo3(name = "u64")]
    U64,
}

impl Type {
    const ALL: [Type; 2] = [Type::Bytes, Type::U64];

    /// The type's name in Python, as `redb.<name>` and `Type.<name>`
    pub(crate) fn name(self) -> &'static str {
        match self {
            Type::Bytes => "bytes",
            Type::U64 => "u64",
        }
    }

    /// Accepts a `Type`, or a Python builtin whose redb encoding is unambiguous
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Type> {
        if let Ok(t) = obj.extract::<Type>() {
            return Ok(t);
        }
        let bytes = obj.py().get_type::<PyBytes>();
        if obj.cast::<PyType>().is_ok_and(|cls| cls.is(&bytes)) {
            return Ok(Type::Bytes);
        }
        Err(PyTypeError::new_err(format!(
            "expected a redb.Type or bytes (int needs an explicit width such as redb.u64), got {}",
            obj.repr()?
        )))
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Type>()?;
    // Each type is a module attribute too: `redb.u64` is `redb.Type.u64`
    let class = m.py().get_type::<Type>();
    for t in Type::ALL {
        m.add(t.name(), class.getattr(t.name())?)?;
    }
    Ok(())
}
