use ::redb::Value;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyType};

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

/// Conversion between a redb value type and Python objects
pub(crate) trait PyValue: Value {
    /// The owned form of a value taken from Python, which `lend` gives redb a view of
    type Owned;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Self::Owned>;
    fn lend(owned: &Self::Owned) -> Self::SelfType<'_>;
    fn to_py<'py>(py: Python<'py>, value: Self::SelfType<'_>) -> PyResult<Bound<'py, PyAny>>;
}

impl PyValue for &'static [u8] {
    type Owned = Vec<u8>;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
        if let Ok(bytes) = obj.cast::<PyBytes>() {
            Ok(bytes.as_bytes().to_vec())
        } else if let Ok(bytes) = obj.cast::<PyByteArray>() {
            Ok(bytes.to_vec())
        } else {
            Err(PyTypeError::new_err(format!(
                "expected bytes, got {}",
                obj.get_type().name()?
            )))
        }
    }

    fn lend(owned: &Vec<u8>) -> &[u8] {
        owned
    }

    fn to_py<'py>(py: Python<'py>, value: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyBytes::new(py, value).into_any())
    }
}

impl PyValue for u64 {
    type Owned = u64;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<u64> {
        obj.extract()
    }

    fn lend(owned: &u64) -> u64 {
        *owned
    }

    fn to_py(py: Python<'_>, value: u64) -> PyResult<Bound<'_, PyAny>> {
        value.into_bound_py_any(py)
    }
}

/// Runs `$body` with `$T` bound to the redb type that `$t` names at runtime.
///
/// redb's API is generic over the key and value types, while a Python table definition only
/// holds `Type` values. So every operation dispatches on them through a match with an arm per
/// type, in which the body is compiled once per type; this macro is that match, written once.
/// Adding a type is adding an arm.
macro_rules! with_type {
    ($t:expr, $T:ident, $body:block) => {
        match $t {
            $crate::types::Type::Bytes => {
                type $T = &'static [u8];
                $body
            }
            $crate::types::Type::U64 => {
                type $T = u64;
                $body
            }
        }
    };
}

/// Runs `$body` with `$K` and `$V` bound to the redb types of a table's key and value: one
/// `with_type!` inside another, so the body is compiled once per pair of types
macro_rules! with_types {
    ($k:expr, $v:expr, $K:ident, $V:ident, $body:block) => {
        $crate::types::with_type!($k, $K, { $crate::types::with_type!($v, $V, $body) })
    };
}

pub(crate) use {with_type, with_types};

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Type>()?;
    // Each type is a module attribute too: `redb.u64` is `redb.Type.u64`
    let class = m.py().get_type::<Type>();
    for t in Type::ALL {
        m.add(t.name(), class.getattr(t.name())?)?;
    }
    Ok(())
}
