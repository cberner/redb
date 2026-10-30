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
