use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyFloat, PyString, PyType};
use redb::Value;

/// The key and value types a table may declare.
///
/// Each maps to the redb type of the same name, so a table written by a Rust program with that
/// type opens from Python with this one, and sorts the same way.
#[pyclass(
    module = "redb",
    name = "Type",
    eq,
    eq_int,
    hash,
    frozen,
    from_py_object
)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Type {
    Bytes,
    Str,
    Bool,
    U64,
    I64,
    U128,
    F64,
}

impl Type {
    const ALL: [Type; 7] = [
        Type::Bytes,
        Type::Str,
        Type::Bool,
        Type::U64,
        Type::I64,
        Type::U128,
        Type::F64,
    ];

    pub(crate) fn is_key(self) -> bool {
        !matches!(self, Type::F64)
    }

    /// Accepts a `Type`, or a Python builtin whose redb width is unambiguous
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Type> {
        if let Ok(t) = obj.extract::<Type>() {
            return Ok(t);
        }
        let py = obj.py();
        if let Ok(cls) = obj.cast::<PyType>() {
            let builtins = [
                (py.get_type::<PyBytes>(), Type::Bytes),
                (py.get_type::<PyString>(), Type::Str),
                (py.get_type::<PyBool>(), Type::Bool),
                (py.get_type::<PyFloat>(), Type::F64),
            ];
            if let Some((_, t)) = builtins.iter().find(|(b, _)| cls.is(b)) {
                return Ok(*t);
            }
        }
        Err(PyTypeError::new_err(format!(
            "expected a redb.Type, or one of bytes, str, bool, float \
             (int needs an explicit width such as redb.U64), got {}",
            obj.repr()?
        )))
    }
}

/// Conversion between a redb value type and Python objects
pub(crate) trait PyValue: Value + 'static {
    /// The owned form of a value taken from Python, which `borrow` lends to redb
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
                obj.get_type()
            )))
        }
    }

    fn lend(owned: &Self::Owned) -> &[u8] {
        owned
    }

    fn to_py<'py>(py: Python<'py>, value: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyBytes::new(py, value).into_any())
    }
}

impl PyValue for &'static str {
    type Owned = String;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<String> {
        obj.extract()
    }

    fn lend(owned: &Self::Owned) -> &str {
        owned
    }

    fn to_py<'py>(py: Python<'py>, value: &str) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyString::new(py, value).into_any())
    }
}

macro_rules! copy_values {
    ($($t:ty),*) => {$(
        impl PyValue for $t {
            type Owned = $t;

            fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<$t> {
                obj.extract()
            }

            fn lend(owned: &Self::Owned) -> $t {
                *owned
            }

            fn to_py(py: Python<'_>, value: $t) -> PyResult<Bound<'_, PyAny>> {
                value.into_bound_py_any(py)
            }
        }
    )*};
}

copy_values!(bool, u64, i64, u128, f64);

/// Runs `$body` with `$V` bound to the redb type of `$t`
macro_rules! with_value_type {
    ($t:expr, $V:ident, $body:block) => {
        match $t {
            $crate::types::Type::Bytes => {
                type $V = &'static [u8];
                $body
            }
            $crate::types::Type::Str => {
                type $V = &'static str;
                $body
            }
            $crate::types::Type::Bool => {
                type $V = bool;
                $body
            }
            $crate::types::Type::U64 => {
                type $V = u64;
                $body
            }
            $crate::types::Type::I64 => {
                type $V = i64;
                $body
            }
            $crate::types::Type::U128 => {
                type $V = u128;
                $body
            }
            $crate::types::Type::F64 => {
                type $V = f64;
                $body
            }
        }
    };
}

/// Runs `$body` with `$K` bound to the redb type of `$t`, which must be a key type
macro_rules! with_key_type {
    ($t:expr, $K:ident, $body:block) => {
        match $t {
            $crate::types::Type::Bytes => {
                type $K = &'static [u8];
                $body
            }
            $crate::types::Type::Str => {
                type $K = &'static str;
                $body
            }
            $crate::types::Type::Bool => {
                type $K = bool;
                $body
            }
            $crate::types::Type::U64 => {
                type $K = u64;
                $body
            }
            $crate::types::Type::I64 => {
                type $K = i64;
                $body
            }
            $crate::types::Type::U128 => {
                type $K = u128;
                $body
            }
            $crate::types::Type::F64 => return Err($crate::types::not_a_key()),
        }
    };
}

/// Dispatches on a table's key and value types
macro_rules! with_types {
    ($k:expr, $v:expr, $K:ident, $V:ident, $body:block) => {
        $crate::types::with_key_type!($k, $K, { $crate::types::with_value_type!($v, $V, $body) })
    };
}

/// Dispatches on a multimap table's key and value types, both of which are key types
macro_rules! with_key_types {
    ($k:expr, $v:expr, $K:ident, $V:ident, $body:block) => {
        $crate::types::with_key_type!($k, $K, { $crate::types::with_key_type!($v, $V, $body) })
    };
}

pub(crate) use {with_key_type, with_key_types, with_types, with_value_type};

pub(crate) fn not_a_key() -> PyErr {
    PyTypeError::new_err("F64 cannot be a key type")
}

/// Raises the conversion error for a key of the wrong Python type, if `key` is given
pub(crate) fn check_key(t: Type, key: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let Some(key) = key else { return Ok(()) };
    with_key_type!(t, K, {
        K::from_py(key)?;
        Ok(())
    })
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Type>()?;
    // redb.U64 and friends, as shorthand for redb.Type.U64
    for t in Type::ALL {
        m.add(format!("{t:?}"), t)?;
    }
    Ok(())
}
