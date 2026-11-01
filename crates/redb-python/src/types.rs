use ::redb::Value;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyFloat, PyString, PyType};

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
    #[pyo3(name = "str")]
    Str,
    #[pyo3(name = "i64")]
    I64,
    #[pyo3(name = "bool")]
    Bool,
    #[pyo3(name = "u128")]
    U128,
    #[pyo3(name = "i128")]
    I128,
    /// A value type alone: redb gives floats no order
    #[pyo3(name = "f64")]
    F64,
}

impl Type {
    const ALL: [Type; 8] = [
        Type::Bytes,
        Type::U64,
        Type::Str,
        Type::I64,
        Type::Bool,
        Type::U128,
        Type::I128,
        Type::F64,
    ];

    /// The type's name in Python, as `redb.<name>` and `Type.<name>`
    pub(crate) fn name(self) -> &'static str {
        match self {
            Type::Bytes => "bytes",
            Type::U64 => "u64",
            Type::Str => "str",
            Type::I64 => "i64",
            Type::Bool => "bool",
            Type::U128 => "u128",
            Type::I128 => "i128",
            Type::F64 => "f64",
        }
    }

    /// Accepts a `Type`, or a Python builtin whose redb encoding is unambiguous
    pub(crate) fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<Type> {
        if let Ok(t) = obj.extract::<Type>() {
            return Ok(t);
        }
        let py = obj.py();
        if let Ok(cls) = obj.cast::<PyType>() {
            if cls.is(py.get_type::<PyBytes>()) {
                return Ok(Type::Bytes);
            }
            if cls.is(py.get_type::<PyString>()) {
                return Ok(Type::Str);
            }
            if cls.is(py.get_type::<PyBool>()) {
                return Ok(Type::Bool);
            }
            if cls.is(py.get_type::<PyFloat>()) {
                return Ok(Type::F64);
            }
        }
        Err(PyTypeError::new_err(format!(
            "expected a redb.Type, bytes, str, bool or float (int needs an explicit width such as redb.u64), got {}",
            obj.repr()?
        )))
    }

    /// `from_py` for a key type, which `f64` cannot be
    pub(crate) fn key_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Type> {
        match Type::from_py(obj)? {
            Type::F64 => Err(PyTypeError::new_err(
                "f64 cannot be a key type: redb orders ints, bool, bytes and str, not floats",
            )),
            t => Ok(t),
        }
    }
}

/// The numeric types, whose conversions are Python's own: a `TypeError` for an object of
/// another type, an `OverflowError` for an int outside the type's range
macro_rules! number_value {
    ($($t:ty),*) => {$(
        impl PyValue for $t {
            type Owned = $t;

            fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<$t> {
                obj.extract()
            }

            fn lend(owned: &$t) -> $t {
                *owned
            }

            fn to_py(py: Python<'_>, value: $t) -> PyResult<Bound<'_, PyAny>> {
                value.into_bound_py_any(py)
            }
        }
    )*};
}

number_value!(u64, i64, u128, i128, f64);

impl PyValue for bool {
    type Owned = bool;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
        match obj.cast::<PyBool>() {
            Ok(value) => Ok(value.is_true()),
            Err(_) => Err(PyTypeError::new_err(format!(
                "expected bool, got {}",
                obj.get_type().name()?
            ))),
        }
    }

    fn lend(owned: &bool) -> bool {
        *owned
    }

    fn to_py(py: Python<'_>, value: bool) -> PyResult<Bound<'_, PyAny>> {
        Ok(PyBool::new(py, value).to_owned().into_any())
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

impl PyValue for &'static str {
    type Owned = String;

    fn from_py(obj: &Bound<'_, PyAny>) -> PyResult<String> {
        match obj.cast::<PyString>() {
            Ok(string) => Ok(string.to_str()?.to_owned()),
            Err(_) => Err(PyTypeError::new_err(format!(
                "expected str, got {}",
                obj.get_type().name()?
            ))),
        }
    }

    fn lend(owned: &String) -> &str {
        owned
    }

    fn to_py<'py>(py: Python<'py>, value: &str) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyString::new(py, value).into_any())
    }
}

/// Runs `$body` with `$T` bound to the redb type that `$t` names at runtime, for a key
/// (`with_type!(key ...)`) or a value (`with_type!(value ...)`).
///
/// redb's API is generic over the key and value types, while a Python table definition only
/// holds `Type` values. So every operation dispatches on them through a match with an arm per
/// type, in which the body is compiled once per type; this macro is that match, written once.
/// Adding a type is adding an arm. `f64` has only the value form: a `TableDefinition` never
/// has it as its key.
macro_rules! with_type {
    (key $t:expr, $T:ident, $body:block) => {
        match $t {
            $crate::types::Type::F64 => unreachable!("f64 is not a key type"),
            t => $crate::types::with_type!(@ordered t, $T, $body),
        }
    };
    (value $t:expr, $T:ident, $body:block) => {
        match $t {
            $crate::types::Type::F64 => {
                type $T = f64;
                $body
            }
            t => $crate::types::with_type!(@ordered t, $T, $body),
        }
    };
    (@ordered $t:expr, $T:ident, $body:block) => {
        match $t {
            $crate::types::Type::Bytes => {
                type $T = &'static [u8];
                $body
            }
            $crate::types::Type::U64 => {
                type $T = u64;
                $body
            }
            $crate::types::Type::Str => {
                type $T = &'static str;
                $body
            }
            $crate::types::Type::I64 => {
                type $T = i64;
                $body
            }
            $crate::types::Type::Bool => {
                type $T = bool;
                $body
            }
            $crate::types::Type::U128 => {
                type $T = u128;
                $body
            }
            $crate::types::Type::I128 => {
                type $T = i128;
                $body
            }
            $crate::types::Type::F64 => unreachable!("f64 is dispatched before the ordered types"),
        }
    };
}

/// Runs `$body` with `$K` and `$V` bound to the redb types of a table's key and value: one
/// `with_type!` inside another, so the body is compiled once per pair of types
macro_rules! with_types {
    ($k:expr, $v:expr, $K:ident, $V:ident, $body:block) => {
        $crate::types::with_type!(key $k, $K, {
            $crate::types::with_type!(value $v, $V, $body)
        })
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
