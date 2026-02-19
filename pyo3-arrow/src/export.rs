//! Wrappers around objects defined in this crate to simplify returning data to `arro3-core`.
//!
//! By default, if you return something like a `PyArray` from your Python function, it will work
//! because `PyArray` implements `#[pyclass]`, but it will statically link the private methods
//! defined on `PyArray` in your given version of `pyo3-arrow`.
//!
//! This isn't ideal for a few reasons. For one, this means that the actual classes returned from
//! multiple packages will be _different_. This also means that any updates in the latest `arro3`
//! version won't be reflected in your exported classes.
//!
//! Instead, because Arrow is an ABI-stable format, it's easy to _dynamically_ link the data. So we
//! can pass Arrow data at runtime to whatever version of `arro3-core` the user has in their Python
//! environment.
//!
//! Because each of the objects in this module implements `[IntoPyObject]`, you can return these
//! objects directly.
//!
//! ```notest
//! /// A function that will automatically return
//! #[pyfunction]
//! fn my_function() -> pyo3_arrow::export::Arro3Array {
//!     todo!()
//! }
//! ```
//!
//! Note that this means you must require `arro3-core` as a Python dependency in the
//! `pyproject.toml` of your Rust-Python library.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::ffi::{to_array_pycapsules, to_schema_pycapsule, ArrayReader};
use crate::{
    PyArray, PyArrayReader, PyChunkedArray, PyDataType, PyField, PyRecordBatch,
    PyRecordBatchReader, PyScalar, PySchema, PyTable,
};

/// A wrapper around a [PyArray] that implements [IntoPyObject] to convert to a runtime-available
/// `arro3.core.Array`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.Array` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3Array(PyArray);

impl From<PyArray> for Arro3Array {
    fn from(value: PyArray) -> Self {
        Self(value)
    }
}

impl From<ArrayRef> for Arro3Array {
    fn from(value: ArrayRef) -> Self {
        Self(value.into())
    }
}

impl<'py> IntoPyObject<'py> for Arro3Array {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Array"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            to_array_pycapsules(py, self.0.field().clone(), &self.0.array(), None)?,
        )
    }
}

/// A wrapper around a [PyArrayReader] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.ArrayReader`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.ArrayReader` and not the one statically linked from Rust.
pub struct Arro3ArrayReader(PyArrayReader);

impl From<PyArrayReader> for Arro3ArrayReader {
    fn from(value: PyArrayReader) -> Self {
        Self(value)
    }
}

impl From<Box<dyn ArrayReader + Send>> for Arro3ArrayReader {
    fn from(value: Box<dyn ArrayReader + Send>) -> Self {
        Self(value.into())
    }
}

impl<'py> IntoPyObject<'py> for Arro3ArrayReader {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = self.0.to_stream_pycapsule(py, None)?;

        arro3_mod.getattr(intern!(py, "ArrayReader"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, [capsule])?,
        )
    }
}

/// A wrapper around a [PyChunkedArray] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.ChunkedArray`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.ChunkedArray` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3ChunkedArray(PyChunkedArray);

impl From<PyChunkedArray> for Arro3ChunkedArray {
    fn from(value: PyChunkedArray) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for Arro3ChunkedArray {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let capsule = self.0.to_stream_pycapsule(py, None)?;

        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod
            .getattr(intern!(py, "ChunkedArray"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![capsule])?,
            )
    }
}

/// A wrapper around a [PyField] that implements [IntoPyObject] to convert to a runtime-available
/// `arro3.core.Field`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.Field` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3Field(PyField);

impl From<PyField> for Arro3Field {
    fn from(value: PyField) -> Self {
        Self(value)
    }
}

impl From<FieldRef> for Arro3Field {
    fn from(value: FieldRef) -> Self {
        Self(value.into())
    }
}

impl From<&Field> for Arro3Field {
    fn from(value: &Field) -> Self {
        Self(Arc::new(value.clone()).into())
    }
}

impl<'py> IntoPyObject<'py> for Arro3Field {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Field"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![to_schema_pycapsule(py, self.0.as_ref())?])?,
        )
    }
}

/// A wrapper around a [PyDataType] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.DataType`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.DataType` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3DataType(PyDataType);

impl From<PyDataType> for Arro3DataType {
    fn from(value: PyDataType) -> Self {
        Self(value)
    }
}

impl From<DataType> for Arro3DataType {
    fn from(value: DataType) -> Self {
        Self(PyDataType::new(value))
    }
}

impl<'py> IntoPyObject<'py> for Arro3DataType {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "DataType"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![to_schema_pycapsule(py, self.0.as_ref())?])?,
        )
    }
}

/// A wrapper around a [PyRecordBatch] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.RecordBatch`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.RecordBatch` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3RecordBatch(PyRecordBatch);

impl From<PyRecordBatch> for Arro3RecordBatch {
    fn from(value: PyRecordBatch) -> Self {
        Self(value)
    }
}

impl From<RecordBatch> for Arro3RecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self(value.into())
    }
}

impl<'py> IntoPyObject<'py> for Arro3RecordBatch {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsules = PyRecordBatch::to_array_pycapsules(py, self.0.into_inner(), None)?;
        arro3_mod
            .getattr(intern!(py, "RecordBatch"))?
            .call_method1(intern!(py, "from_arrow_pycapsule"), capsules)
    }
}

/// A wrapper around a [PyRecordBatchReader] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.RecordBatchReader`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.RecordBatchReader` and not the one statically linked from Rust.
pub struct Arro3RecordBatchReader(PyRecordBatchReader);

impl From<PyRecordBatchReader> for Arro3RecordBatchReader {
    fn from(value: PyRecordBatchReader) -> Self {
        Self(value)
    }
}

impl From<Box<dyn RecordBatchReader + Send>> for Arro3RecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader + Send>) -> Self {
        Self(PyRecordBatchReader::new(value))
    }
}

impl<'py> IntoPyObject<'py> for Arro3RecordBatchReader {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = self.0.to_stream_pycapsule(py, None)?;
        arro3_mod
            .getattr(intern!(py, "RecordBatchReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![capsule])?,
            )
    }
}

/// A wrapper around a [PyScalar] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.Scalar`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.Scalar` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3Scalar(PyScalar);

impl From<PyScalar> for Arro3Scalar {
    fn from(value: PyScalar) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for Arro3Scalar {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let capsules = to_array_pycapsules(py, self.0.field().clone(), &self.0.array(), None)?;

        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod
            .getattr(intern!(py, "Scalar"))?
            .call_method1(intern!(py, "from_arrow_pycapsule"), capsules)
    }
}

/// A wrapper around a [PySchema] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.Schema`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.Schema` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3Schema(PySchema);

impl From<PySchema> for Arro3Schema {
    fn from(value: PySchema) -> Self {
        Self(value)
    }
}

impl From<SchemaRef> for Arro3Schema {
    fn from(value: SchemaRef) -> Self {
        Self(PySchema::new(value))
    }
}

impl From<Schema> for Arro3Schema {
    fn from(value: Schema) -> Self {
        Self(PySchema::new(Arc::new(value)))
    }
}

impl<'py> IntoPyObject<'py> for Arro3Schema {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Schema"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![to_schema_pycapsule(py, self.0.as_ref())?])?,
        )
    }
}

/// A wrapper around a [PyTable] that implements [IntoPyObject] to convert to a
/// runtime-available `arro3.core.Table`.
///
/// This ensures that we return data with the **user's** runtime-provided (dynamically-linked)
/// `arro3.core.Table` and not the one statically linked from Rust.
#[derive(Debug)]
pub struct Arro3Table(PyTable);

impl From<PyTable> for Arro3Table {
    fn from(value: PyTable) -> Self {
        Self(value)
    }
}

impl<'py> IntoPyObject<'py> for Arro3Table {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = self.0.to_stream_pycapsule(py, None)?;
        arro3_mod.getattr(intern!(py, "Table"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![capsule])?,
        )
    }
}
