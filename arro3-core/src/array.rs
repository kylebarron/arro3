use std::ffi::CString;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;

#[pyclass(module = "arro3.core._rust", name = "Array", subclass)]
pub struct PyArray {
    array: ArrayRef,
    field: FieldRef,
}

impl PyArray {
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        Self { array, field }
    }
}

#[pymethods]
impl PyArray {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__(&self, requested_schema: Option<PyObject>) -> PyArrowResult<PyObject> {
        let field = &self.field;
        let ffi_schema = FFI_ArrowSchema::try_from(field)?;
        let ffi_array = FFI_ArrowArray::new(&self.array.to_data());

        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let array_capsule_name = CString::new("arrow_array").unwrap();

        Python::with_gil(|py| {
            let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
            let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
            let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);
            Ok(tuple.to_object(py))
        })
    }

    /// Construct this object from existing Arrow data
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
    #[classmethod]
    pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
        input.extract()
    }
}
