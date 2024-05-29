use std::ffi::CString;
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::interop::numpy::to_numpy::to_numpy;

#[pyclass(module = "arro3.core._rust", name = "Array", subclass)]
pub struct PyArray {
    array: ArrayRef,
    field: FieldRef,
}

impl PyArray {
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        Self { array, field }
    }

    pub fn to_python(&self, py: Python) -> PyArrowResult<PyObject> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Array"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            self.__arrow_c_array__(py, None)?,
        )?;
        Ok(core_obj.to_object(py))
    }
}

#[pymethods]
impl PyArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
        to_numpy(py, &self.array)
    }

    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<PyObject>,
    ) -> PyArrowResult<&'py PyTuple> {
        let field = &self.field;
        let ffi_schema = FFI_ArrowSchema::try_from(field)?;
        let ffi_array = FFI_ArrowArray::new(&self.array.to_data());

        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let array_capsule_name = CString::new("arrow_array").unwrap();

        let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
        let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
        let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);

        Ok(tuple)
    }

    pub fn __eq__(&self, other: &PyArray) -> bool {
        self.array.as_ref() == other.array.as_ref() && self.field == other.field
    }

    pub fn __len__(&self) -> usize {
        self.array.len()
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

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &PyType,
        schema_capsule: &PyCapsule,
        array_capsule: &PyCapsule,
    ) -> PyResult<Self> {
        let (array, field) = import_array_pycapsules(schema_capsule, array_capsule)?;
        Ok(Self::new(array, Arc::new(field)))
    }

    /// Copy this array to a `numpy` NDArray
    pub fn to_numpy(&self, py: Python) -> PyResult<PyObject> {
        self.__array__(py)
    }
}
