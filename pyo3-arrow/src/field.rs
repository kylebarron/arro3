use std::ffi::CString;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::{Field, FieldRef};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;

/// A Python-facing Arrow field.
///
/// This is a wrapper around a [FieldRef].
#[pyclass(module = "arro3.core._rust", name = "Field", subclass)]
pub struct PyField(FieldRef);

impl PyField {
    pub fn new(field: FieldRef) -> Self {
        Self(field)
    }

    /// Export this to a Python `arro3.core.Field`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Field"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_schema__(py)?]),
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.Field
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "field"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl From<PyField> for FieldRef {
    fn from(value: PyField) -> Self {
        value.0
    }
}

impl From<FieldRef> for PyField {
    fn from(value: FieldRef) -> Self {
        Self(value)
    }
}

impl AsRef<Field> for PyField {
    fn as_ref(&self) -> &Field {
        &self.0
    }
}

#[pymethods]
impl PyField {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.field()`][pyarrow.field] to convert this array
    /// into a pyarrow field, without copying memory.
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.0.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        Ok(PyCapsule::new_bound(
            py,
            ffi_schema,
            Some(schema_capsule_name),
        )?)
    }

    pub fn __eq__(&self, other: &PyField) -> bool {
        self.0 == other.0
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow schema interface
    /// (`__arrow_c_schema__`).
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let field =
            Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(field)))
    }
}
