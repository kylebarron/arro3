use std::ffi::CString;
use std::fmt::Display;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;

/// A Python-facing Arrow schema.
///
/// This is a wrapper around a [SchemaRef].
#[pyclass(module = "arro3.core._rust", name = "Schema", subclass)]
pub struct PySchema(SchemaRef);

impl PySchema {
    pub fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }

    /// Export this to a Python `arro3.core.Schema`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Schema"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_schema__(py)?]),
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.Schema
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "schema"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl From<PySchema> for SchemaRef {
    fn from(value: PySchema) -> Self {
        value.0
    }
}

impl From<SchemaRef> for PySchema {
    fn from(value: SchemaRef) -> Self {
        Self(value)
    }
}

impl AsRef<Schema> for PySchema {
    fn as_ref(&self) -> &Schema {
        &self.0
    }
}

impl Display for PySchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.Schema")?;
        writeln!(f, "------------")?;
        display_schema(&self.0, f)
    }
}

pub(crate) fn display_schema(schema: &Schema, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    schema.fields().iter().try_for_each(|field| {
        f.write_str(field.name().as_str())?;
        write!(f, ": ")?;
        field.data_type().fmt(f)?;
        writeln!(f)?;
        Ok::<_, std::fmt::Error>(())
    })?;
    Ok(())
}

#[pymethods]
impl PySchema {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this array
    /// into a pyarrow schema, without copying memory.
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
        Ok(schema_capsule)
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this object from an existing Arrow object
    ///
    /// It can be called on anything that exports the Arrow data interface
    /// (`__arrow_c_array__`) and returns a struct field.
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
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
        let schema =
            Schema::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(schema)))
    }

    pub fn __eq__(&self, other: &PySchema) -> bool {
        self.0 == other.0
    }
}
