use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow_schema::{Field, FieldRef};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_python::to_schema_pycapsule;
use crate::input::MetadataInput;
use crate::PyDataType;

/// A Python-facing Arrow field.
///
/// This is a wrapper around a [FieldRef].
#[pyclass(module = "arro3.core._core", name = "Field", subclass)]
pub struct PyField(FieldRef);

impl PyField {
    /// Construct a new PyField around a [FieldRef]
    pub fn new(field: FieldRef) -> Self {
        Self(field)
    }

    /// Construct from a raw Arrow C Schema capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let field =
            Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(field)))
    }

    /// Consume this and return its internal [FieldRef]
    pub fn into_inner(self) -> FieldRef {
        self.0
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

impl Display for PyField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.Field<")?;
        f.write_str(self.0.name().as_str())?;
        write!(f, ": ")?;
        self.0.data_type().fmt(f)?;
        if !self.0.is_nullable() {
            write!(f, " not null")?;
        }
        writeln!(f, ">")?;
        Ok(())
    }
}

#[pymethods]
impl PyField {
    #[new]
    #[pyo3(signature = (name, r#type, nullable=true, *, metadata=None))]
    fn init(
        name: String,
        r#type: PyDataType,
        nullable: bool,
        metadata: Option<MetadataInput>,
    ) -> PyResult<Self> {
        let field = Field::new(name, r#type.into_inner(), nullable)
            .with_metadata(metadata.unwrap_or_default().into_string_hashmap()?);
        Ok(PyField::new(field.into()))
    }

    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.0.as_ref())
    }

    fn __eq__(&self, other: &PyField) -> bool {
        self.0 == other.0
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: Self) -> Self {
        input
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(_cls: &Bound<PyType>, capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    fn equals(&self, other: PyField) -> bool {
        self.0 == other.0
    }

    // Note: we can't return HashMap<Vec<u8>, Vec<u8>> because that will coerce keys and values to
    // a list, not bytes
    #[getter]
    fn metadata<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new_bound(py);
        self.0.metadata().iter().try_for_each(|(key, val)| {
            d.set_item(
                PyBytes::new_bound(py, key.as_bytes()),
                PyBytes::new_bound(py, val.as_bytes()),
            )
        })?;
        Ok(d)
    }

    #[getter]
    fn metadata_str(&self) -> HashMap<String, String> {
        self.0.metadata().clone()
    }

    #[getter]
    fn name(&self) -> String {
        self.0.name().clone()
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.0.is_nullable()
    }

    fn remove_metadata(&self, py: Python) -> PyResult<PyObject> {
        PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_metadata(Default::default())
                .into(),
        )
        .to_arro3(py)
    }

    #[getter]
    fn r#type(&self, py: Python) -> PyResult<PyObject> {
        PyDataType::new(self.0.data_type().clone()).to_arro3(py)
    }

    fn with_metadata(&self, py: Python, metadata: MetadataInput) -> PyResult<PyObject> {
        PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_metadata(metadata.into_string_hashmap()?)
                .into(),
        )
        .to_arro3(py)
    }

    fn with_name(&self, py: Python, name: String) -> PyResult<PyObject> {
        PyField::new(self.0.as_ref().clone().with_name(name).into()).to_arro3(py)
    }

    fn with_nullable(&self, py: Python, nullable: bool) -> PyResult<PyObject> {
        PyField::new(self.0.as_ref().clone().with_nullable(nullable).into()).to_arro3(py)
    }

    fn with_type(&self, py: Python, new_type: PyDataType) -> PyResult<PyObject> {
        PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_data_type(new_type.into_inner())
                .into(),
        )
        .to_arro3(py)
    }
}
