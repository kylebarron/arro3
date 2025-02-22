use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow_schema::{Field, FieldRef};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::export::{Arro3DataType, Arro3Field};
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_python::to_schema_pycapsule;
use crate::input::MetadataInput;
use crate::PyDataType;

/// A Python-facing Arrow field.
///
/// This is a wrapper around a [FieldRef].
#[derive(Debug)]
#[pyclass(module = "arro3.core._core", name = "Field", subclass, frozen)]
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
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Field"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![self.__arrow_c_schema__(py)?])?,
        )
    }

    /// Export this to a Python `arro3.core.Field`.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = to_schema_pycapsule(py, self.0.as_ref())?;
        arro3_mod.getattr(intern!(py, "Field"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![capsule])?,
        )
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.Field
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let cloned = PyField::new(self.0.clone());
        pyarrow_mod
            .getattr(intern!(py, "field"))?
            .call1(PyTuple::new(py, vec![cloned.into_pyobject(py)?])?)
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
        let d = PyDict::new(py);
        self.0.metadata().iter().try_for_each(|(key, val)| {
            d.set_item(
                PyBytes::new(py, key.as_bytes()),
                PyBytes::new(py, val.as_bytes()),
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

    fn remove_metadata(&self) -> Arro3Field {
        PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_metadata(Default::default())
                .into(),
        )
        .into()
    }

    #[getter]
    fn r#type(&self) -> Arro3DataType {
        PyDataType::new(self.0.data_type().clone()).into()
    }

    fn with_metadata(&self, metadata: MetadataInput) -> PyResult<Arro3Field> {
        Ok(PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_metadata(metadata.into_string_hashmap()?)
                .into(),
        )
        .into())
    }

    fn with_name(&self, name: String) -> Arro3Field {
        PyField::new(self.0.as_ref().clone().with_name(name).into()).into()
    }

    fn with_nullable(&self, nullable: bool) -> Arro3Field {
        PyField::new(self.0.as_ref().clone().with_nullable(nullable).into()).into()
    }

    fn with_type(&self, new_type: PyDataType) -> Arro3Field {
        PyField::new(
            self.0
                .as_ref()
                .clone()
                .with_data_type(new_type.into_inner())
                .into(),
        )
        .into()
    }
}
