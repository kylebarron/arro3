use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::PyArrowResult;
use crate::export::{Arro3DataType, Arro3Field, Arro3Schema, Arro3Table};
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_python::to_schema_pycapsule;
use crate::input::{FieldIndexInput, MetadataInput};
use crate::{PyDataType, PyField, PyTable};

/// A Python-facing Arrow schema.
///
/// This is a wrapper around a [SchemaRef].
#[derive(Debug)]
#[pyclass(module = "arro3.core._core", name = "Schema", subclass, frozen)]
pub struct PySchema(SchemaRef);

impl PySchema {
    /// Construct a new PySchema
    pub fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }

    /// Construct from a raw Arrow C Schema capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let schema =
            Schema::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(schema)))
    }

    /// Consume this and return its internal [SchemaRef]
    pub fn into_inner(self) -> SchemaRef {
        self.0
    }

    /// Export this to a Python `arro3.core.Schema`.
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Schema"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![self.__arrow_c_schema__(py)?])?,
        )
    }

    /// Export this to a Python `arro3.core.Schema`.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = to_schema_pycapsule(py, self.0.as_ref())?;
        arro3_mod.getattr(intern!(py, "Schema"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![capsule])?,
        )
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.Schema
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "schema"))?
            .call1(PyTuple::new(py, vec![self.into_pyobject(py)?])?)?;
        pyarrow_obj.into_py_any(py)
    }
}

impl From<PySchema> for SchemaRef {
    fn from(value: PySchema) -> Self {
        value.0
    }
}

impl From<&PySchema> for SchemaRef {
    fn from(value: &PySchema) -> Self {
        value.0.as_ref().clone().into()
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
    #[new]
    #[pyo3(signature = (fields, *, metadata=None))]
    fn init(fields: Vec<PyField>, metadata: Option<MetadataInput>) -> PyResult<Self> {
        let fields = fields
            .into_iter()
            .map(|field| field.into_inner())
            .collect::<Vec<_>>();
        let schema = PySchema::new(
            Schema::new_with_metadata(fields, metadata.unwrap_or_default().into_string_hashmap()?)
                .into(),
        );
        Ok(schema)
    }

    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.0.as_ref())
    }

    fn __eq__(&self, other: &PySchema) -> bool {
        self.0 == other.0
    }

    fn __getitem__(&self, key: FieldIndexInput) -> PyArrowResult<Arro3Field> {
        self.field(key)
    }

    fn __len__(&self) -> usize {
        self.0.fields().len()
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

    fn append(&self, field: PyField) -> Arro3Schema {
        let mut fields = self.0.fields().to_vec();
        fields.push(field.into_inner());
        Schema::new_with_metadata(fields, self.0.metadata().clone()).into()
    }

    fn empty_table(&self) -> PyResult<Arro3Table> {
        Ok(PyTable::try_new(vec![], self.into())?.into())
    }

    fn equals(&self, other: PySchema) -> bool {
        self.0 == other.0
    }

    fn field(&self, i: FieldIndexInput) -> PyArrowResult<Arro3Field> {
        let index = i.into_position(&self.0)?;
        Ok(self.0.field(index).into())
    }

    fn get_all_field_indices(&self, name: String) -> Vec<usize> {
        let mut indices = self
            .0
            .fields()
            .iter()
            .enumerate()
            .filter(|(_idx, field)| field.name() == name.as_str())
            .map(|(idx, _field)| idx)
            .collect::<Vec<_>>();
        indices.sort();
        indices
    }

    fn get_field_index(&self, name: String) -> PyArrowResult<usize> {
        let indices = self
            .0
            .fields()
            .iter()
            .enumerate()
            .filter(|(_idx, field)| field.name() == name.as_str())
            .map(|(idx, _field)| idx)
            .collect::<Vec<_>>();
        if indices.len() == 1 {
            Ok(indices[0])
        } else {
            Err(PyValueError::new_err("Multiple fields with given name").into())
        }
    }

    fn insert(&self, i: usize, field: PyField) -> Arro3Schema {
        let mut fields = self.0.fields().to_vec();
        fields.insert(i, field.into_inner());
        Schema::new_with_metadata(fields, self.0.metadata().clone()).into()
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
    fn names(&self) -> Vec<String> {
        self.0.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn remove(&self, i: usize) -> Arro3Schema {
        let mut fields = self.0.fields().to_vec();
        fields.remove(i);
        Schema::new_with_metadata(fields, self.0.metadata().clone()).into()
    }

    fn remove_metadata(&self) -> Arro3Schema {
        self.0
            .as_ref()
            .clone()
            .with_metadata(Default::default())
            .into()
    }

    fn set(&self, i: usize, field: PyField) -> Arro3Schema {
        let mut fields = self.0.fields().to_vec();
        fields[i] = field.into_inner();
        Schema::new_with_metadata(fields, self.0.metadata().clone()).into()
    }

    #[getter]
    fn types(&self) -> Vec<Arro3DataType> {
        self.0
            .fields()
            .iter()
            .map(|f| PyDataType::new(f.data_type().clone()).into())
            .collect()
    }

    fn with_metadata(&self, metadata: MetadataInput) -> PyResult<Arro3Schema> {
        let schema = self
            .0
            .as_ref()
            .clone()
            .with_metadata(metadata.into_string_hashmap()?);
        Ok(schema.into())
    }
}
