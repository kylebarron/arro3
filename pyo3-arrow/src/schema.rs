use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_python::to_schema_pycapsule;
use crate::input::{FieldIndexInput, MetadataInput};
use crate::{PyDataType, PyField, PyTable};

/// A Python-facing Arrow schema.
///
/// This is a wrapper around a [SchemaRef].
#[pyclass(module = "arro3.core._rust", name = "Schema", subclass)]
pub struct PySchema(SchemaRef);

impl PySchema {
    pub fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }

    pub fn into_inner(self) -> SchemaRef {
        self.0
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

    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this array
    /// into a pyarrow schema, without copying memory.
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.0.as_ref())
    }

    pub fn __eq__(&self, other: &PySchema) -> bool {
        self.0 == other.0
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

    fn append(&self, py: Python, field: PyField) -> PyResult<PyObject> {
        let mut fields = self.0.fields().to_vec();
        fields.push(field.into_inner());
        let schema = Schema::new_with_metadata(fields, self.0.metadata().clone());
        PySchema::new(schema.into()).to_arro3(py)
    }

    fn empty_table(&self, py: Python) -> PyResult<PyObject> {
        PyTable::new(vec![], self.into()).to_arro3(py)
    }

    fn equals(&self, other: PySchema) -> bool {
        self.0 == other.0
    }

    /// Select a field by its column name or numeric index.
    fn field(&self, py: Python, i: FieldIndexInput) -> PyArrowResult<PyObject> {
        let field = match i {
            FieldIndexInput::String(name) => self.0.field_with_name(&name)?,
            FieldIndexInput::Int(i) => self.0.field(i),
        };
        Ok(PyField::new(field.clone().into()).to_arro3(py)?)
    }

    /// Return sorted list of indices for the fields with the given name.
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

    /// Return index of the unique field with the given name.
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

    fn insert(&self, py: Python, i: usize, field: PyField) -> PyResult<PyObject> {
        let mut fields = self.0.fields().to_vec();
        fields.insert(i, field.into_inner());
        let schema = Schema::new_with_metadata(fields, self.0.metadata().clone());
        PySchema::new(schema.into()).to_arro3(py)
    }

    /// The schema's metadata.
    #[getter]
    fn metadata(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut new_metadata = HashMap::with_capacity(self.0.metadata.len());
        self.0.metadata().iter().for_each(|(key, val)| {
            new_metadata.insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
        });
        new_metadata
    }

    /// The schema's metadata where keys and values are `str`, not `bytes`.
    #[getter]
    fn metadata_str(&self) -> HashMap<String, String> {
        self.0.metadata().clone()
    }

    /// The schema’s field names.
    #[getter]
    fn names(&self) -> Vec<String> {
        self.0.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn remove(&self, py: Python, i: usize) -> PyResult<PyObject> {
        let mut fields = self.0.fields().to_vec();
        fields.remove(i);
        let schema = Schema::new_with_metadata(fields, self.0.metadata().clone());
        PySchema::new(schema.into()).to_arro3(py)
    }

    fn remove_metadata(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(
            self.0
                .as_ref()
                .clone()
                .with_metadata(Default::default())
                .into(),
        )
        .to_arro3(py)
    }

    fn set(&self, py: Python, i: usize, field: PyField) -> PyResult<PyObject> {
        let mut fields = self.0.fields().to_vec();
        fields[i] = field.into_inner();
        let schema = Schema::new_with_metadata(fields, self.0.metadata().clone());
        PySchema::new(schema.into()).to_arro3(py)
    }

    /// The schema’s field types.
    #[getter]
    fn types(&self, py: Python) -> PyArrowResult<Vec<PyObject>> {
        Ok(self
            .0
            .fields()
            .iter()
            .map(|f| PyDataType::new(f.data_type().clone()).to_arro3(py))
            .collect::<PyResult<_>>()?)
    }

    fn with_metadata(&self, py: Python, metadata: MetadataInput) -> PyResult<PyObject> {
        let schema = self
            .0
            .as_ref()
            .clone()
            .with_metadata(metadata.into_string_hashmap()?);
        PySchema::new(schema.into()).to_arro3(py)
    }
}
