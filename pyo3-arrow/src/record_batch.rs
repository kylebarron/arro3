use std::fmt::Display;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow_cast::pretty::pretty_format_batches_with_options;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use indexmap::IndexMap;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::PyArrowResult;
use crate::export::{Arro3Array, Arro3Field, Arro3RecordBatch, Arro3Schema};
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array;
use crate::ffi::to_python::to_array_pycapsules;
use crate::ffi::to_schema_pycapsule;
use crate::input::{AnyRecordBatch, FieldIndexInput, MetadataInput, NameOrField, SelectIndices};
use crate::utils::default_repr_options;
use crate::{PyArray, PyField, PySchema};

/// A Python-facing Arrow record batch.
///
/// This is a wrapper around a [RecordBatch].
#[pyclass(module = "arro3.core._core", name = "RecordBatch", subclass, frozen)]
#[derive(Debug)]
pub struct PyRecordBatch(RecordBatch);

impl PyRecordBatch {
    /// Construct a new PyRecordBatch from a [RecordBatch].
    pub fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }

    /// Construct from raw Arrow capsules
    pub fn from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let (array, field) = import_array_pycapsules(schema_capsule, array_capsule)?;

        match field.data_type() {
            DataType::Struct(fields) => {
                let struct_array = array.as_struct();
                let schema = SchemaBuilder::from(fields)
                    .finish()
                    .with_metadata(field.metadata().clone());
                assert_eq!(
                    struct_array.null_count(),
                    0,
                    "Cannot convert nullable StructArray to RecordBatch"
                );

                let columns = struct_array.columns().to_vec();

                let batch = RecordBatch::try_new_with_options(
                    Arc::new(schema),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(array.len())),
                )
                .map_err(|err| PyValueError::new_err(err.to_string()))?;
                Ok(Self::new(batch))
            }
            dt => Err(PyValueError::new_err(format!(
                "Unexpected data type {}",
                dt
            ))),
        }
    }

    /// Consume this, returning its internal [RecordBatch].
    pub fn into_inner(self) -> RecordBatch {
        self.0
    }

    /// Export this to a Python `arro3.core.RecordBatch`.
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "RecordBatch"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            self.__arrow_c_array__(py, None)?,
        )
    }

    /// Export this to a Python `arro3.core.RecordBatch`.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsules = Self::to_array_pycapsules(py, self.0.clone(), None)?;
        arro3_mod
            .getattr(intern!(py, "RecordBatch"))?
            .call_method1(intern!(py, "from_arrow_pycapsule"), capsules)
    }

    /// Export this to a Python `nanoarrow.Array`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_array(py, self.__arrow_c_array__(py, None)?)
    }

    /// Export to a pyarrow.RecordBatch
    ///
    /// Requires pyarrow >=14
    pub fn into_pyarrow(self, py: Python) -> PyResult<Bound<PyAny>> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        pyarrow_mod
            .getattr(intern!(py, "record_batch"))?
            .call1(PyTuple::new(py, vec![self.into_pyobject(py)?])?)
    }

    pub(crate) fn to_array_pycapsules<'py>(
        py: Python<'py>,
        record_batch: RecordBatch,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyTuple>> {
        let field = Field::new_struct("", record_batch.schema_ref().fields().clone(), false);
        let array: ArrayRef = Arc::new(StructArray::from(record_batch.clone()));
        to_array_pycapsules(py, field.into(), &array, requested_schema)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self(value)
    }
}

impl From<PyRecordBatch> for RecordBatch {
    fn from(value: PyRecordBatch) -> Self {
        value.0
    }
}

impl AsRef<RecordBatch> for PyRecordBatch {
    fn as_ref(&self) -> &RecordBatch {
        &self.0
    }
}

impl Display for PyRecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.RecordBatch")?;
        pretty_format_batches_with_options(
            &[self.0.slice(0, 10.min(self.0.num_rows()))],
            &default_repr_options(),
        )
        .map_err(|_| std::fmt::Error)?
        .fmt(f)?;

        Ok(())
    }
}

#[pymethods]
impl PyRecordBatch {
    #[new]
    #[pyo3(signature = (data, *,  schema=None, metadata=None))]
    fn init(
        py: Python,
        data: &Bound<PyAny>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyArrowResult<Self> {
        if data.hasattr(intern!(py, "__arrow_c_array__"))? {
            Ok(data.extract::<PyRecordBatch>()?)
        } else if let Ok(mapping) = data.extract::<IndexMap<String, PyArray>>() {
            Self::from_pydict(&py.get_type::<PyRecordBatch>(), mapping, metadata)
        } else if let Ok(arrays) = data.extract::<Vec<PyArray>>() {
            Self::from_arrays(
                &py.get_type::<PyRecordBatch>(),
                arrays,
                schema.ok_or(PyValueError::new_err(
                    "Schema must be passed with list of arrays",
                ))?,
            )
        } else {
            Err(PyTypeError::new_err(
                "Expected RecordBatch-like input or dict of arrays or list of arrays.",
            )
            .into())
        }
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyTuple>> {
        Self::to_array_pycapsules(py, self.0.clone(), requested_schema)
    }

    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.0.schema_ref().as_ref())
    }

    fn __eq__(&self, other: &PyRecordBatch) -> bool {
        self.0 == other.0
    }

    fn __getitem__(&self, key: FieldIndexInput) -> PyResult<Arro3Array> {
        self.column(key)
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    #[pyo3(signature = (arrays, *, schema))]
    fn from_arrays(
        _cls: &Bound<PyType>,
        arrays: Vec<PyArray>,
        schema: PySchema,
    ) -> PyArrowResult<Self> {
        let rb = RecordBatch::try_new(
            schema.into(),
            arrays
                .into_iter()
                .map(|arr| {
                    let (arr, _field) = arr.into_inner();
                    arr
                })
                .collect(),
        )?;
        Ok(Self::new(rb))
    }

    #[classmethod]
    #[pyo3(signature = (mapping, *, metadata=None))]
    fn from_pydict(
        _cls: &Bound<PyType>,
        mapping: IndexMap<String, PyArray>,
        metadata: Option<MetadataInput>,
    ) -> PyArrowResult<Self> {
        let mut fields = vec![];
        let mut arrays = vec![];
        mapping.into_iter().for_each(|(name, py_array)| {
            let (arr, field) = py_array.into_inner();
            fields.push(field.as_ref().clone().with_name(name));
            arrays.push(arr);
        });
        let schema =
            Schema::new_with_metadata(fields, metadata.unwrap_or_default().into_string_hashmap()?);
        let rb = RecordBatch::try_new(schema.into(), arrays)?;
        Ok(Self::new(rb))
    }

    #[classmethod]
    fn from_struct_array(_cls: &Bound<PyType>, struct_array: PyArray) -> PyArrowResult<Self> {
        let (array, field) = struct_array.into_inner();
        match field.data_type() {
            DataType::Struct(fields) => {
                let schema = Schema::new_with_metadata(fields.clone(), field.metadata().clone());
                let struct_arr = array.as_struct();
                let columns = struct_arr.columns().to_vec();
                let rb = RecordBatch::try_new(schema.into(), columns)?;
                Ok(Self::new(rb))
            }
            _ => Err(PyTypeError::new_err("Expected struct array").into()),
        }
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyRecordBatch) -> PyArrowResult<Self> {
        match input {
            AnyRecordBatch::RecordBatch(rb) => Ok(rb),
            AnyRecordBatch::Stream(stream) => {
                let (batches, schema) = stream.into_table()?.into_inner();
                let single_batch = concat_batches(&schema, batches.iter())?;
                Ok(Self::new(single_batch))
            }
        }
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        Self::from_arrow_pycapsule(schema_capsule, array_capsule)
    }

    fn add_column(
        &self,
        i: usize,
        field: NameOrField,
        column: PyArray,
    ) -> PyArrowResult<Arro3RecordBatch> {
        let mut fields = self.0.schema_ref().fields().to_vec();
        fields.insert(i, field.into_field(column.field()));
        let schema = Schema::new_with_metadata(fields, self.0.schema_ref().metadata().clone());

        let mut arrays = self.0.columns().to_vec();
        arrays.insert(i, column.array().clone());

        let new_rb = RecordBatch::try_new(schema.into(), arrays)?;
        Ok(PyRecordBatch::new(new_rb).into())
    }

    fn append_column(
        &self,
        field: NameOrField,
        column: PyArray,
    ) -> PyArrowResult<Arro3RecordBatch> {
        let mut fields = self.0.schema_ref().fields().to_vec();
        fields.push(field.into_field(column.field()));
        let schema = Schema::new_with_metadata(fields, self.0.schema_ref().metadata().clone());

        let mut arrays = self.0.columns().to_vec();
        arrays.push(column.array().clone());

        let new_rb = RecordBatch::try_new(schema.into(), arrays)?;
        Ok(PyRecordBatch::new(new_rb).into())
    }

    fn column(&self, i: FieldIndexInput) -> PyResult<Arro3Array> {
        let column_index = i.into_position(self.0.schema_ref())?;
        let field = self.0.schema().field(column_index).clone();
        let array = self.0.column(column_index).clone();
        Ok(PyArray::new(array, field.into()).into())
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.0
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<Arro3Array>> {
        (0..self.num_columns())
            .map(|i| self.column(FieldIndexInput::Position(i)))
            .collect()
    }

    fn equals(&self, other: PyRecordBatch) -> bool {
        self.0 == other.0
    }

    fn field(&self, i: FieldIndexInput) -> PyResult<Arro3Field> {
        let schema_ref = self.0.schema_ref();
        let field = schema_ref.field(i.into_position(schema_ref)?);
        Ok(PyField::new(field.clone().into()).into())
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.0.get_array_memory_size()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    fn remove_column(&self, i: usize) -> Arro3RecordBatch {
        let mut rb = self.0.clone();
        rb.remove_column(i);
        PyRecordBatch::new(rb).into()
    }

    #[getter]
    fn schema(&self) -> Arro3Schema {
        self.0.schema().into()
    }

    fn select(&self, columns: SelectIndices) -> PyArrowResult<Arro3RecordBatch> {
        let positions = columns.into_positions(self.0.schema_ref().fields())?;
        Ok(self.0.project(&positions)?.into())
    }

    fn set_column(
        &self,
        i: usize,
        field: NameOrField,
        column: PyArray,
    ) -> PyArrowResult<Arro3RecordBatch> {
        let mut fields = self.0.schema_ref().fields().to_vec();
        fields[i] = field.into_field(column.field());
        let schema = Schema::new_with_metadata(fields, self.0.schema_ref().metadata().clone());

        let mut arrays = self.0.columns().to_vec();
        arrays[i] = column.array().clone();

        Ok(RecordBatch::try_new(schema.into(), arrays)?.into())
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        (self.num_rows(), self.num_columns())
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Arro3RecordBatch {
        let length = length.unwrap_or_else(|| self.num_rows() - offset);
        self.0.slice(offset, length).into()
    }

    fn take(&self, indices: PyArray) -> PyArrowResult<Arro3RecordBatch> {
        let new_batch = take_record_batch(self.as_ref(), indices.as_ref())?;
        Ok(new_batch.into())
    }

    fn to_struct_array(&self) -> Arro3Array {
        let struct_array: StructArray = self.0.clone().into();
        let field = Field::new_struct("", self.0.schema_ref().fields().clone(), false)
            .with_metadata(self.0.schema_ref().metadata.clone());
        PyArray::new(Arc::new(struct_array), field.into()).into()
    }

    fn with_schema(&self, schema: PySchema) -> PyArrowResult<Arro3RecordBatch> {
        let new_schema = schema.into_inner();
        let new_batch = RecordBatch::try_new(new_schema.clone(), self.0.columns().to_vec())?;
        Ok(new_batch.into())
    }
}
