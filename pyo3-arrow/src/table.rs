use std::fmt::Display;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow_array::{ArrayRef, RecordBatchReader, StructArray};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use indexmap::IndexMap;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::{PyArrowError, PyArrowResult};
use crate::export::{
    Arro3ChunkedArray, Arro3Field, Arro3RecordBatch, Arro3RecordBatchReader, Arro3Schema,
    Arro3Table,
};
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::to_schema_pycapsule;
use crate::input::{
    AnyArray, AnyRecordBatch, FieldIndexInput, MetadataInput, NameOrField, SelectIndices,
};
use crate::schema::display_schema;
use crate::utils::schema_equals;
use crate::{PyChunkedArray, PyField, PyRecordBatch, PyRecordBatchReader, PySchema};

/// A Python-facing Arrow table.
///
/// This is a wrapper around a [SchemaRef] and a `Vec` of [RecordBatch].
#[pyclass(module = "arro3.core._core", name = "Table", subclass)]
#[derive(Debug)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    /// Create a new table from batches and a schema.
    pub fn try_new(batches: Vec<RecordBatch>, schema: SchemaRef) -> PyResult<Self> {
        if !batches
            .iter()
            .all(|rb| schema_equals(rb.schema_ref(), &schema))
        {
            return Err(PyTypeError::new_err("All batches must have same schema"));
        }

        Ok(Self { schema, batches })
    }

    /// Construct from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowRecordBatchStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        Self::try_new(batches, schema)
    }

    /// Access the underlying batches
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Consume this and return its internal batches and schema.
    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }

    /// Export this to a Python `arro3.core.Table`.
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Table"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![self.__arrow_c_stream__(py, None)?])?,
        )
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.Table
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "table"))?
            .call1(PyTuple::new(py, vec![self.into_pyobject(py)?])?)?;
        pyarrow_obj.into_py_any(py)
    }

    pub(crate) fn to_stream_pycapsule<'py>(
        py: Python<'py>,
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let field = schema.fields();
        let array_reader = batches.into_iter().map(|batch| {
            let arr: ArrayRef = Arc::new(StructArray::from(batch));
            Ok(arr)
        });
        let array_reader = Box::new(ArrayIterator::new(
            array_reader,
            Field::new_struct("", field.clone(), false)
                .with_metadata(schema.metadata.clone())
                .into(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    pub(crate) fn rechunk(&self, chunk_lengths: Vec<usize>) -> PyArrowResult<Self> {
        let total_chunk_length = chunk_lengths.iter().sum::<usize>();
        if total_chunk_length != self.num_rows() {
            return Err(
                PyValueError::new_err("Chunk lengths do not add up to table length").into(),
            );
        }

        // If the desired rechunking is the existing chunking, return early
        let matches_existing_chunking = chunk_lengths
            .iter()
            .zip(self.batches())
            .all(|(length, batch)| *length == batch.num_rows());
        if matches_existing_chunking {
            return Ok(Self::try_new(self.batches.clone(), self.schema.clone())?);
        }

        let mut offset = 0;
        let batches = chunk_lengths
            .iter()
            .map(|chunk_length| {
                let sliced_table = self.slice(offset, *chunk_length)?;
                let sliced_concatted = concat_batches(&self.schema, sliced_table.batches.iter())?;
                offset += chunk_length;
                Ok(sliced_concatted)
            })
            .collect::<PyArrowResult<Vec<_>>>()?;

        Ok(Self::try_new(batches, self.schema.clone())?)
    }

    pub(crate) fn slice(&self, mut offset: usize, mut length: usize) -> PyArrowResult<Self> {
        if offset + length > self.num_rows() {
            return Err(
                PyValueError::new_err("offset + length may not exceed length of array").into(),
            );
        }

        let mut sliced_batches: Vec<RecordBatch> = vec![];
        for chunk in self.batches() {
            if chunk.num_rows() == 0 {
                continue;
            }

            // If the offset is greater than the len of this chunk, don't include any rows from
            // this chunk
            if offset >= chunk.num_rows() {
                offset -= chunk.num_rows();
                continue;
            }

            let take_count = length.min(chunk.num_rows() - offset);
            let sliced_chunk = chunk.slice(offset, take_count);
            sliced_batches.push(sliced_chunk);

            length -= take_count;

            // If we've selected all rows, exit
            if length == 0 {
                break;
            } else {
                offset = 0;
            }
        }

        Ok(Self::try_new(sliced_batches, self.schema.clone())?)
    }
}

impl Display for PyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.Table")?;
        writeln!(f, "-----------")?;
        display_schema(&self.schema, f)
    }
}

#[pymethods]
impl PyTable {
    #[new]
    #[pyo3(signature = (data, *, names=None, schema=None, metadata=None))]
    fn new(
        py: Python,
        data: &Bound<PyAny>,
        names: Option<Vec<String>>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyArrowResult<Self> {
        if let Ok(data) = data.extract::<AnyRecordBatch>() {
            Ok(data.into_table()?)
        } else if let Ok(mapping) = data.extract::<IndexMap<String, AnyArray>>() {
            Self::from_pydict(&py.get_type::<PyTable>(), mapping, schema, metadata)
        } else if let Ok(arrays) = data.extract::<Vec<AnyArray>>() {
            Self::from_arrays(&py.get_type::<PyTable>(), arrays, names, schema, metadata)
        } else {
            Err(PyTypeError::new_err(
                "Expected Table-like input or dict of arrays or sequence of arrays.",
            )
            .into())
        }
    }

    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.schema.as_ref())
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        Self::to_stream_pycapsule(
            py,
            self.batches.clone(),
            self.schema.clone(),
            requested_schema,
        )
    }

    fn __eq__(&self, other: &PyTable) -> bool {
        self.batches == other.batches && self.schema == other.schema
    }

    fn __getitem__(&self, key: FieldIndexInput) -> PyArrowResult<Arro3ChunkedArray> {
        self.column(key)
    }

    fn __len__(&self) -> usize {
        self.batches.iter().fold(0, |acc, x| acc + x.num_rows())
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyRecordBatch) -> PyArrowResult<Self> {
        input.into_table()
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(_cls: &Bound<PyType>, capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[classmethod]
    #[pyo3(signature = (batches, *, schema=None))]
    fn from_batches(
        _cls: &Bound<PyType>,
        batches: Vec<PyRecordBatch>,
        schema: Option<PySchema>,
    ) -> PyArrowResult<Self> {
        if batches.is_empty() {
            let schema = schema.ok_or(PyValueError::new_err(
                "schema must be passed for an empty list of batches",
            ))?;
            return Ok(Self::try_new(vec![], schema.into_inner())?);
        }

        let batches = batches
            .into_iter()
            .map(|batch| batch.into_inner())
            .collect::<Vec<_>>();
        let schema = schema
            .map(|s| s.into_inner())
            .unwrap_or(batches.first().unwrap().schema());
        Ok(Self::try_new(batches, schema)?)
    }

    #[classmethod]
    #[pyo3(signature = (mapping, *, schema=None, metadata=None))]
    fn from_pydict(
        cls: &Bound<PyType>,
        mapping: IndexMap<String, AnyArray>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyArrowResult<Self> {
        let (names, arrays): (Vec<_>, Vec<_>) = mapping.into_iter().unzip();
        Self::from_arrays(cls, arrays, Some(names), schema, metadata)
    }

    #[classmethod]
    #[pyo3(signature = (arrays, *, names=None, schema=None, metadata=None))]
    fn from_arrays(
        _cls: &Bound<PyType>,
        arrays: Vec<AnyArray>,
        names: Option<Vec<String>>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyArrowResult<Self> {
        let columns = arrays
            .into_iter()
            .map(|array| array.into_chunked_array())
            .collect::<PyArrowResult<Vec<_>>>()?;

        let schema: SchemaRef = if let Some(schema) = schema {
            schema.into_inner()
        } else {
            let names = names.ok_or(PyValueError::new_err(
                "names must be passed if schema is not passed.",
            ))?;

            let fields = columns
                .iter()
                .zip(names.iter())
                .map(|(array, name)| Arc::new(array.field().as_ref().clone().with_name(name)))
                .collect::<Vec<_>>();
            Arc::new(
                Schema::new(fields)
                    .with_metadata(metadata.unwrap_or_default().into_string_hashmap().unwrap()),
            )
        };

        if columns.is_empty() {
            return Ok(Self::try_new(vec![], schema)?);
        }

        let column_chunk_lengths = columns
            .iter()
            .map(|column| {
                let chunk_lengths = column
                    .chunks()
                    .iter()
                    .map(|chunk| chunk.len())
                    .collect::<Vec<_>>();
                chunk_lengths
            })
            .collect::<Vec<_>>();
        if !column_chunk_lengths.windows(2).all(|w| w[0] == w[1]) {
            return Err(
                PyValueError::new_err("All columns must have the same chunk lengths").into(),
            );
        }
        let num_batches = column_chunk_lengths[0].len();

        let mut batches = vec![];
        for batch_idx in 0..num_batches {
            let batch = RecordBatch::try_new(
                schema.clone(),
                columns
                    .iter()
                    .map(|column| column.chunks()[batch_idx].clone())
                    .collect(),
            )?;
            batches.push(batch);
        }

        Ok(Self::try_new(batches, schema)?)
    }

    fn add_column(
        &self,
        i: usize,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<Arro3Table> {
        if self.num_rows() != column.len() {
            return Err(
                PyValueError::new_err("Number of rows in column does not match table.").into(),
            );
        }

        let column = column.rechunk(self.chunk_lengths())?;

        let mut fields = self.schema.fields().to_vec();
        fields.insert(i, field.into_field(column.field()));
        let new_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));

        let new_batches = self
            .batches
            .iter()
            .zip(column.chunks())
            .map(|(batch, array)| {
                debug_assert_eq!(
                    array.len(),
                    batch.num_rows(),
                    "Array and batch should have same number of rows."
                );

                let mut columns = batch.columns().to_vec();
                columns.insert(i, array.clone());
                Ok(RecordBatch::try_new(new_schema.clone(), columns)?)
            })
            .collect::<Result<Vec<_>, PyArrowError>>()?;

        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }

    fn append_column(
        &self,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<Arro3Table> {
        if self.num_rows() != column.len() {
            return Err(
                PyValueError::new_err("Number of rows in column does not match table.").into(),
            );
        }

        let column = column.rechunk(self.chunk_lengths())?;

        let mut fields = self.schema.fields().to_vec();
        fields.push(field.into_field(column.field()));
        let new_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));

        let new_batches = self
            .batches
            .iter()
            .zip(column.chunks())
            .map(|(batch, array)| {
                debug_assert_eq!(
                    array.len(),
                    batch.num_rows(),
                    "Array and batch should have same number of rows."
                );

                let mut columns = batch.columns().to_vec();
                columns.push(array.clone());
                Ok(RecordBatch::try_new(new_schema.clone(), columns)?)
            })
            .collect::<Result<Vec<_>, PyArrowError>>()?;

        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }

    #[getter]
    fn chunk_lengths(&self) -> Vec<usize> {
        self.batches.iter().map(|batch| batch.num_rows()).collect()
    }

    fn column(&self, i: FieldIndexInput) -> PyArrowResult<Arro3ChunkedArray> {
        let column_index = i.into_position(&self.schema)?;
        let field = self.schema.field(column_index).clone();
        let chunks = self
            .batches
            .iter()
            .map(|batch| batch.column(column_index).clone())
            .collect();
        Ok(PyChunkedArray::try_new(chunks, field.into())?.into())
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[getter]
    fn columns(&self) -> PyArrowResult<Vec<Arro3ChunkedArray>> {
        (0..self.num_columns())
            .map(|i| self.column(FieldIndexInput::Position(i)))
            .collect()
    }

    fn combine_chunks(&self) -> PyArrowResult<Arro3Table> {
        let batch = concat_batches(&self.schema, &self.batches)?;
        Ok(PyTable::try_new(vec![batch], self.schema.clone())?.into())
    }

    fn field(&self, i: FieldIndexInput) -> PyArrowResult<Arro3Field> {
        let field = self.schema.field(i.into_position(&self.schema)?);
        Ok(PyField::new(field.clone().into()).into())
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.batches
            .iter()
            .fold(0, |acc, batch| acc + batch.get_array_memory_size())
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.batches()
            .iter()
            .fold(0, |acc, batch| acc + batch.num_rows())
    }

    #[pyo3(signature = (*, max_chunksize=None))]
    #[pyo3(name = "rechunk")]
    fn rechunk_py(&self, max_chunksize: Option<usize>) -> PyArrowResult<Arro3Table> {
        let max_chunksize = max_chunksize.unwrap_or(self.num_rows());
        if max_chunksize == 0 {
            return Err(PyValueError::new_err("max_chunksize must be > 0").into());
        }

        let mut chunk_lengths = vec![];
        let mut offset = 0;
        while offset < self.num_rows() {
            let chunk_length = max_chunksize.min(self.num_rows() - offset);
            offset += chunk_length;
            chunk_lengths.push(chunk_length);
        }
        Ok(self.rechunk(chunk_lengths)?.into())
    }

    fn remove_column(&self, i: usize) -> PyArrowResult<Arro3Table> {
        let mut fields = self.schema.fields().to_vec();
        fields.remove(i);
        let new_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));

        let new_batches = self
            .batches
            .iter()
            .map(|batch| {
                let mut columns = batch.columns().to_vec();
                columns.remove(i);
                Ok(RecordBatch::try_new(new_schema.clone(), columns)?)
            })
            .collect::<Result<Vec<_>, PyArrowError>>()?;

        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }

    fn rename_columns(&self, names: Vec<String>) -> PyArrowResult<Arro3Table> {
        if names.len() != self.num_columns() {
            return Err(PyValueError::new_err("When names is a list[str], must pass the same number of names as there are columns.").into());
        }

        let new_fields = self
            .schema
            .fields()
            .iter()
            .zip(names)
            .map(|(field, name)| field.as_ref().clone().with_name(name))
            .collect::<Vec<_>>();
        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            self.schema.metadata().clone(),
        ));
        Ok(PyTable::try_new(self.batches.clone(), new_schema)?.into())
    }

    #[getter]
    fn schema(&self) -> Arro3Schema {
        PySchema::new(self.schema.clone()).into()
    }

    fn select(&self, columns: SelectIndices) -> PyArrowResult<Arro3Table> {
        let positions = columns.into_positions(self.schema.fields())?;

        let new_schema = Arc::new(self.schema.project(&positions)?);
        let new_batches = self
            .batches
            .iter()
            .map(|batch| batch.project(&positions))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }

    fn set_column(
        &self,
        i: usize,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<Arro3Table> {
        if self.num_rows() != column.len() {
            return Err(
                PyValueError::new_err("Number of rows in column does not match table.").into(),
            );
        }

        let column = column.rechunk(self.chunk_lengths())?;

        let mut fields = self.schema.fields().to_vec();
        fields[i] = field.into_field(column.field());
        let new_schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ));

        let new_batches = self
            .batches
            .iter()
            .zip(column.chunks())
            .map(|(batch, array)| {
                debug_assert_eq!(
                    array.len(),
                    batch.num_rows(),
                    "Array and batch should have same number of rows."
                );

                let mut columns = batch.columns().to_vec();
                columns[i] = array.clone();
                Ok(RecordBatch::try_new(new_schema.clone(), columns)?)
            })
            .collect::<Result<Vec<_>, PyArrowError>>()?;

        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }

    #[getter]
    fn shape(&self) -> (usize, usize) {
        (self.num_rows(), self.num_columns())
    }

    #[pyo3(signature = (offset=0, length=None))]
    #[pyo3(name = "slice")]
    fn slice_py(&self, offset: usize, length: Option<usize>) -> PyArrowResult<Arro3Table> {
        let length = length.unwrap_or_else(|| self.num_rows() - offset);
        Ok(self.slice(offset, length)?.into())
    }

    fn to_batches(&self) -> Vec<Arro3RecordBatch> {
        self.batches
            .iter()
            .map(|batch| PyRecordBatch::new(batch.clone()).into())
            .collect()
    }

    fn to_reader(&self) -> Arro3RecordBatchReader {
        let reader = Box::new(RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        ));
        PyRecordBatchReader::new(reader).into()
    }

    fn to_struct_array(&self) -> PyArrowResult<Arro3ChunkedArray> {
        let chunks = self
            .batches
            .iter()
            .map(|batch| {
                let struct_array: StructArray = batch.clone().into();
                Arc::new(struct_array) as ArrayRef
            })
            .collect::<Vec<_>>();
        let field = Field::new_struct("", self.schema.fields().clone(), false)
            .with_metadata(self.schema.metadata.clone());
        Ok(PyChunkedArray::try_new(chunks, field.into())?.into())
    }

    fn with_schema(&self, schema: PySchema) -> PyArrowResult<Arro3Table> {
        let new_schema = schema.into_inner();
        let new_batches = self
            .batches
            .iter()
            .map(|batch| RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec()))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(PyTable::try_new(new_batches, new_schema)?.into())
    }
}
