use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow_array::{ArrayRef, RecordBatchReader, StructArray};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::{PyArrowError, PyArrowResult};
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::input::{AnyArray, FieldIndexInput, MetadataInput, NameOrField, SelectIndices};
use crate::schema::display_schema;
use crate::{PyChunkedArray, PyField, PyRecordBatch, PyRecordBatchReader, PySchema};

/// A Python-facing Arrow table.
///
/// This is a wrapper around a [SchemaRef] and a `Vec` of [RecordBatch].
#[pyclass(module = "arro3.core._rust", name = "Table", subclass)]
#[derive(Debug)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        // TODO: allow batches to have different schema metadata?
        assert!(
            batches.iter().all(|rb| rb.schema_ref() == &schema),
            "All batches must have same schema"
        );
        Self { schema, batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }

    /// Export this to a Python `arro3.core.Table`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Table"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.Table
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "table"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
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
    #[allow(unused_variables)]
    pub fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let field = self.schema.fields().clone();
        let array_reader = self.batches.clone().into_iter().map(|batch| {
            let arr: ArrayRef = Arc::new(StructArray::from(batch));
            Ok(arr)
        });
        let array_reader = Box::new(ArrayIterator::new(
            array_reader,
            Field::new_struct("", field, false).into(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    pub fn __eq__(&self, other: &PyTable) -> bool {
        self.batches == other.batches && self.schema == other.schema
    }

    pub fn __len__(&self) -> usize {
        self.batches.iter().fold(0, |acc, x| acc + x.num_rows())
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowRecordBatchStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        Ok(Self::new(batches, schema))
    }

    #[classmethod]
    #[pyo3(signature = (mapping, *, schema=None, metadata=None))]
    pub fn from_pydict(
        cls: &Bound<PyType>,
        mapping: HashMap<String, AnyArray>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyResult<Self> {
        let (names, arrays): (Vec<_>, Vec<_>) = mapping.into_iter().unzip();
        Self::from_arrays(cls, arrays, Some(names), schema, metadata)
        // TODO: Construct record batches from Vec<PyChunkedArray>
        // Can I reuse from_pylist here? I.e. this func only unwraps the dict to a list of column anmes and a list of chunked arrays, and then that passes in to from_arrays
        // I probably want a helper to rechunk as necessary
        // todo!()
    }

    #[classmethod]
    #[pyo3(signature = (arrays, *, names=None, schema=None, metadata=None))]
    pub fn from_arrays(
        _cls: &Bound<PyType>,
        arrays: Vec<AnyArray>,
        names: Option<Vec<String>>,
        schema: Option<PySchema>,
        metadata: Option<MetadataInput>,
    ) -> PyResult<Self> {
        let columns = arrays
            .into_iter()
            .map(|array| array.into_chunked_array())
            .collect::<PyArrowResult<Vec<_>>>()?;

        // let schema = schema.map(|schema| schema.into_inner()).unwrap_or_else(|| {
        //     let fields = columns
        //         .iter()
        //         .zip(names.iter())
        //         .map(|(array, name)| {
        //             Field::new(name.clone(), array.field().data_type().clone(), true)
        //         })
        //         .collect::<Vec<_>>();
        //     Arc::new(
        //         Schema::new(fields)
        //             .with_metadata(metadata.unwrap_or_default().into_string_hashmap().unwrap()),
        //     )
        // });

        todo!()
    }

    pub fn add_column(
        &self,
        py: Python,
        i: usize,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<PyObject> {
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

        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }

    pub fn append_column(
        &self,
        py: Python,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<PyObject> {
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

        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }

    #[getter]
    pub fn chunk_lengths(&self) -> Vec<usize> {
        self.batches.iter().map(|batch| batch.num_rows()).collect()
    }

    pub fn column(&self, py: Python, i: FieldIndexInput) -> PyArrowResult<PyObject> {
        let column_index = i.into_position(&self.schema)?;
        let field = self.schema.field(column_index).clone();
        let chunks = self
            .batches
            .iter()
            .map(|batch| batch.column(column_index).clone())
            .collect();
        Ok(PyChunkedArray::new(chunks, field.into()).to_arro3(py)?)
    }

    #[getter]
    pub fn column_names(&self) -> Vec<String> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    #[getter]
    pub fn columns(&self, py: Python) -> PyArrowResult<Vec<PyObject>> {
        (0..self.num_columns())
            .map(|i| self.column(py, FieldIndexInput::Position(i)))
            .collect()
    }

    pub fn combine_chunks(&self, py: Python) -> PyArrowResult<PyObject> {
        let batch = concat_batches(&self.schema, &self.batches)?;
        Ok(PyTable::new(vec![batch], self.schema.clone()).to_arro3(py)?)
    }

    pub fn field(&self, py: Python, i: FieldIndexInput) -> PyArrowResult<PyObject> {
        let field = self.schema.field(i.into_position(&self.schema)?);
        Ok(PyField::new(field.clone().into()).to_arro3(py)?)
    }

    #[getter]
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    #[getter]
    pub fn num_rows(&self) -> usize {
        self.batches()
            .iter()
            .fold(0, |acc, batch| acc + batch.num_rows())
    }

    // pub fn rechunk(&self, py: Python, max_chunksize: usize) {}

    pub fn remove_column(&self, py: Python, i: usize) -> PyArrowResult<PyObject> {
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

        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }

    pub fn rename_columns(&self, py: Python, names: Vec<String>) -> PyArrowResult<PyObject> {
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
        Ok(PyTable::new(self.batches.clone(), new_schema).to_arro3(py)?)
    }

    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(self.schema.clone()).to_arro3(py)
    }

    pub fn select(&self, py: Python, columns: SelectIndices) -> PyArrowResult<PyObject> {
        let positions = columns.into_positions(self.schema.fields())?;

        let new_schema = Arc::new(self.schema.project(&positions)?);
        let new_batches = self
            .batches
            .iter()
            .map(|batch| batch.project(&positions))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }

    pub fn set_column(
        &self,
        py: Python,
        i: usize,
        field: NameOrField,
        column: PyChunkedArray,
    ) -> PyArrowResult<PyObject> {
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

        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }

    #[getter]
    pub fn shape(&self) -> (usize, usize) {
        (self.num_rows(), self.num_columns())
    }

    pub fn to_batches(&self, py: Python) -> PyResult<Vec<PyObject>> {
        self.batches
            .iter()
            .map(|batch| PyRecordBatch::new(batch.clone()).to_arro3(py))
            .collect()
    }

    pub fn to_reader(&self, py: Python) -> PyResult<PyObject> {
        let reader = Box::new(RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        ));
        PyRecordBatchReader::new(reader).to_arro3(py)
    }

    pub fn to_struct_array(&self, py: Python) -> PyArrowResult<PyObject> {
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
        Ok(PyChunkedArray::new(chunks, field.into()).to_arro3(py)?)
    }

    pub fn with_schema(&self, py: Python, schema: PySchema) -> PyArrowResult<PyObject> {
        let new_schema = schema.into_inner();
        let new_batches = self
            .batches
            .iter()
            .map(|batch| RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec()))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(PyTable::new(new_batches, new_schema).to_arro3(py)?)
    }
}
