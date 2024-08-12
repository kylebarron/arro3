//! Special input types to allow broader user input.
//!
//! These types tend to be used for unknown user input, and thus do not exist for return types,
//! where the exact type is known.

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, FieldRef, SchemaRef};
use pyo3::prelude::*;

use crate::array_reader::PyArrayReader;
use crate::error::PyArrowResult;
use crate::ffi::{ArrayIterator, ArrayReader};
use crate::{PyArray, PyChunkedArray, PyRecordBatch, PyRecordBatchReader, PyTable};

/// An enum over [PyRecordBatch] and [PyRecordBatchReader], used when a function accepts either
/// Arrow object as input.
pub enum AnyRecordBatch {
    RecordBatch(PyRecordBatch),
    Stream(PyRecordBatchReader),
}

impl AnyRecordBatch {
    pub fn into_reader(self) -> PyResult<Box<dyn RecordBatchReader + Send>> {
        match self {
            Self::RecordBatch(batch) => {
                let batch = batch.into_inner();
                let schema = batch.schema();
                Ok(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
            }
            Self::Stream(stream) => stream.into_reader(),
        }
    }

    pub fn into_table(self) -> PyArrowResult<PyTable> {
        let reader = self.into_reader()?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<_, ArrowError>>()?;
        Ok(PyTable::new(batches, schema))
    }

    pub fn schema(&self) -> PyResult<SchemaRef> {
        match self {
            Self::RecordBatch(batch) => Ok(batch.as_ref().schema()),
            Self::Stream(stream) => stream.schema_ref(),
        }
    }
}

/// An enum over [PyArray] and [PyArrayReader], used when a function accepts either
/// Arrow object as input.
pub enum AnyArray {
    Array(PyArray),
    Stream(PyArrayReader),
}

impl AnyArray {
    pub fn into_chunked_array(self) -> PyArrowResult<PyChunkedArray> {
        let reader = self.into_reader()?;
        let field = reader.field();
        let chunks = reader.collect::<Result<_, ArrowError>>()?;
        Ok(PyChunkedArray::new(chunks, field))
    }

    pub fn into_reader(self) -> PyResult<Box<dyn ArrayReader + Send>> {
        match self {
            Self::Array(array) => {
                let (array, field) = array.into_inner();
                Ok(Box::new(ArrayIterator::new(vec![Ok(array)], field)))
            }
            Self::Stream(stream) => stream.into_reader(),
        }
    }

    pub fn field(&self) -> PyResult<FieldRef> {
        match self {
            Self::Array(array) => Ok(array.field().clone()),
            Self::Stream(stream) => stream.field_ref(),
        }
    }
}
