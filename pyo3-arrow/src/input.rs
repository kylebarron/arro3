//! Special input types to allow broader user input.
//!
//! These types tend to be used for unknown user input, and thus do not exist for return types,
//! where the exact type is known.

use std::collections::HashMap;
use std::string::FromUtf8Error;
use std::sync::Arc;

use arrow_array::{Datum, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Field, FieldRef, Fields, Schema, SchemaRef};
use pyo3::exceptions::{PyIndexError, PyKeyError, PyValueError};
use pyo3::prelude::*;

use crate::array_reader::PyArrayReader;
use crate::error::PyArrowResult;
use crate::ffi::{ArrayIterator, ArrayReader};
use crate::{
    PyArray, PyChunkedArray, PyField, PyRecordBatch, PyRecordBatchReader, PyScalar, PyTable,
};

/// An enum over [PyRecordBatch] and [PyRecordBatchReader], used when a function accepts either
/// Arrow object as input.
pub enum AnyRecordBatch {
    /// A single RecordBatch, held in a [PyRecordBatch].
    RecordBatch(PyRecordBatch),
    /// A stream of possibly multiple RecordBatches, held in a [PyRecordBatchReader].
    Stream(PyRecordBatchReader),
}

impl AnyRecordBatch {
    /// Consume this and convert it into a [RecordBatchReader].
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

    /// Consume this and convert it into a [PyTable].
    ///
    /// All record batches from the stream will be materialized in memory.
    pub fn into_table(self) -> PyArrowResult<PyTable> {
        let reader = self.into_reader()?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<_, ArrowError>>()?;
        Ok(PyTable::try_new(batches, schema)?)
    }

    /// Access the underlying [SchemaRef] of this object.
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
    /// A single Array, held in a [PyArray].
    Array(PyArray),
    /// A stream of possibly multiple Arrays, held in a [PyArrayReader].
    Stream(PyArrayReader),
}

impl AnyArray {
    /// Consume this and convert it into a [PyChunkedArray].
    ///
    /// All arrays from the stream will be materialized in memory.
    pub fn into_chunked_array(self) -> PyArrowResult<PyChunkedArray> {
        let reader = self.into_reader()?;
        let field = reader.field();
        let chunks = reader.collect::<Result<_, ArrowError>>()?;
        Ok(PyChunkedArray::try_new(chunks, field)?)
    }

    /// Consume this and convert it into a [ArrayReader].
    pub fn into_reader(self) -> PyResult<Box<dyn ArrayReader + Send>> {
        match self {
            Self::Array(array) => {
                let (array, field) = array.into_inner();
                Ok(Box::new(ArrayIterator::new(vec![Ok(array)], field)))
            }
            Self::Stream(stream) => stream.into_reader(),
        }
    }

    /// Access the underlying [FieldRef] of this object.
    pub fn field(&self) -> PyResult<FieldRef> {
        match self {
            Self::Array(array) => Ok(array.field().clone()),
            Self::Stream(stream) => stream.field_ref(),
        }
    }
}

/// An enum over [PyArray] and [PyScalar], used for functions that accept
pub enum AnyDatum {
    /// A single Array, held in a [PyArray].
    Array(PyArray),
    /// An Arrow Scalar, held in a [pyScalar]
    Scalar(PyScalar),
}

impl AnyDatum {
    /// Access the field of this object.
    pub fn field(&self) -> &FieldRef {
        match self {
            Self::Array(inner) => inner.field(),
            Self::Scalar(inner) => inner.field(),
        }
    }
}

impl Datum for AnyDatum {
    fn get(&self) -> (&dyn arrow_array::Array, bool) {
        match self {
            Self::Array(inner) => inner.get(),
            Self::Scalar(inner) => inner.get(),
        }
    }
}

#[derive(FromPyObject)]
pub(crate) enum MetadataInput {
    String(HashMap<String, String>),
    Bytes(HashMap<Vec<u8>, Vec<u8>>),
}

impl MetadataInput {
    pub(crate) fn into_string_hashmap(self) -> PyResult<HashMap<String, String>> {
        match self {
            Self::String(hm) => Ok(hm),
            Self::Bytes(hm) => {
                let mut new_hashmap = HashMap::with_capacity(hm.len());
                hm.into_iter().try_for_each(|(key, value)| {
                    new_hashmap.insert(String::from_utf8(key)?, String::from_utf8(value)?);
                    Ok::<_, FromUtf8Error>(())
                })?;
                Ok(new_hashmap)
            }
        }
    }
}

impl Default for MetadataInput {
    fn default() -> Self {
        Self::String(Default::default())
    }
}

#[derive(FromPyObject)]
pub(crate) enum FieldIndexInput {
    Name(String),
    Position(usize),
}

impl FieldIndexInput {
    /// This will additionally check that the input is valid against the given schema.
    ///
    /// This will raise a KeyError if the provided name does not exist, or an IndexError if the
    /// provided integer index is out of bounds.
    pub fn into_position(self, schema: &Schema) -> PyResult<usize> {
        match self {
            Self::Name(name) => schema
                .index_of(name.as_ref())
                .map_err(|err| PyKeyError::new_err(err.to_string())),
            Self::Position(position) => {
                if position >= schema.fields().len() {
                    return Err(PyIndexError::new_err("Index out of range").into());
                }
                Ok(position)
            }
        }
    }
}

#[derive(FromPyObject)]
pub(crate) enum NameOrField {
    Name(String),
    Field(PyField),
}

impl NameOrField {
    pub fn into_field(self, source_field: &Field) -> FieldRef {
        match self {
            Self::Name(name) => Arc::new(
                Field::new(
                    name,
                    source_field.data_type().clone(),
                    source_field.is_nullable(),
                )
                .with_metadata(source_field.metadata().clone()),
            ),
            Self::Field(field) => field.into_inner(),
        }
    }
}

#[derive(FromPyObject)]
pub(crate) enum SelectIndices {
    Names(Vec<String>),
    Positions(Vec<usize>),
}

impl SelectIndices {
    pub fn into_positions(self, fields: &Fields) -> PyResult<Vec<usize>> {
        match self {
            Self::Names(names) => {
                let mut positions = Vec::with_capacity(names.len());
                for name in names {
                    let index = fields
                        .iter()
                        .position(|field| field.name() == &name)
                        .ok_or(PyValueError::new_err(format!("{name} not in schema.")))?;
                    positions.push(index);
                }
                Ok(positions)
            }
            Self::Positions(positions) => Ok(positions),
        }
    }
}
