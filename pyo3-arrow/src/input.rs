//! Special input types to allow broader user input.
//!
//! These types tend to be used for unknown user input, and thus do not exist for return types,
//! where the exact type is known.

use std::collections::HashMap;
use std::string::FromUtf8Error;
use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Field, FieldRef, Fields, Schema, SchemaRef};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::array_reader::PyArrayReader;
use crate::error::PyArrowResult;
use crate::ffi::{ArrayIterator, ArrayReader};
use crate::{PyArray, PyChunkedArray, PyField, PyRecordBatch, PyRecordBatchReader};

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
        match self {
            Self::Array(array) => {
                let (array, field) = array.into_inner();
                Ok(PyChunkedArray::new(vec![array], field))
            }
            Self::Stream(stream) => {
                let field = stream.field_ref()?;
                let chunks = stream
                    .into_reader()?
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(PyChunkedArray::new(chunks, field))
            }
        }
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

#[derive(FromPyObject)]
pub enum MetadataInput {
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
pub enum FieldIndexInput {
    Name(String),
    Position(usize),
}

impl FieldIndexInput {
    pub fn into_position(self, schema: &Schema) -> PyArrowResult<usize> {
        match self {
            Self::Name(name) => Ok(schema.index_of(name.as_ref())?),
            Self::Position(position) => Ok(position),
        }
    }
}

#[derive(FromPyObject)]
pub enum NameOrField {
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
pub enum SelectIndices {
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
