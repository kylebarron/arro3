//! Special input types to allow broader user input.
//!
//! These types tend to be used for unknown user input, and thus do not exist for return types,
//! where the exact type is known.

use std::collections::HashMap;
use std::string::FromUtf8Error;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{FieldRef, SchemaRef};
use pyo3::prelude::*;

use crate::array_reader::PyArrayReader;
use crate::ffi::{ArrayIterator, ArrayReader};
use crate::{PyArray, PyRecordBatch, PyRecordBatchReader};

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
    String(String),
    Int(usize),
}
