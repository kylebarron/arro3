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
use crate::{PyArray, PyChunkedArray, PyField, PyRecordBatch, PyRecordBatchReader, PyTable};

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
