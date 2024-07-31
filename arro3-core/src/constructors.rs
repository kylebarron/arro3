use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyDataType, PyField};

#[pyfunction]
#[pyo3(signature=(values, list_size, *, r#type=None))]
pub(crate) fn fixed_size_list_array(
    py: Python,
    values: PyArray,
    list_size: i32,
    r#type: Option<PyDataType>,
) -> PyArrowResult<PyObject> {
    let (values_array, values_field) = values.into_inner();
    let list_data_type = r#type
        .map(|t| t.into_inner())
        .unwrap_or_else(|| DataType::FixedSizeList(values_field.clone(), list_size));
    let inner_field = match &list_data_type {
        DataType::FixedSizeList(inner_field, _) => inner_field,
        _ => {
            return Err(
                PyValueError::new_err("Expected fixed size list as the outer data type").into(),
            )
        }
    };
    let array = FixedSizeListArray::try_new(inner_field.clone(), list_size, values_array, None)?;
    Ok(PyArray::new(Arc::new(array), Field::new("", list_data_type, true).into()).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature=(offsets, values, *, r#type=None))]
pub(crate) fn list_array(
    py: Python,
    offsets: PyArray,
    values: PyArray,
    r#type: Option<PyDataType>,
) -> PyArrowResult<PyObject> {
    let (values_array, values_field) = values.into_inner();
    let (offsets_array, _) = offsets.into_inner();
    let large_offsets = match offsets_array.data_type() {
        DataType::Int32 => false,
        DataType::Int64 => true,
        _ => {
            return Err(
                PyValueError::new_err("Expected offsets to have int32 or int64 type").into(),
            )
        }
    };
    let list_data_type = r#type.map(|t| t.into_inner()).unwrap_or_else(|| {
        if large_offsets {
            DataType::LargeList(values_field.clone())
        } else {
            DataType::List(values_field.clone())
        }
    });
    let inner_field = match &list_data_type {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => inner_field,
        _ => {
            return Err(
                PyValueError::new_err("Expected fixed size list as the outer data type").into(),
            )
        }
    };

    let list_array: ArrayRef = if large_offsets {
        Arc::new(LargeListArray::try_new(
            inner_field.clone(),
            OffsetBuffer::new(offsets_array.as_primitive::<Int64Type>().values().clone()),
            values_array,
            None,
        )?)
    } else {
        Arc::new(ListArray::try_new(
            inner_field.clone(),
            OffsetBuffer::new(offsets_array.as_primitive::<Int32Type>().values().clone()),
            values_array,
            None,
        )?)
    };
    Ok(PyArray::new(
        Arc::new(list_array),
        Field::new("", list_data_type, true).into(),
    )
    .to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature=(arrays, *, fields))]
pub(crate) fn struct_array(
    py: Python,
    arrays: Vec<PyArray>,
    fields: Vec<PyField>,
) -> PyArrowResult<PyObject> {
    let arrays = arrays
        .into_iter()
        .map(|arr| {
            let (arr, _field) = arr.into_inner();
            arr
        })
        .collect::<Vec<_>>();
    let fields = fields
        .into_iter()
        .map(|field| field.into_inner())
        .collect::<Vec<_>>();

    let array = StructArray::try_new(fields.clone().into(), arrays, None)?;
    let field = Field::new_struct("", fields, true);
    Ok(PyArray::new(Arc::new(array), field.into()).to_arro3(py)?)
}
