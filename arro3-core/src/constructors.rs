use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3Array;
use pyo3_arrow::{PyArray, PyField};

#[pyfunction]
#[pyo3(signature=(values, list_size, *, r#type=None))]
pub(crate) fn fixed_size_list_array(
    values: PyArray,
    list_size: i32,
    r#type: Option<PyField>,
) -> PyArrowResult<Arro3Array> {
    let (values_array, values_field) = values.into_inner();
    let output_field = r#type.map(|t| t.into_inner()).unwrap_or_else(|| {
        Arc::new(Field::new(
            "",
            DataType::FixedSizeList(values_field.clone(), list_size),
            true,
        ))
    });
    let inner_field = match output_field.data_type() {
        DataType::FixedSizeList(inner_field, _) => inner_field,
        _ => {
            return Err(
                PyValueError::new_err("Expected fixed size list as the outer data type").into(),
            )
        }
    };
    let array = FixedSizeListArray::try_new(inner_field.clone(), list_size, values_array, None)?;
    Ok(PyArray::new(Arc::new(array), output_field).into())
}

#[pyfunction]
#[pyo3(signature=(offsets, values, *, r#type=None))]
pub(crate) fn list_array(
    offsets: PyArray,
    values: PyArray,
    r#type: Option<PyField>,
) -> PyArrowResult<Arro3Array> {
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
    let output_field = r#type.map(|t| t.into_inner()).unwrap_or_else(|| {
        if large_offsets {
            Arc::new(Field::new(
                "",
                DataType::LargeList(values_field.clone()),
                true,
            ))
        } else {
            Arc::new(Field::new("", DataType::List(values_field.clone()), true))
        }
    });
    let inner_field = match output_field.data_type() {
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
    Ok(PyArray::new(Arc::new(list_array), output_field).into())
}

#[pyfunction]
#[pyo3(signature=(arrays, *, fields, r#type=None))]
pub(crate) fn struct_array(
    arrays: Vec<PyArray>,
    fields: Vec<PyField>,
    r#type: Option<PyField>,
) -> PyArrowResult<Arro3Array> {
    let output_field = r#type.map(|t| t.into_inner()).unwrap_or_else(|| {
        let fields = fields
            .into_iter()
            .map(|field| field.into_inner())
            .collect::<Vec<_>>();
        Arc::new(Field::new_struct("", fields, true))
    });
    let inner_fields = match output_field.data_type() {
        DataType::Struct(inner_fields) => inner_fields.clone(),
        _ => return Err(PyValueError::new_err("Expected struct as the outer data type").into()),
    };

    let arrays = arrays
        .into_iter()
        .map(|arr| {
            let (arr, _field) = arr.into_inner();
            arr
        })
        .collect::<Vec<_>>();

    let array = StructArray::try_new(inner_fields, arrays, None)?;
    Ok(PyArray::new(Arc::new(array), output_field).into())
}
