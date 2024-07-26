use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyField};

#[pyfunction]
#[pyo3(signature=(values, list_size, *, r#type=None))]
pub(crate) fn fixed_size_list_array(
    py: Python,
    values: PyArray,
    list_size: i32,
    r#type: Option<PyField>,
) -> PyArrowResult<PyObject> {
    let (values_array, values_field) = values.into_inner();
    let field = r#type.map(|f| f.into_inner()).unwrap_or_else(|| {
        Arc::new(Field::new_fixed_size_list(
            "",
            values_field,
            list_size,
            true,
        ))
    });

    let array = FixedSizeListArray::try_new(field.clone(), list_size, values_array, None)?;
    Ok(PyArray::new(Arc::new(array), field).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature=(offsets, values, *, r#type=None))]
pub(crate) fn list_array(
    py: Python,
    offsets: PyArray,
    values: PyArray,
    r#type: Option<PyField>,
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
    let field = r#type.map(|f| f.into_inner()).unwrap_or_else(|| {
        if large_offsets {
            Arc::new(Field::new_large_list("item", values_field, true))
        } else {
            Arc::new(Field::new_list("item", values_field, true))
        }
    });

    let list_array: ArrayRef = if large_offsets {
        Arc::new(LargeListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets_array.as_primitive::<Int64Type>().values().clone()),
            values_array,
            None,
        )?)
    } else {
        Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets_array.as_primitive::<Int32Type>().values().clone()),
            values_array,
            None,
        )?)
    };
    Ok(PyArray::new(Arc::new(list_array), field).to_arro3(py)?)
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
