use arrow::array::AsArray;
use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType, FieldRef};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

#[derive(FromPyObject)]
pub(crate) enum StructIndex {
    Int(usize),
    ListInt(Vec<usize>),
}

impl StructIndex {
    fn into_list(self) -> Vec<usize> {
        match self {
            Self::Int(i) => vec![i],
            Self::ListInt(i) => i,
        }
    }
}

#[pyfunction]
#[pyo3(signature=(values, /, indices, * ))]
pub(crate) fn struct_field(
    py: Python,
    values: PyArray,
    indices: StructIndex,
) -> PyArrowResult<PyObject> {
    let (array, field) = values.into_inner();
    let indices = indices.into_list();

    let mut array_ref = &array;
    let mut field_ref = &field;
    for i in indices {
        (array_ref, field_ref) = get_child(&array, i)?;
    }

    Ok(PyArray::new(array_ref.clone(), field_ref.clone()).to_arro3(py)?)
}

fn get_child(array: &ArrayRef, i: usize) -> Result<(&ArrayRef, &FieldRef), ArrowError> {
    match array.data_type() {
        DataType::Struct(fields) => {
            let arr = array.as_struct();
            let inner_arr = arr.columns().get(i).ok_or(ArrowError::SchemaError(
                "Out of range for number of fields".into(),
            ))?;
            let inner_field = &fields[i];
            Ok((inner_arr, inner_field))
        }
        _ => Err(ArrowError::SchemaError(
            "DataType must be struct.".to_string(),
        )),
    }
}
