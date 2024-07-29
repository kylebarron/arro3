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
    let (orig_array, field) = values.into_inner();
    let indices = indices.into_list();

    dbg!(orig_array.offset());
    dbg!(orig_array.len());

    let mut array_ref = &orig_array;
    let mut field_ref = &field;
    for i in indices {
        (array_ref, field_ref) = get_child(array_ref, i)?;
        dbg!(array_ref.offset());
    }

    Ok(PyArray::new(
        array_ref.slice(orig_array.offset(), orig_array.len()),
        field_ref.clone(),
    )
    .to_arro3(py)?)
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

#[cfg(test)]
mod test {
    use arrow::datatypes::Int64Type;
    use arrow_array::{Array, PrimitiveArray};

    #[test]
    fn test_offset() {
        let arr: PrimitiveArray<Int64Type> = PrimitiveArray::from(vec![1, 2, 3, 4]);
        let offset = 2;
        let sliced = arr.slice(offset, 2);
        assert_eq!(sliced.offset(), offset);
    }
}
