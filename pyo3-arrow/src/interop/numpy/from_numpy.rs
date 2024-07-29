use std::sync::Arc;

use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use arrow_schema::DataType;
use numpy::{PyArray1, PyUntypedArray};
use pyo3::exceptions::PyValueError;

use crate::error::PyArrowResult;

pub fn from_numpy(array: &PyUntypedArray, arrow_data_type: DataType) -> PyArrowResult<ArrayRef> {
    macro_rules! numpy_to_arrow {
        ($rust_type:ty, $arrow_type:ty) => {{
            let arr = array.downcast::<PyArray1<$rust_type>>()?;
            Ok(Arc::new(PrimitiveArray::<$arrow_type>::from(
                arr.to_owned_array().to_vec(),
            )))
        }};
    }

    match arrow_data_type {
        // DataType::Float16 => numpy_to_arrow!(f16, Float16Type),
        DataType::Float32 => numpy_to_arrow!(f32, Float32Type),
        DataType::Float64 => numpy_to_arrow!(f64, Float64Type),
        DataType::UInt8 => numpy_to_arrow!(u8, UInt8Type),
        DataType::UInt16 => numpy_to_arrow!(u16, UInt16Type),
        DataType::UInt32 => numpy_to_arrow!(u32, UInt32Type),
        DataType::UInt64 => numpy_to_arrow!(u64, UInt64Type),
        DataType::Int8 => numpy_to_arrow!(i8, Int8Type),
        DataType::Int16 => numpy_to_arrow!(i16, Int16Type),
        DataType::Int32 => numpy_to_arrow!(i32, Int32Type),
        DataType::Int64 => numpy_to_arrow!(i64, Int64Type),
        DataType::Boolean => {
            let arr = array.downcast::<PyArray1<bool>>()?;
            Ok(Arc::new(BooleanArray::from(arr.to_owned_array().to_vec())))
        }
        _ => {
            Err(PyValueError::new_err(format!("Unsupported data type {}", arrow_data_type)).into())
        }
    }
}
