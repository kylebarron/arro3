use std::sync::Arc;

use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use numpy::{dtype_bound, PyArray1, PyArrayDescr, PyUntypedArray};
use pyo3::exceptions::PyValueError;
use pyo3::Python;

use crate::error::PyArrowResult;

pub fn from_numpy(py: Python, array: &PyUntypedArray) -> PyArrowResult<ArrayRef> {
    macro_rules! numpy_to_arrow {
        ($rust_type:ty, $arrow_type:ty) => {{
            let arr = array.downcast::<PyArray1<$rust_type>>()?;
            Ok(Arc::new(PrimitiveArray::<$arrow_type>::from(
                arr.to_owned_array().to_vec(),
            )))
        }};
    }
    let dtype = array.dtype();
    if is_type::<f32>(py, dtype) {
        numpy_to_arrow!(f32, Float32Type)
    } else if is_type::<f64>(py, dtype) {
        numpy_to_arrow!(f64, Float64Type)
    } else if is_type::<u8>(py, dtype) {
        numpy_to_arrow!(u8, UInt8Type)
    } else if is_type::<u16>(py, dtype) {
        numpy_to_arrow!(u16, UInt16Type)
    } else if is_type::<u32>(py, dtype) {
        numpy_to_arrow!(u32, UInt32Type)
    } else if is_type::<u64>(py, dtype) {
        numpy_to_arrow!(u64, UInt64Type)
    } else if is_type::<i8>(py, dtype) {
        numpy_to_arrow!(i8, Int8Type)
    } else if is_type::<i16>(py, dtype) {
        numpy_to_arrow!(i16, Int16Type)
    } else if is_type::<i32>(py, dtype) {
        numpy_to_arrow!(i32, Int32Type)
    } else if is_type::<i64>(py, dtype) {
        numpy_to_arrow!(i64, Int64Type)
    } else if is_type::<bool>(py, dtype) {
        let arr = array.downcast::<PyArray1<bool>>()?;
        Ok(Arc::new(BooleanArray::from(arr.to_owned_array().to_vec())))
    } else {
        Err(PyValueError::new_err(format!("Unsupported data type {}", dtype)).into())
    }
}

fn is_type<T: numpy::Element>(py: Python, dtype: &PyArrayDescr) -> bool {
    dtype.is_equiv_to(dtype_bound::<T>(py).as_gil_ref())
}
