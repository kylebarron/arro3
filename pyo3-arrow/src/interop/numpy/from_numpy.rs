use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, Date32Builder, DurationMicrosecondBuilder, DurationMillisecondBuilder,
    DurationNanosecondBuilder, DurationSecondBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use numpy::datetime::units::{Days, Microseconds, Milliseconds, Nanoseconds, Seconds};
use numpy::datetime::{Datetime, Timedelta};
use numpy::{
    PyArray1, PyArrayDescr, PyArrayDescrMethods, PyArrayMethods, PyUntypedArray,
    PyUntypedArrayMethods,
};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};

use crate::error::PyArrowResult;

pub fn from_numpy(py: Python, array: &Bound<PyUntypedArray>) -> PyArrowResult<ArrayRef> {
    macro_rules! primitive_numpy_to_arrow {
        ($rust_type:ty, $arrow_type:ty) => {{
            let np_arr = array.cast::<PyArray1<$rust_type>>()?;
            let np_readonly_arr = np_arr.try_readonly()?;
            let arrow_arr = if let Ok(contiguous_arr) = np_readonly_arr.as_slice() {
                PrimitiveArray::<$arrow_type>::from_iter_values(contiguous_arr.iter().map(|v| *v))
            } else {
                PrimitiveArray::<$arrow_type>::from(np_readonly_arr.to_owned_array().to_vec())
            };
            Ok(Arc::new(arrow_arr))
        }};
    }

    let dtype = array.dtype();
    if is_type::<half::f16>(py, &dtype) {
        primitive_numpy_to_arrow!(half::f16, Float16Type)
    } else if is_type::<f32>(py, &dtype) {
        primitive_numpy_to_arrow!(f32, Float32Type)
    } else if is_type::<f64>(py, &dtype) {
        primitive_numpy_to_arrow!(f64, Float64Type)
    } else if is_type::<u8>(py, &dtype) {
        primitive_numpy_to_arrow!(u8, UInt8Type)
    } else if is_type::<u16>(py, &dtype) {
        primitive_numpy_to_arrow!(u16, UInt16Type)
    } else if is_type::<u32>(py, &dtype) {
        primitive_numpy_to_arrow!(u32, UInt32Type)
    } else if is_type::<u64>(py, &dtype) {
        primitive_numpy_to_arrow!(u64, UInt64Type)
    } else if is_type::<i8>(py, &dtype) {
        primitive_numpy_to_arrow!(i8, Int8Type)
    } else if is_type::<i16>(py, &dtype) {
        primitive_numpy_to_arrow!(i16, Int16Type)
    } else if is_type::<i32>(py, &dtype) {
        primitive_numpy_to_arrow!(i32, Int32Type)
    } else if is_type::<i64>(py, &dtype) {
        primitive_numpy_to_arrow!(i64, Int64Type)
    } else if is_type::<bool>(py, &dtype) {
        let arr = array.cast::<PyArray1<bool>>()?;
        Ok(Arc::new(BooleanArray::from(arr.to_owned_array().to_vec())))
    } else if let Ok(array) = array.cast::<PyArray1<Datetime<Days>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            days_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            days_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Datetime<Seconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            seconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            seconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Datetime<Milliseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            milliseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            milliseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Datetime<Microseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            microseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            microseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Datetime<Nanoseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            nanoseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            nanoseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Timedelta<Seconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            seconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            seconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Timedelta<Milliseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            milliseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            milliseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Timedelta<Microseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            microseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            microseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.cast::<PyArray1<Timedelta<Nanoseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            nanoseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            nanoseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if dtype.char() == b'U' {
        import_fixed_width_string_array(array)
    } else if dtype.char() == b'S' {
        import_fixed_width_binary_array(array)
    } else if let Ok(array) = array.cast::<PyArray1<Py<PyAny>>>() {
        try_import_object_array(py, array)
    } else if dtype.char() == b'T' {
        import_variable_width_string_array(
            array,
            dtype.getattr_opt(intern!(py, "na_object"))?.as_ref(),
        )
    } else {
        Err(PyValueError::new_err(format!("Unsupported data type {}", dtype)).into())
    }
}

fn is_type<T: numpy::Element>(py: Python, dtype: &Bound<PyArrayDescr>) -> bool {
    dtype.is_equiv_to(&numpy::dtype::<T>(py))
}

fn days_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Days>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = Date32Builder::with_capacity(capacity);
    for val in vals {
        builder.append_value(i64::from(*val).try_into().unwrap());
    }

    Ok(Arc::new(builder.finish()))
}

fn seconds_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Seconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = TimestampSecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn milliseconds_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Milliseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = TimestampMillisecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn microseconds_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Microseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn nanoseconds_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Nanoseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = TimestampNanosecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn seconds_to_duration_array<'a>(
    vals: impl Iterator<Item = &'a Timedelta<Seconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = DurationSecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn milliseconds_to_duration_array<'a>(
    vals: impl Iterator<Item = &'a Timedelta<Milliseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = DurationMillisecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn microseconds_to_duration_array<'a>(
    vals: impl Iterator<Item = &'a Timedelta<Microseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = DurationMicrosecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn nanoseconds_to_duration_array<'a>(
    vals: impl Iterator<Item = &'a Timedelta<Nanoseconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = DurationNanosecondBuilder::with_capacity(capacity);
    for val in vals {
        builder.append_value((*val).into());
    }
    Ok(Arc::new(builder.finish()))
}

fn import_fixed_width_string_array(array: &Bound<PyUntypedArray>) -> PyArrowResult<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    for item in array.try_iter()? {
        builder.append_value(item?.extract::<PyBackedStr>()?);
    }
    Ok(Arc::new(builder.finish()))
}

fn import_fixed_width_binary_array(array: &Bound<PyUntypedArray>) -> PyArrowResult<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(array.len(), 0);
    for item in array.try_iter()? {
        builder.append_value(item?.extract::<PyBackedBytes>()?);
    }
    Ok(Arc::new(builder.finish()))
}

/// For now we import Numpy v2 string arrays through Python string objects.
///
/// This is less performant than accessing the numpy string data directly,
/// but the `numpy` crate as of v0.25 doesn't have a safe way to access the underlying
/// string data
fn import_variable_width_string_array(
    array: &Bound<PyUntypedArray>,
    na_object: Option<&Bound<PyAny>>,
) -> PyArrowResult<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(array.len(), 0);
    for item in array.try_iter()? {
        let item = item?;
        if na_object.is_some_and(|x| item.is(x)) {
            builder.append_null();
        } else {
            builder.append_value(item.extract::<PyBackedStr>()?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn try_import_object_array(
    py: Python,
    array: &Bound<PyArray1<Py<PyAny>>>,
) -> PyArrowResult<ArrayRef> {
    let np_readonly_arr = array.try_readonly()?;
    if let Some(first_element) = np_readonly_arr.get(0) {
        if first_element.extract::<PyBackedStr>(py).is_ok() {
            if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
                try_import_object_array_as_string(
                    py,
                    np_contiguous_arr.iter(),
                    np_contiguous_arr.len(),
                )
            } else {
                try_import_object_array_as_string(
                    py,
                    np_readonly_arr.to_owned_array().iter(),
                    np_readonly_arr.len(),
                )
            }
        } else if first_element.extract::<PyBackedBytes>(py).is_ok() {
            if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
                try_import_object_array_as_binary(
                    py,
                    np_contiguous_arr.iter(),
                    np_contiguous_arr.len(),
                )
            } else {
                try_import_object_array_as_binary(
                    py,
                    np_readonly_arr.to_owned_array().iter(),
                    np_readonly_arr.len(),
                )
            }
        } else {
            Err(PyValueError::new_err(format!(
                "Only arrays of bytes or strings are supported for object dtype, got a '{}' object",
                type_repr(py, first_element)?
            ))
            .into())
        }
    } else {
        Err(PyValueError::new_err("Cannot import empty numpy array of type np.object_").into())
    }
}

fn try_import_object_array_as_string<'a>(
    py: Python,
    vals: impl Iterator<Item = &'a Py<PyAny>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(capacity, 0);
    for val in vals {
        if let Ok(s) = val.extract::<PyBackedStr>(py) {
            builder.append_value(s);
        } else {
            return Err(PyValueError::new_err(format!(
                "Expected string, got a '{}' object",
                type_repr(py, val)?
            ))
            .into());
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn try_import_object_array_as_binary<'a>(
    py: Python,
    vals: impl Iterator<Item = &'a Py<PyAny>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = BinaryBuilder::with_capacity(capacity, 0);
    for val in vals {
        if let Ok(s) = val.extract::<PyBackedBytes>(py) {
            builder.append_value(s);
        } else {
            return Err(PyValueError::new_err(format!(
                "Expected bytes, got a '{}' object",
                type_repr(py, val)?
            ))
            .into());
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn type_repr(py: Python, obj: &Py<PyAny>) -> PyResult<String> {
    let builtins = py.import(intern!(py, "builtins"))?;
    let type_fn = builtins.getattr(intern!(py, "type"))?;
    type_fn
        .call1((obj,))?
        .getattr(intern!(py, "__name__"))?
        .extract()
}
