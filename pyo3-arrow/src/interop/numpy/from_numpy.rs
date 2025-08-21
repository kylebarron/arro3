use std::sync::Arc;

use arrow_array::builder::{
    Date64Builder, DurationMicrosecondBuilder, DurationMillisecondBuilder,
    DurationNanosecondBuilder, DurationSecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use chrono::NaiveDate;
use numpy::datetime::units::{Days, Microseconds, Milliseconds, Nanoseconds, Seconds};
use numpy::datetime::{Datetime, Timedelta};
use numpy::{
    PyArray1, PyArrayDescr, PyArrayDescrMethods, PyArrayMethods, PyUntypedArray,
    PyUntypedArrayMethods,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::error::PyArrowResult;

pub fn from_numpy(py: Python, array: &Bound<PyUntypedArray>) -> PyArrowResult<ArrayRef> {
    macro_rules! primitive_numpy_to_arrow {
        ($rust_type:ty, $arrow_type:ty) => {{
            let np_arr = array.downcast::<PyArray1<$rust_type>>()?;
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
        let arr = array.downcast::<PyArray1<bool>>()?;
        Ok(Arc::new(BooleanArray::from(arr.to_owned_array().to_vec())))
    } else if let Ok(array) = array.downcast::<PyArray1<Datetime<Days>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            days_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            days_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Datetime<Seconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            seconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            seconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Datetime<Milliseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            milliseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            milliseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Datetime<Microseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            microseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            microseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Datetime<Nanoseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            nanoseconds_to_timestamp_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            nanoseconds_to_timestamp_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Timedelta<Seconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            seconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            seconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Timedelta<Milliseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            milliseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            milliseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Timedelta<Microseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            microseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            microseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
    } else if let Ok(array) = array.downcast::<PyArray1<Timedelta<Nanoseconds>>>() {
        let np_readonly_arr = array.try_readonly()?;
        if let Ok(np_contiguous_arr) = np_readonly_arr.as_slice() {
            nanoseconds_to_duration_array(np_contiguous_arr.iter(), np_contiguous_arr.len())
        } else {
            nanoseconds_to_duration_array(
                np_readonly_arr.to_owned_array().iter(),
                np_readonly_arr.len(),
            )
        }
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
    let mut builder = Date64Builder::with_capacity(capacity);

    let chrono_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    for val in vals {
        let day_delta = chrono::Days::new(i64::from(*val).try_into().unwrap());
        let date = chrono_epoch
            .checked_add_days(day_delta)
            .ok_or(PyValueError::new_err(
                "Days out of bounds for Arrow Date64Array",
            ))?;

        let datetime = date.and_hms_opt(0, 0, 0).unwrap();
        let millis = datetime.and_utc().timestamp_millis();
        builder.append_value(millis);
    }

    Ok(Arc::new(builder.finish()))
}

fn seconds_to_timestamp_array<'a>(
    vals: impl Iterator<Item = &'a Datetime<Seconds>>,
    capacity: usize,
) -> PyArrowResult<ArrayRef> {
    let mut builder = TimestampSecondBuilder::with_capacity(capacity);
    for val in vals {
        let millis = (*val).into();
        builder.append_value(millis);
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
