use arrow::array::AsArray;
use arrow::datatypes::*;
use arrow_array::Array;
use arrow_schema::DataType;
use numpy::ToPyArray;
use pyo3::exceptions::{PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::{intern, PyResult, Python};

pub fn to_numpy<'py>(py: Python<'py>, arr: &'py dyn Array) -> PyResult<Bound<'py, PyAny>> {
    if arr.null_count() > 0 {
        return Err(PyValueError::new_err(
            "Cannot create numpy array from arrow array with nulls.",
        ));
    }

    macro_rules! impl_primitive {
        ($arrow_type:ty) => {
            arr.as_primitive::<$arrow_type>()
                .values()
                .to_pyarray(py)
                .into_any()
        };
    }

    let result = match arr.data_type() {
        DataType::Float16 => impl_primitive!(Float16Type),
        DataType::Float32 => impl_primitive!(Float32Type),
        DataType::Float64 => impl_primitive!(Float64Type),
        DataType::UInt8 => impl_primitive!(UInt8Type),
        DataType::UInt16 => impl_primitive!(UInt16Type),
        DataType::UInt32 => impl_primitive!(UInt32Type),
        DataType::UInt64 => impl_primitive!(UInt64Type),
        DataType::Int8 => impl_primitive!(Int8Type),
        DataType::Int16 => impl_primitive!(Int16Type),
        DataType::Int32 => impl_primitive!(Int32Type),
        DataType::Int64 => impl_primitive!(Int64Type),
        DataType::Boolean => {
            let bools = arr.as_boolean().values().iter().collect::<Vec<_>>();
            bools.to_pyarray(py).into_any()
        }
        // For other data types we create Python objects and then create an object-typed numpy
        // array
        DataType::Binary => {
            let mut py_bytes = Vec::with_capacity(arr.len());
            arr.as_binary::<i32>()
                .iter()
                .for_each(|x| py_bytes.push(PyBytes::new(py, x.unwrap())));
            let py_list = PyList::new(py, py_bytes)?;
            let numpy_mod = py.import(intern!(py, "numpy"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
            numpy_mod.call_method(
                intern!(py, "array"),
                PyTuple::new(py, vec![py_list])?,
                Some(&kwargs),
            )?
        }
        DataType::LargeBinary => {
            let mut py_bytes = Vec::with_capacity(arr.len());
            arr.as_binary::<i64>()
                .iter()
                .for_each(|x| py_bytes.push(PyBytes::new(py, x.unwrap())));
            let py_list = PyList::new(py, py_bytes)?;
            let numpy_mod = py.import(intern!(py, "numpy"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
            numpy_mod.call_method(
                intern!(py, "array"),
                PyTuple::new(py, vec![py_list])?,
                Some(&kwargs),
            )?
        }
        DataType::Utf8 => {
            let mut py_bytes = Vec::with_capacity(arr.len());
            arr.as_string::<i32>()
                .iter()
                .for_each(|x| py_bytes.push(PyString::new(py, x.unwrap())));
            let py_list = PyList::new(py, py_bytes)?;
            let numpy_mod = py.import(intern!(py, "numpy"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
            numpy_mod.call_method(
                intern!(py, "array"),
                PyTuple::new(py, vec![py_list])?,
                Some(&kwargs),
            )?
        }
        DataType::LargeUtf8 => {
            let mut py_bytes = Vec::with_capacity(arr.len());
            arr.as_string::<i64>()
                .iter()
                .for_each(|x| py_bytes.push(PyString::new(py, x.unwrap())));
            let py_list = PyList::new(py, py_bytes)?;
            let numpy_mod = py.import(intern!(py, "numpy"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", numpy_mod.getattr(intern!(py, "object_"))?)?;
            numpy_mod.call_method(
                intern!(py, "array"),
                PyTuple::new(py, vec![py_list])?,
                Some(&kwargs),
            )?
        }
        dt => {
            return Err(PyNotImplementedError::new_err(format!(
                "Unsupported type in to_numpy {dt}"
            )))
        }
    };
    Ok(result)
}

pub fn chunked_to_numpy<'py>(
    py: Python<'py>,
    arrs: Vec<&'py dyn Array>,
) -> PyResult<Bound<'py, PyAny>> {
    let py_arrays = arrs
        .iter()
        .map(|arr| to_numpy(py, *arr))
        .collect::<PyResult<Vec<_>>>()?;

    let numpy_mod = py.import(intern!(py, "numpy"))?;
    numpy_mod.call_method1(intern!(py, "concatenate"), (py_arrays,))
}
