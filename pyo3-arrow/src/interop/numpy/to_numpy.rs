use arrow::array::AsArray;
use arrow::datatypes::*;
use arrow_array::Array;
use arrow_schema::DataType;
use numpy::ToPyArray;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyAnyMethods;
use pyo3::{intern, PyObject, PyResult, Python, ToPyObject};

pub fn to_numpy(py: Python, arr: &dyn Array) -> PyResult<PyObject> {
    if arr.null_count() > 0 {
        return Err(PyValueError::new_err(
            "Cannot create numpy array from arrow array with nulls.",
        ));
    }

    macro_rules! impl_primitive {
        ($arrow_type:ty) => {
            arr.as_primitive::<$arrow_type>()
                .values()
                .to_pyarray_bound(py)
                .to_object(py)
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
            bools.to_pyarray_bound(py).to_object(py)
        }
        _ => todo!(),
    };
    Ok(result)
}

pub fn chunked_to_numpy(py: Python, arrs: &[&dyn Array]) -> PyResult<PyObject> {
    let py_arrays = arrs
        .iter()
        .map(|arr| to_numpy(py, *arr))
        .collect::<PyResult<Vec<_>>>()?;

    let numpy_mod = py.import_bound(intern!(py, "numpy"))?;
    Ok(numpy_mod
        .call_method1(intern!(py, "concatenate"), (py_arrays,))?
        .to_object(py))
}
