use arrow_arith::numeric;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyDatum;
use pyo3_arrow::PyArray;

#[pyfunction]
pub fn add(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::add(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn add_wrapping(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::add_wrapping(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn div(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::div(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn mul(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::mul(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn mul_wrapping(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::mul_wrapping(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn neg(py: Python, array: PyArray) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::neg(array.as_ref())?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn neg_wrapping(py: Python, array: PyArray) -> PyArrowResult<PyObject> {
    Ok(
        PyArray::from_array_ref(numeric::neg_wrapping(array.as_ref())?)
            .to_arro3(py)?
            .unbind(),
    )
}

#[pyfunction]
pub fn rem(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::rem(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn sub(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::sub(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}

#[pyfunction]
pub fn sub_wrapping(py: Python, lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<PyObject> {
    Ok(PyArray::from_array_ref(numeric::sub_wrapping(&lhs, &rhs)?)
        .to_arro3(py)?
        .unbind())
}
