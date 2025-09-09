use arrow_arith::numeric;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3Array;
use pyo3_arrow::input::AnyDatum;
use pyo3_arrow::PyArray;

#[pyfunction]
pub fn add(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::add(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn add_wrapping(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::add_wrapping(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn div(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::div(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn mul(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::mul(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn mul_wrapping(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::mul_wrapping(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn neg(array: PyArray) -> PyArrowResult<Arro3Array> {
    Ok(numeric::neg(array.as_ref())?.into())
}

#[pyfunction]
pub fn neg_wrapping(array: PyArray) -> PyArrowResult<Arro3Array> {
    Ok(numeric::neg_wrapping(array.as_ref())?.into())
}

#[pyfunction]
pub fn rem(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::rem(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn sub(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::sub(&lhs, &rhs)?.into())
}

#[pyfunction]
pub fn sub_wrapping(lhs: AnyDatum, rhs: AnyDatum) -> PyArrowResult<Arro3Array> {
    Ok(numeric::sub_wrapping(&lhs, &rhs)?.into())
}
