use pyo3::prelude::*;
use pyo3_arrow::PyDataType;

#[pyfunction]
pub fn parse_data_type<'py>(
    py: Python<'py>,
    python_input: PyDataType,
) -> PyResult<Bound<'py, PyAny>> {
    let arrow_data_type = python_input.into_inner();
    println!("Parsed Arrow DataType: {:?}", arrow_data_type);
    PyDataType::new(arrow_data_type).into_arro3(py)
}
