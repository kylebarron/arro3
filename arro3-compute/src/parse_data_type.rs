use pyo3::prelude::*;
use pyo3_arrow::PyDataType;

#[pyfunction]
pub fn parse_data_type(data_type: PyDataType) {
    let arrow_data_type = data_type.as_ref();
    println!("Parsed Arrow DataType: {:?}", arrow_data_type);
}
