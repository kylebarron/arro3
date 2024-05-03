use std::sync::Arc;

use crate::ffi::from_python::utils::import_arrow_c_schema;
use crate::field::PyField;
use arrow_schema::Field;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyField {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let schema_ptr = import_arrow_c_schema(ob)?;
        let field =
            Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(field)))
    }
}
