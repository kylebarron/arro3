use std::sync::Arc;

use crate::ffi::from_python::utils::import_arrow_c_schema;
use crate::schema::PySchema;
use arrow_schema::Schema;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PySchema {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let schema_ptr = import_arrow_c_schema(ob)?;
        let schema =
            Schema::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(schema)))
    }
}
