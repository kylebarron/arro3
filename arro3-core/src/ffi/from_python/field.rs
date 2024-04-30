use crate::ffi::from_python::utils::import_arrow_c_schema;
use crate::field::PyField;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

// TODO: need to update import_arrow_c_schema to not always coerce to a Schema
// impl<'a> FromPyObject<'a> for PyField {
//     fn extract(ob: &'a PyAny) -> PyResult<Self> {
//         let schema = import_arrow_c_schema(ob)?;
//         schema.fi
//         Ok(Self(schema))
//     }
// }
