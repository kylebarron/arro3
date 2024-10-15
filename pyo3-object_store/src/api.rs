use pyo3::intern;
use pyo3::prelude::*;

use crate::{PyAzureStore, PyGCSStore, PyHttpStore, PyLocalStore, PyMemoryStore, PyS3Store};

/// Export the default Python API as a submodule named "store" within the given parent module
// https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
// https://github.com/PyO3/pyo3/issues/759#issuecomment-977835119
pub fn register_store_module(
    py: Python<'_>,
    parent_module: &Bound<'_, PyModule>,
    parent_module_str: &str,
) -> PyResult<()> {
    let full_module_string = format!("{}.store", parent_module_str);

    let child_module = PyModule::new_bound(parent_module.py(), "store")?;

    child_module.add_class::<PyAzureStore>()?;
    child_module.add_class::<PyGCSStore>()?;
    child_module.add_class::<PyHttpStore>()?;
    child_module.add_class::<PyLocalStore>()?;
    child_module.add_class::<PyMemoryStore>()?;
    child_module.add_class::<PyS3Store>()?;

    parent_module.add_submodule(&child_module)?;

    py.import_bound(intern!(py, "sys"))?
        .getattr(intern!(py, "modules"))?
        .set_item(full_module_string.as_str(), child_module.to_object(py))?;

    // needs to be set *after* `add_submodule()`
    child_module.setattr("__name__", full_module_string)?;

    Ok(())
}
