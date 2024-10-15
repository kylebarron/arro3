use pyo3::intern;
use pyo3::prelude::*;

use crate::{PyAzureStore, PyGCSStore, PyHttpStore, PyS3Store};

/// Export the default Python API as a submodule named "store" within the given parent module
pub fn register_store_module(
    py: Python<'_>,
    parent_module: &Bound<'_, PyModule>,
    parent_module_str: &str,
) -> PyResult<()> {
    let full_module_string = format!("{}.store", parent_module_str);
    let child_module = PyModule::new_bound(parent_module.py(), full_module_string.as_str())?;

    // https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
    let sys_mod = py.import_bound(intern!(py, "sys"))?;
    let modules = sys_mod.getattr(intern!(py, "modules"))?;
    modules.set_item(full_module_string, child_module.to_object(py))?;

    child_module.add_class::<PyS3Store>()?;
    child_module.add_class::<PyAzureStore>()?;
    child_module.add_class::<PyGCSStore>()?;
    child_module.add_class::<PyHttpStore>()?;

    parent_module.add_submodule(&child_module)
}
