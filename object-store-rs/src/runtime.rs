use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use pyo3_object_store::PyObjectStore;
use tokio::runtime::Runtime;

static RUNTIME: GILOnceCell<Runtime> = GILOnceCell::new();

/// Get the tokio runtime for sync requests
pub(crate) fn get_runtime(py: Python<'_>) -> PyResult<&Runtime> {
    RUNTIME.get_or_try_init(py, || {
        Runtime::new().map_err(|err| {
            PyValueError::new_err(format!("Could not create tokio runtime. {}", err))
        })
    })
}

#[pyfunction]
pub fn get(py: Python, store: PyObjectStore, location: String) -> PyResult<Bound<PyBytes>> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();

    let buf = py.allow_threads(|| {
        let get_result = runtime.block_on(store.get(&location.into())).unwrap();
        runtime.block_on(get_result.bytes()).unwrap()
    });

    Ok(PyBytes::new_bound(py, &buf))
}
