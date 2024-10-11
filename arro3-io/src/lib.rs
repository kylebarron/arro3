use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_object_store::PyObjectStore;
use url::Url;

mod csv;
mod ipc;
mod json;
mod parquet;
mod utils;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pyfunction]
pub fn accept_store(store: PyObjectStore) {
    dbg!(store.into_inner().to_string());
    // todo!()
}

#[pyfunction]
pub fn from_url(py: Python, url: String) -> PyResult<PyObject> {
    let (store, path) = object_store::parse_url(&Url::parse(&url).unwrap()).unwrap();
    dbg!(store.to_string());
    dbg!(path.to_string());

    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let resp = store.get_opts(&path, Default::default()).await.unwrap();
        let bytes = resp.bytes().await.unwrap();
        let v = bytes.to_vec();
        Ok(v)
    })?;
    Ok(fut.into())
}

struct BytesWrapper(bytes::Bytes);

impl IntoPy<PyObject> for BytesWrapper {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyBytes::new_bound(py, &self.0).to_object(py)
    }
}

#[pyfunction]
pub fn read_path(py: Python, store: PyObjectStore, path: String) -> PyResult<PyObject> {
    // let (store, path) = object_store::parse_url(&Url::parse(&url).unwrap()).unwrap();
    // dbg!(store.to_string());
    // dbg!(path.to_string());

    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let resp = store
            .inner()
            .get_opts(&path.into(), Default::default())
            .await
            .unwrap();
        let bytes = resp.bytes().await.unwrap();
        Ok(BytesWrapper(bytes))
    })?;
    Ok(fut.into())
}

#[pymodule]
fn _io(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<pyo3_object_store::PyS3Store>()?;
    m.add_wrapped(wrap_pyfunction!(accept_store))?;
    m.add_wrapped(wrap_pyfunction!(from_url))?;
    m.add_wrapped(wrap_pyfunction!(read_path))?;

    m.add_wrapped(wrap_pyfunction!(csv::infer_csv_schema))?;
    m.add_wrapped(wrap_pyfunction!(csv::read_csv))?;
    m.add_wrapped(wrap_pyfunction!(csv::write_csv))?;

    m.add_wrapped(wrap_pyfunction!(json::infer_json_schema))?;
    m.add_wrapped(wrap_pyfunction!(json::read_json))?;
    m.add_wrapped(wrap_pyfunction!(json::write_json))?;
    m.add_wrapped(wrap_pyfunction!(json::write_ndjson))?;

    m.add_wrapped(wrap_pyfunction!(ipc::read_ipc))?;
    m.add_wrapped(wrap_pyfunction!(ipc::read_ipc_stream))?;
    m.add_wrapped(wrap_pyfunction!(ipc::write_ipc))?;
    m.add_wrapped(wrap_pyfunction!(ipc::write_ipc_stream))?;

    m.add_wrapped(wrap_pyfunction!(parquet::read_parquet))?;
    m.add_wrapped(wrap_pyfunction!(parquet::write_parquet))?;

    Ok(())
}
