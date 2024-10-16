use core::time::Duration;
use std::sync::Arc;

use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path;
use object_store::signer::Signer;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3_object_store::{PyAzureStore, PyGCSStore, PyS3Store};
use url::Url;

use crate::runtime::get_runtime;

#[derive(Debug)]
pub(crate) enum SignCapableStore {
    S3(Arc<AmazonS3>),
    Gcs(Arc<GoogleCloudStorage>),
    Azure(Arc<MicrosoftAzure>),
}

impl<'py> FromPyObject<'py> for SignCapableStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self::S3(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self::Gcs(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self::Azure(store.borrow().as_ref().clone()))
        } else {
            let py = ob.py();
            // Check for object-store instance from other library
            let cls_name = ob
                .getattr(intern!(py, "__class__"))?
                .getattr(intern!(py, "__name__"))?
                .extract::<PyBackedStr>()?;
            if [
                "AzureStore",
                "GCSStore",
                "HTTPStore",
                "LocalStore",
                "MemoryStore",
                "S3Store",
            ]
            .contains(&cls_name.as_ref())
            {
                return Err(PyValueError::new_err("You must use an object store instance exported from **the same library** as this function. They cannot be used across libraries.\nThis is because object store instances are compiled with a specific version of Rust and Python." ));
            }

            Err(PyValueError::new_err(format!(
                "Expected an S3Store, GCSStore, or AzureStore instance, got {}",
                ob.repr()?
            )))
        }
    }
}

impl Signer for SignCapableStore {
    fn signed_url<'life0, 'life1, 'async_trait>(
        &'life0 self,
        method: http::Method,
        path: &'life1 object_store::path::Path,
        expires_in: Duration,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<Url>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::S3(inner) => inner.signed_url(method, path, expires_in),
            Self::Gcs(inner) => inner.signed_url(method, path, expires_in),
            Self::Azure(inner) => inner.signed_url(method, path, expires_in),
        }
    }
}

pub(crate) struct PyMethod(http::Method);

impl<'py> FromPyObject<'py> for PyMethod {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?;
        let method = match s.as_ref() {
            "GET" => http::Method::GET,
            "PUT" => http::Method::PUT,
            "POST" => http::Method::POST,
            "HEAD" => http::Method::HEAD,
            "PATCH" => http::Method::PATCH,
            "TRACE" => http::Method::TRACE,
            "DELETE" => http::Method::DELETE,
            "OPTIONS" => http::Method::OPTIONS,
            "CONNECT" => http::Method::CONNECT,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported HTTP method {}",
                    other
                )))
            }
        };
        Ok(Self(method))
    }
}

pub(crate) struct PyUrl(url::Url);

impl IntoPy<String> for PyUrl {
    fn into_py(self, _py: Python<'_>) -> String {
        self.0.into()
    }
}

impl IntoPy<PyObject> for PyUrl {
    fn into_py(self, py: Python<'_>) -> PyObject {
        String::from(self.0).into_py(py)
    }
}

#[pyfunction]
pub(crate) fn sign_url(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    path: String,
    expires_in: Duration,
) -> PyResult<String> {
    let runtime = get_runtime(py)?;
    let method = method.0;

    let signed_url = py.allow_threads(|| {
        runtime
            .block_on(store.signed_url(method, &path.into(), expires_in))
            .unwrap()
    });
    Ok(signed_url.into())
}

#[pyfunction]
pub(crate) fn sign_url_async(
    py: Python,
    store: SignCapableStore,
    method: PyMethod,
    path: String,
    expires_in: Duration,
) -> PyResult<PyObject> {
    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        sign_url_async_inner(store, method.0, path.into(), expires_in).await
    })?;
    Ok(fut.into())
}

async fn sign_url_async_inner(
    store: SignCapableStore,
    method: http::Method,
    path: Path,
    expires_in: Duration,
) -> PyResult<PyUrl> {
    let url = store.signed_url(method, &path, expires_in).await.unwrap();
    Ok(PyUrl(url))
}
