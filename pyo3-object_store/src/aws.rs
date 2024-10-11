use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass]
pub struct S3Store(Arc<AmazonS3>);

impl S3Store {
    pub fn inner(&self) -> &Arc<AmazonS3> {
        &self.0
    }
}

#[pymethods]
impl S3Store {
    #[classmethod]
    fn from_session(
        _cls: &Bound<PyType>,
        py: Python,
        session: &Bound<PyAny>,
        bucket: String,
    ) -> PyResult<Self> {
        // boto3.Session has a region_name attribute, but botocore.session.Session does not.
        let region = if let Ok(region) = session.getattr(intern!(py, "region_name")) {
            Some(region.extract::<String>()?)
        } else {
            None
        };

        let creds = session.call_method0(intern!(py, "get_credentials"))?;
        let frozen_creds = creds.call_method0(intern!(py, "get_frozen_credentials"))?;

        let access_key = frozen_creds
            .getattr(intern!(py, "access_key"))?
            .extract::<Option<String>>()?;
        let secret_key = frozen_creds
            .getattr(intern!(py, "secret_key"))?
            .extract::<Option<String>>()?;
        let token = frozen_creds
            .getattr(intern!(py, "token"))?
            .extract::<Option<String>>()?;

        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);
        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        if let Some(access_key) = access_key {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = secret_key {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(token) = token {
            builder = builder.with_token(token);
        }

        Ok(Self(Arc::new(builder.build().unwrap())))
    }
}
