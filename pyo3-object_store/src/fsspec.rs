use std::fmt::Display;
use std::sync::Arc;

// use bytes::{Buf, Bytes};
use futures::future::{BoxFuture, FutureExt};
use object_store::path::Path;
use object_store::ObjectStore;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

/// A wrapper around an Async fsspec filesystem instance
#[derive(Debug)]
pub struct AsyncFsspec {
    fs: PyObject,
    path: String,
    file_length: usize,
}

impl Display for AsyncFsspec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: get the __repr__ of the underlying fsspec object, and print like
        // "RustWrapper(fsspec_repr)"
        write!(f, "AsyncFsspec")
    }
}

impl AsyncFsspec {
    pub fn new(fs: PyObject, path: String, file_length: usize) -> Self {
        // TODO: verify isinstance of fsspec base class
        // TODO: verify is async
        Self {
            fs,
            path,
            file_length,
        }
    }
}

impl ObjectStore for AsyncFsspec {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<object_store::PutResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // put_file
        todo!()
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: object_store::PutMultipartOpts,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = object_store::Result<Box<dyn object_store::MultipartUpload>>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: object_store::GetOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<object_store::GetResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        async move {
            Python::with_gil(|py| -> PyResult<_> {
                let path = location.to_string();
                let args = PyTuple::new_bound(py, vec![path]);

                let coroutine = self.fs.call_method1(py, intern!(py, "_rm"), args)?;
                pyo3_asyncio_0_21::tokio::into_future(coroutine.bind(py).clone())
            })
            .unwrap()
            .await
            .unwrap();

            Ok(())
        }
        .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        todo!()
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<object_store::ListResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        // .copy
        todo!()
    }

    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = object_store::Result<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
