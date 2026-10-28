//! An [`AsyncFileReader`] for reading Parquet files from an [`ObjectStore`].
//!
//! Copied from parquet's
//! [`examples/object_store.rs`](https://github.com/apache/arrow-rs/blob/60.1.0/parquet/examples/object_store.rs),
//! which replaces the deprecated `ParquetObjectReader`. See
//! <https://github.com/apache/arrow-rs/issues/10308>.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

/// An [`AsyncFileReader`] for a location in an [`ObjectStore`]
///
/// This mirrors the example on the [`AsyncFileReader`] trait documentation.
#[derive(Clone, Debug)]
pub(crate) struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ObjectStoreReader {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self { store, path }
    }
}

impl AsyncFileReader for ObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_err)
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    /// Loads the metadata, respecting the provided [`ArrowReaderOptions`] via
    /// [`ParquetMetaDataReader::with_arrow_reader_options`]
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Supports fetching the Parquet footer without knowing the file size upfront,
/// via suffix range requests
impl MetadataSuffixFetch for &mut ObjectStoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let resp = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(to_parquet_err)?;
            resp.bytes().await.map_err(to_parquet_err)
        }
        .boxed()
    }
}
