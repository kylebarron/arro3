#[cfg(feature = "async")]
mod r#async;
mod sync;

#[cfg(feature = "async")]
pub(crate) use r#async::read_parquet_async;
pub(crate) use sync::{read_parquet, write_parquet};
