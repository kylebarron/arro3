#![doc = include_str!("../README.md")]
// #![deny(missing_docs)]

mod aws;
mod azure;
mod client;
mod gcp;
mod http;
mod retry;
mod store;

pub use aws::PyS3Store;
pub use azure::PyAzureStore;
pub use client::{PyClientConfigKey, PyClientOptions};
pub use gcp::PyGCSStore;
pub use http::PyHttpStore;
pub use store::AnyObjectStore;
