#![doc = include_str!("../README.md")]
// #![deny(missing_docs)]

mod api;
mod aws;
mod azure;
mod client;
mod gcp;
mod http;
mod retry;
mod store;

pub use api::register_store_module;
pub use aws::PyS3Store;
pub use azure::PyAzureStore;
pub use client::{PyClientConfigKey, PyClientOptions};
pub use gcp::PyGCSStore;
pub use http::PyHttpStore;
pub use store::PyObjectStore;
