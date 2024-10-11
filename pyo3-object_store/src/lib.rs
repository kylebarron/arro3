#![doc = include_str!("../README.md")]
// #![deny(missing_docs)]

mod aws;
mod store;

pub use aws::S3Store;
pub use store::AnyObjectStore;
