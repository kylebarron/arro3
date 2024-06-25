# pyo3-arrow

[![crates.io version][crates.io_badge]][crates.io_link]
[![docs.rs docs][docs.rs_badge]][docs.rs_link]

[crates.io_badge]: https://img.shields.io/crates/v/pyo3-arrow.svg
[crates.io_link]: https://crates.io/crates/pyo3-arrow
[docs.rs_badge]: https://docs.rs/pyo3-arrow/badge.svg
[docs.rs_link]: https://docs.rs/pyo3-arrow

Arrow integration for pyo3.

pyo3-arrow is a crate that facilitates the zero-copy transfer of Apache Arrow memory between Python and Rust. It implements zero-copy FFI conversions between Python objects and Rust representations using the `arrow` crate.

This relies heavily on the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) for seamless interoperability across the Python Arrow ecosystem.

<!-- ## Why not use `arrow`'s Python integration? -->

