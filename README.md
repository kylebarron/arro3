# arro3

A minimal Python library for [Apache Arrow](https://arrow.apache.org/docs/index.html), binding to the [Rust Arrow implementation](https://github.com/apache/arrow-rs).

## Why another Arrow library?

[pyarrow](https://arrow.apache.org/docs/python/index.html) is the reference Arrow implementation in Python, but there are a few reasons for `arro3` to exist:

- **Lightweight**. pyarrow is 100MB on disk, plus 35MB for its required numpy dependency. `arro3-core` is around 1MB on disk with no required dependencies.
- **Minimal**. The core library (`arro3-core`) has a very small scope. Other functionality, such as compute kernels, will be distributed in other namespace packages.
- **Modular**. The [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) makes it easier to create small Arrow libraries that communicate via zero-copy data transfer. arro3's Python functions accept Arrow data from any Python Arrow library that implements the PyCapsule interface, including `pyarrow` and `nanoarrow`.
- **Extensible**. Over time, can connect to [compute kernels provided by the Rust Arrow implementation](https://docs.rs/arrow/latest/arrow/compute/index.html).
- **Compliant**. Full support for the Arrow specification*, including extension types. (*Limited to what the Arrow Rust crate supports, which does not yet support Arrow view types.)

## Drawbacks

In general, arro3 isn't designed for _constructing_ arrow data from other formats, but should enable users to manage arrow data created by other Arrow-compatible libraries. arro3 does not implement conversion of arbitrary Python objects to Arrow. This is complex and well served by other libraries (e.g. pyarrow).

## Using from Rust

Refer to [pyo3-arrow documentation](https://docs.rs/pyo3-arrow).
