# arro3

[![PyPI][pypi_arro3_core]][pypi_link_arro3_core]

[pypi_arro3_core]: https://badge.fury.io/py/arro3-core.svg
[pypi_link_arro3_core]: https://pypi.org/project/arro3-core/

A minimal Python library for [Apache Arrow](https://arrow.apache.org/docs/index.html), binding to the [Rust Arrow implementation](https://github.com/apache/arrow-rs).

arro3 features:

- Classes to manage and operate on Arrow data.
- Streaming-capable readers and writers for Parquet, Arrow IPC, JSON, and CSV.
- Streaming compute functions. All relevant compute functions accept streams of input data and return a stream of output data. This means you can transform larger-than-memory data files

## Install

arro3 is distributed with [namespace packaging](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/), meaning that individual submodules are distributed separately to PyPI and can be used in isolation.

```
pip install arro3-core arro3-io arro3-compute
```

arro3 is also on Conda and can be installed with [pixi](https://github.com/prefix-dev/pixi)

```
pixi add arro3-core arro3-io arro3-compute
```

## Using

Consult the [documentation](https://kylebarron.dev/arro3/latest/).

## Why another Arrow library?

[pyarrow](https://arrow.apache.org/docs/python/index.html) is the reference Arrow implementation in Python, and is generally great, but there are a few reasons for `arro3` to exist:

- **Lightweight**. on MacOS, pyarrow is 100MB on disk, plus 35MB for its required numpy dependency. `arro3-core` is around 7MB on disk with no required dependencies.
- **Minimal**. The core library (`arro3-core`) has a smaller scope than pyarrow. It includes classes to manage and operate on Arrow data, including `Table`, `RecordBatch`, `Array`, `ChunkedArray`, `RecordBatchReader`, `Schema`, `Field`, `DataType`. But, for example, it has a single `Array` class, while pyarrow has an `Int8Array`, `Int16Array`, and so on.

    `arro3-core` will likely not grow much over time. Other functionality, such as file format readers and writers and compute kernels, will be distributed in other namespace packages, such as `arro3-io` and `arro3-compute`.
- **Modular**. The [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) makes it easy to create small Arrow libraries that communicate via zero-copy data transfer. arro3's Python functions accept Arrow data from _any Python library that implements the Arrow PyCapsule Interface_, including `pyarrow`, `polars` (v1.2+), `pandas` (v2.2+), `nanoarrow`, [and more](https://github.com/apache/arrow/issues/39195#issuecomment-2245718008).

    Every functional API in arro3 accepts Arrow data from _any_ Python library. So you can pass a `pyarrow.Table` directly into `arro3.io.write_parquet`, and it'll _just work_.

- **Extensible**. arro3 and its sister library pyo3-arrow make it easier for Rust Arrow libraries to be exported to Python. Over time, arro3 can connect to more [compute kernels](https://docs.rs/arrow/latest/arrow/compute/index.html) provided by the Rust Arrow implementation.
- **Compliant**. Full support for the Arrow specification, including extension types. (Arrow's new view types will be supported from the next Rust `arrow` release).
- **Streaming-first**. All compute and IO functionality is streaming-based with lazy iterators, so you can work with larger-than-memory data.

    For example, `arro3.io.read_parquet` returns a `RecordBatchReader`, an iterator that yields Arrow RecordBatches. This `RecordBatchReader` can then be passed into any compute function to transform to another `RecordBatchReader`, that in turn can be passed into `arro3.io.write_parquet`, at which point both iterators are used.

    Note that if you _do_ want to materialize data in memory, you should call `RecordBatchReader.read_all()` or pass the `RecordBatchReader` to `arro3.core.Table()`.

- **Pyodide support**. arro3 works in [Pyodide](https://github.com/pyodide/pyodide), a WebAssembly version of Python, and can integrate with other Python packages that implement the Arrow PyCapsule Interface without a pyarrow dependency.
- **Type hints**. Type hints are provided for all functionality, making it easier to code in an IDE.

## Drawbacks

In general, arro3 wraps what already exists in arrow-rs. This ensures that arro3 has a reasonable maintenance burden.

arro3 shies away from implementing complete conversion of arbitrary Python objects (or pandas DataFrames) to Arrow. This is complex and well served by other libraries (e.g. pyarrow). But arro3 should provide a minimal and efficient toolbox for to interoperate with other Arrow-compatible libraries.

## Using from Rust

You can use [pyo3-arrow](https://crates.io/crates/pyo3-arrow) to simplify passing Arrow data between Rust and Python. Refer to [its documentation](https://docs.rs/pyo3-arrow).
