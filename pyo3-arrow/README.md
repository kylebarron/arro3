# pyo3-arrow

[![crates.io version][crates.io_badge]][crates.io_link]
[![docs.rs docs][docs.rs_badge]][docs.rs_link]

[crates.io_badge]: https://img.shields.io/crates/v/pyo3-arrow.svg
[crates.io_link]: https://crates.io/crates/pyo3-arrow
[docs.rs_badge]: https://docs.rs/pyo3-arrow/badge.svg
[docs.rs_link]: https://docs.rs/pyo3-arrow

Lightweight [Apache Arrow](https://arrow.apache.org/docs/index.html) integration for [pyo3](https://pyo3.rs/). Designed to make it easier for Rust libraries to add interoperable, zero-copy Python bindings.

Specifically, pyo3-arrow implements zero-copy FFI conversions between Python objects and Rust representations using the `arrow` crate. This relies heavily on the [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) for seamless interoperability across the Python Arrow ecosystem.

## Usage

We can wrap a function to be used in Python with just a few lines of code.

When you use a struct defined in `pyo3_arrow` as an argument to your function, it will automatically convert user input to a Rust `arrow` object via zero-copy FFI. Then once you're done, call `to_arro3` or `to_pyarrow` to export the data back to Python.

```rs
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

/// Take elements by index from an Array, creating a new Array from those
/// indexes.
#[pyfunction]
pub fn take(py: Python, values: PyArray, indices: PyArray) -> PyArrowResult<PyObject> {
    let (values_array, values_field) = values.into_inner();
    let (indices, _) = indices.into_inner();

    // We can call py.allow_threads to ensure the GIL is released during our
    // operations
    // This example just wraps `arrow_select::take::take`
    let output_array =
        py.allow_threads(|| arrow_select::take::take(&values_array, &indices, None))?;

    // Construct a PyArray and export it to the arro3 Python Arrow
    // implementation
    PyArray::new(output_array, values_field).to_arro3(py)
}
```

Then on the Python side, we can call this function (exported via `arro3.compute.take`):

```py
import pyarrow as pa
from arro3.compute import take

arr = pa.array([2, 3, 0, 1])
output = take(arr, arr)
output
# <arro3.core._rust.Array at 0x10787b510>
pa.array(output)
<pyarrow.lib.Int64Array object at 0x10aa11000>
[
  0,
  1,
  2,
  3
]
```

In this example, we use pyarrow to create the original array and to view the result, but the use of pyarrow is not required. It does, at least, show how the Arrow PyCapsule Interface makes it seamless to share these Arrow objects between Python Arrow implementations.

### Using Arrow data as input

Just include one of the pyo3-arrow structs in your function signature, and user input will be transparently converted

This uses the Arrow PyCapsule Interface. But note that that only defines _three_ methods, and `pyo3-arrow` contains more the three structs. Several structs are overloaded and use the same underlying transport mechanism.

For example, `PySchema` and `PyField` both use the `__arrow_c_schema__` mechanism, but with different behavior. The former expects the transported field to be a struct type, and its children get unpacked to be the fields of the schema, while the latter has no constraint and passes a field through as-is. `PySchema` will error if the passed field is not of struct type.

| Struct name | Unpacks struct field |
| ----------- | -------------------- |
| `PySchema`  | Yes                  |
| `PyField`   | No                   |

`PyArray` and `PyRecordBatch` both use the `__arrow_c_array__` mechanism:

| Struct name     | Unpacks `StructArray` to `RecordBatch` |
| --------------- | -------------------------------------- |
| `PyRecordBatch` | Yes                                    |
| `PyArray`       | No                                     |

`PyTable`, `PyChunkedArray`, and `PyRecordBatchReader` all use the `__arrow_c_stream__` mechanism:

| Struct name           | Unpacks `StructArray` to `RecordBatch` | Materializes in memory |
| --------------------- | -------------------------------------- | ---------------------- |
| `PyTable`             | Yes                                    | Yes                    |
| `PyRecordBatchReader` | Yes                                    | No                     |
| `PyChunkedArray`      | No                                     | Yes                    |

### Returning Arrow data back to Python

#### Using `arro3.core`

[`arro3.core`](https://github.com/kylebarron/arro3) is a very minimal Python Arrow implementation, designed to be lightweight (<1MB) and relatively stable. In comparison, pyarrow is on the order of ~100MB.

You must depend on the `arro3-core` Python package; then you can use the `to_arro3` method of each exported Arrow object to pass the data into an `arro3.core` class.

| Rust struct           | arro3 class                    |
| --------------------- | ------------------------------ |
| `PyField`             | `arro3.core.Field`             |
| `PySchema`            | `arro3.core.Schema`            |
| `PyArray`             | `arro3.core.Array`             |
| `PyRecordBatch`       | `arro3.core.RecordBatch`       |
| `PyChunkedArray`      | `arro3.core.ChunkedArray`      |
| `PyTable`             | `arro3.core.Table`             |
| `PyRecordBatchReader` | `arro3.core.RecordBatchReader` |

#### Using `pyarrow`

[`pyarrow`](https://arrow.apache.org/docs/python/index.html), the canonical Python Arrow implementation, is a very large dependency. It's roughly 100MB in size on its own, plus 35MB more for its hard dependency on numpy. However, `numpy` is very likely already in the user environment, and `pyarrow` is quite common as well, so requiring a `pyarrow` dependency may not be a problem.

In this case, you must depend on `pyarrow` and you can use the `to_pyarrow` method of Python structs to return data to Python. This requires `pyarrow>=14` (`pyarrow>=15` is required to return `PyRecordBatchReader`).

| Rust struct           | pyarrow class               |
| --------------------- | --------------------------- |
| `PyField`             | `pyarrow.Field`             |
| `PySchema`            | `pyarrow.Schema`            |
| `PyArray`             | `pyarrow.Array`             |
| `PyRecordBatch`       | `pyarrow.RecordBatch`       |
| `PyChunkedArray`      | `pyarrow.ChunkedArray`      |
| `PyTable`             | `pyarrow.Table`             |
| `PyRecordBatchReader` | `pyarrow.RecordBatchReader` |

#### Using `nanoarrow`

[`nanoarrow`](https://arrow.apache.org/nanoarrow/latest/index.html) is an alternative Python library for working with Arrow data. It's similar in goals to arro3, but is written in C instead of Rust. Additionally, it has a smaller type system than `pyarrow` or `arro3`, with logical arrays and record batches both represented by the `nanoarrow.Array` class.

In this case, you must depend on `nanoarrow` and you can use the `to_nanoarrow` method of Python structs to return data to Python.

| Rust struct           | nanoarrow class         |
| --------------------- | ----------------------- |
| `PyField`             | `nanoarrow.Schema`      |
| `PySchema`            | `nanoarrow.Schema`      |
| `PyArray`             | `nanoarrow.Array`       |
| `PyRecordBatch`       | `nanoarrow.Array`       |
| `PyChunkedArray`      | `nanoarrow.ArrayStream` |
| `PyTable`             | `nanoarrow.ArrayStream` |
| `PyRecordBatchReader` | `nanoarrow.ArrayStream` |

## Why not use arrow-rs's Python integration?

arrow-rs has [some existing Python integration](https://docs.rs/arrow/latest/arrow/pyarrow/index.html), but there are a few reasons why I created `pyo3-arrow`:

- In my opinion arrow-rs is too tightly connected to pyo3 and pyarrow. pyo3 releases don't line up with arrow-rs's release cadence, which means it could be a bit of a wait to use the latest pyo3 version with arrow-rs, especially with arrow-rs [waiting longer to release breaking changes](https://github.com/apache/arrow-rs#release-versioning-and-schedule).
- arrow-rs only supports returning data as pyarrow classes. pyarrow is a very large dependency and some projects may wish not to use it. Now that the Arrow PyCapsule interface exists, it's possible to have a modular approach, where a very small library contains core Arrow objects, and works seamlessly with other libraries.
- arrow-rs's Python FFI integration does not support extension types, because it omits field metadata when constructing an `Arc<dyn Array>`. pyo3-arrow gets around this by storing both an `ArrayRef` (`Arc<dyn Array>`) and a `FieldRef` (`Arc<Field>`) in a `PyArray` struct.
- arrow-rs doesn't have a way to interface with `Table` and `ChunkedArray` constructs. It suggests to use a `RecordBatchReader` instead of a `Table`, but regardless arrow-rs has no ability to work with an Arrow stream of bare arrays that are not record batches.

## Scope

pyo3-arrow defines Rust wrappers for Arrow concepts that are ABI stable. This means that some Arrow concepts, like `DataType`, are not implemented. `DataType` is not a concept that can be shared across Arrow implementations. (It can be shared as part of a `Field` or `Schema`, but not on its own).
