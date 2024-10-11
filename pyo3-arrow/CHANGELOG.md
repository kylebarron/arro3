# Changelog

## [0.5.0] - 2024-10-11

## What's Changed

- Bump to pyo3 0.22 by @kylebarron in https://github.com/kylebarron/arro3/pull/226

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.4.0...pyo3-arrow-v0.5.0

## [0.4.0] - 2024-10-03

### Enhancements :magic_wand:

- Zero-copy buffer protocol data import by @kylebarron in https://github.com/kylebarron/arro3/pull/204
  - Handle multi-dimensional buffer protocol input by @kylebarron in https://github.com/kylebarron/arro3/pull/208
- Put buffer protocol behind feature flag by @kylebarron in https://github.com/kylebarron/arro3/pull/215
- Implement `FromPyObject` for `PyScalar` by @kylebarron in https://github.com/kylebarron/arro3/pull/199

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.3.0...pyo3-arrow-v0.4.0

## [0.3.0] - 2024-09-27

### Enhancements :magic_wand:

- Implement casting via Arrow PyCapsule Interface. This means that pyo3-arrow now respects the [`requested_schema` argument](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#schema-requests). If the cast is invalid, the original data is exported.
- New `PyScalar` class for managing Arrow scalar interop.
- `PyArray` and `PyScalar` implement [`Datum`](https://docs.rs/arrow/latest/arrow/array/trait.Datum.html).
- Public API to import Arrow objects from raw PyCapsules. https://github.com/kylebarron/arro3/pull/183
- Bump to Arrow 53.
- Use `thiserror` for the error enum.
- New `AnyDatum` input object for allowing either array or scalar input.

### Fixes :bug:

- Handle RecordBatch import with positive length but no columns. https://github.com/kylebarron/arro3/pull/177

## [0.2.0] - 2024-08-12

### Enhancements :magic_wand:

- New `ArrayReader`. It parallels `RecordBatchReader` but is more general, supporting arbitrary Arrow arrays that do not have to represent a record batch.
- New `AnyArray` enum that supports either `Array` or `ArrayReader` input.
- Improved documentation.

### Fixes :bug:

- Validate Schema/Field when constructing new Array/ChunkedArray/Table (#72)
- Convert `Table::new` to `Table::try_new` and ensure that all batches have the same schema. Similar for `Array::new` and `ChunkedArray::new`.
- Reorder args for `Table::new`

## [0.1.0] - 2024-06-27

- Initial release
