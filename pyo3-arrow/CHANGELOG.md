# Changelog

## Unreleased

- Rename `to_pyarrow` to `into_pyarrow` because it consumes self. #391

## [0.11.0] - 2025-08-01

- Bump to `arrow` 56.
- Improved array formatting of `PyArray`, `PyChunkedArray`, `PyRecordBatch`, `PyScalar`, and `PyTable` in the `__repr__` exposed to Python if you re-export these types in your Python library #335.

## [0.10.1] - 2025-05-19

- Fix pyo3-arrow error when importing buffer protocol object. We were incorrectly validating the stride. But since we already checked the buffer to be C-contiguous, we don't need to check for strides again. (Part of #328).
- Make a workaround to the upstream regression/change in https://github.com/apache/arrow-rs/pull/7247 that caused the test that checks we can import a zero-length record batch to fail. This also makes my code cleaner by utilizing the upstream `make_array` now that we have a fix to https://github.com/apache/arrow-rs/issues/6151 (Part of #328).

## [0.10.0] - 2025-05-19

- Bump to pyo3 0.25.

## [0.9.0] - 2025-04-11

- Bump to arrow 55.
- Minimize arrow crate dependency surface (#318). Don't depend on `arrow`; instead depend on individual arrow crates for a smaller dependency tree and faster compile times.

## [0.8.0] - 2025-03-17

### What's Changed

- Bump to pyo3 0.24.

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.7.2...pyo3-arrow-v0.8.0

## [0.7.2] - 2025-02-22

### What's Changed

- Add `into_arro3` method https://github.com/kylebarron/arro3/pull/289

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.7.1...pyo3-arrow-v0.7.2

## [0.7.1] - 2025-02-21

### What's Changed

- Bump to pyo3 0.23.4 https://github.com/kylebarron/arro3/pull/287

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.7.0...pyo3-arrow-v0.7.1

## [0.7.0] - 2025-02-13

### What's Changed

- Add frozen parameter to pyclass macro by @kylebarron in https://github.com/kylebarron/arro3/pull/282
- Avoid returning `PyObject` and add wrapper types for arro3 export by @kylebarron in https://github.com/kylebarron/arro3/pull/269
- Bump to arrow 54 by @kylebarron in https://github.com/kylebarron/arro3/pull/285

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.6.0...pyo3-arrow-v0.7.0

## [0.6.0] - 2024-12-04

### What's Changed

- Bump pyo3-arrow to pyo3 0.23 by @kylebarron in https://github.com/kylebarron/arro3/pull/265
- Add test for segmentation fault when converting arro3.core.Array to pyarrow.Array at interpreter exit by @3ok in https://github.com/kylebarron/arro3/pull/236
- Implement FromPyObject for PyArrowBuffer by @kylebarron in https://github.com/kylebarron/arro3/pull/241
- rust constructor for PyArrowBuffer by @kylebarron in https://github.com/kylebarron/arro3/pull/242

### New Contributors

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.5.1...pyo3-arrow-v0.6.0

## [0.5.1] - 2024-10-14

### What's Changed

- Fix `no-default-features` for pyo3-arrow by @kylebarron in https://github.com/kylebarron/arro3/pull/232
- Custom drop on PyBufferWrapper by @kylebarron in https://github.com/kylebarron/arro3/pull/231

**Full Changelog**: https://github.com/kylebarron/arro3/compare/pyo3-arrow-v0.5.0...pyo3-arrow-v0.5.1

## [0.5.0] - 2024-10-11

### What's Changed

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
