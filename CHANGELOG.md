# Changelog

This is the changelog for arro3. pyo3-arrow has a separate changelog.

## [0.5.0] - 2025-05-27

### New features :sparkles:

- feat: access value_fields and inner fields by @ion-elgreco in https://github.com/kylebarron/arro3/pull/334
- feat(arro3-core): Basic data rendering in `__repr__` by @kylebarron in https://github.com/kylebarron/arro3/pull/335

### Bug fixes :bug:

- feat(arro3-core): Make `DataType` hashable by @kylebarron in https://github.com/kylebarron/arro3/pull/336
- test(arro3-core): Check that `Schema` is iterable by @kylebarron in https://github.com/kylebarron/arro3/pull/338

### Documentation updates :memo:

- Add sphinx interlinking for `ArrowStreamExportable` by @kylebarron in https://github.com/kylebarron/arro3/pull/339

### New Contributors

- @ion-elgreco made their first contribution in https://github.com/kylebarron/arro3/pull/334

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.4.6...py-v0.5.0

## [0.4.6] - 2025-03-10

### Bug fixes :bug:

- Use 2_24 for aarch64 wheels by @kylebarron in https://github.com/kylebarron/arro3/pull/279
- Allow None as input into Array constructor by @kylebarron in https://github.com/kylebarron/arro3/pull/294
- Fix rendering `__init__` in docs by @kylebarron in https://github.com/kylebarron/arro3/pull/295
- Subclass Buffer type from collections.abc.Buffer by @kylebarron in https://github.com/kylebarron/arro3/pull/297

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.4.5...py-v0.4.6

## [0.4.5] - 2024-12-16

### Bug fixes :bug:

- Build wheels for linux aarch64. https://github.com/kylebarron/arro3/pull/277

## [0.4.4] - 2024-12-09

### Bug fixes :bug:

- Raise IndexError and KeyError for invalid column access https://github.com/kylebarron/arro3/pull/272

## [0.4.3] - 2024-11-21

### What's Changed

- Build wheels for Python 3.13 by @kylebarron in https://github.com/kylebarron/arro3/pull/260
- Raise RuntimeWarning when compiled in debug mode by @kylebarron in https://github.com/kylebarron/arro3/pull/255
- Add `Buffer.to_bytes` by @kylebarron in https://github.com/kylebarron/arro3/pull/251
- Make buffers have a length by @martindurant in https://github.com/kylebarron/arro3/pull/252

## New Contributors

- @martindurant made their first contribution in https://github.com/kylebarron/arro3/pull/252

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.4.2...py-0.4.3

## [0.4.2] - 2024-10-14

### What's Changed

- Ensure total numpy v2 support by @kylebarron in https://github.com/kylebarron/arro3/pull/226
- Fix segfault when releasing buffer protocol object by @kylebarron in https://github.com/kylebarron/arro3/pull/231

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.4.1...py-v0.4.2

## [0.4.1] - 2024-10-07

### What's Changed

- Support for string view and binary view data types (upgrade to `arrow` 53.1) by @kylebarron in https://github.com/kylebarron/arro3/pull/219
- Fix min/max with datetimes with timezones by @kylebarron in https://github.com/kylebarron/arro3/pull/220

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.4.0...py-v0.4.1

## [0.4.0] - 2024-10-03

### Enhancements :magic_wand:

- Zero-copy buffer protocol data import by @kylebarron in https://github.com/kylebarron/arro3/pull/204

  - Handle multi-dimensional buffer protocol input by @kylebarron in https://github.com/kylebarron/arro3/pull/208
  - Prefer zero-copy in from_numpy by @kylebarron in https://github.com/kylebarron/arro3/pull/214

- New compute functions:
  - Add date_part by @kylebarron in https://github.com/kylebarron/arro3/pull/202
  - Aggregate functions: array min, max, sum by @kylebarron in https://github.com/kylebarron/arro3/pull/193
  - Arith functions by @kylebarron in https://github.com/kylebarron/arro3/pull/194
  - More compute functions by @kylebarron in https://github.com/kylebarron/arro3/pull/104
- Implement scalar equality by @kylebarron in https://github.com/kylebarron/arro3/pull/205
- Improved docstrings by @kylebarron in https://github.com/kylebarron/arro3/pull/198
- Add installation section to readme by @kylebarron in https://github.com/kylebarron/arro3/pull/189

### Bug fixes :bug:

- Fix data type bit width by @kylebarron in https://github.com/kylebarron/arro3/pull/190
- Fix return type of `RecordBatch.column` by @kylebarron in https://github.com/kylebarron/arro3/pull/191

## New Contributors

- @3ok made their first contribution in https://github.com/kylebarron/arro3/pull/210

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.3.1...py-v0.4.0

## [0.3.1] - 2024-09-11

### Bug fixes :bug:

- Enable IPC compression by @kylebarron in https://github.com/kylebarron/arro3/pull/187

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.3.0...py-v0.3.1

## [0.3.0] - 2024-08-27

### Enhancements :magic_wand:

- Wheels for pyodide are auto-built on CI and published to Github releases.
- Ensure Parquet schema metadata is added to arrow table by @kylebarron in https://github.com/kylebarron/arro3/pull/137
- Access dictionary array keys and values by @kylebarron in https://github.com/kylebarron/arro3/pull/139
- Support reading Parquet from file objects by @kylebarron in https://github.com/kylebarron/arro3/pull/142
- Implement dictionary encoding by @kylebarron in https://github.com/kylebarron/arro3/pull/136
- Support for Arrow scalars and converting to Python objects by @kylebarron in https://github.com/kylebarron/arro3/pull/159
- Move functional accessors to core module by @kylebarron in https://github.com/kylebarron/arro3/pull/151
- Support for python buffer protocol by @kylebarron in https://github.com/kylebarron/arro3/pull/156

### Bug fixes :bug:

- Fix field metadata propagation by @kylebarron in https://github.com/kylebarron/arro3/pull/150
- Set strip=true for maturin builds by @kylebarron in https://github.com/kylebarron/arro3/pull/155
- Support `__getitem__` with a negative index by @kylebarron in https://github.com/kylebarron/arro3/pull/171
- support f16 in from_numpy by @kylebarron in https://github.com/kylebarron/arro3/pull/154
- Fix writing to file by @kylebarron in https://github.com/kylebarron/arro3/pull/138

**Full Changelog**: https://github.com/kylebarron/arro3/compare/py-v0.2.1...py-v0.3.0

## [0.2.1] - 2024-08-13

### Bug fixes :bug:

- Fix API docs (#131)

## [0.2.0] - 2024-08-13

### Enhancements :magic_wand:

- In general, high parity with pyarrow for most data access and management. Most of the `Table`, `ChunkedArray`, `Array`, `RecordBatchReader`, `Schema`, and `Field` methods and behavior should be similar to pyarrow.
- An `ArrayReader`, an abstraction beyond `RecordBatchReader` to allow a stream of arrow arrays that are not RecordBatches.
- Readers and writers for Parquet, Arrow IPC, CSV, and JSON.
- Initial compute functions.
- Initial Python tests.
- Improved documentation, both in the type stubs and in the docs website.
- Pyodide wheel support.

## [0.1.0] - 2024-07-01

- Initial release of arro3-core v0.1.
