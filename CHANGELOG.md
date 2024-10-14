# Changelog

This is the changelog for arro3. pyo3-arrow has a separate changelog.

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
