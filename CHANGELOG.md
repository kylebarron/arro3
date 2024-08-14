# Changelog

This is the changelog for arro3. pyo3-arrow has a separate changelog.

## [0.2.0] - 2024-08-13

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
