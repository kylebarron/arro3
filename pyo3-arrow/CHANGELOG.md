# Changelog

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
