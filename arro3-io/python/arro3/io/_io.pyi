from pathlib import Path
from typing import IO, Literal, Sequence

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

#### CSV

def infer_csv_schema(
    file: IO[bytes] | Path | str,
    *,
    has_header: bool | None = None,
    max_records: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> core.Schema:
    """Infer a CSV file's schema

    If `max_records` is `None`, all records will be read, otherwise up to `max_records`
    records are read to infer the schema

    Args:
        file: The input CSV path or buffer.
        has_header: Set whether the CSV file has a header. Defaults to None.
        max_records: The maximum number of records to read to infer schema. Defaults to None.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to None.
        escape: Set the CSV escape character. Defaults to None.
        quote: Set the CSV quote character. Defaults to None.
        terminator: Set the line terminator. Defaults to None.
        comment: Set the comment character. Defaults to None.

    Returns:
        inferred schema from data
    """

def read_csv(
    file: IO[bytes] | Path | str,
    schema: types.ArrowSchemaExportable,
    *,
    has_header: bool | None = None,
    batch_size: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> core.RecordBatchReader:
    """Read a CSV file to an Arrow RecordBatchReader.

    Args:
        file: The input CSV path or buffer.
        schema: The Arrow schema for this CSV file. Use [infer_csv_schema][arro3.io.infer_csv_schema] to infer an Arrow schema if needed.
        has_header: Set whether the CSV file has a header. Defaults to None.
        batch_size: Set the batch size (number of records to load at one time). Defaults to None.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to None.
        escape: Set the CSV escape character. Defaults to None.
        quote: Set the CSV quote character. Defaults to None.
        terminator: Set the line terminator. Defaults to None.
        comment: Set the comment character. Defaults to None.

    Returns:
        A RecordBatchReader with read CSV data
    """

def write_csv(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    header: bool | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    date_format: str | None = None,
    datetime_format: str | None = None,
    time_format: str | None = None,
    timestamp_format: str | None = None,
    timestamp_tz_format: str | None = None,
    null: str | None = None,
) -> None:
    """Write an Arrow Table or stream to a CSV file.

    Args:
        data: The Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: The output buffer or file path for where to write the CSV.
        header: Set whether to write the CSV file with a header. Defaults to None.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to None.
        escape: Set the CSV file's escape character as a byte character.

            In some variants of CSV, quotes are escaped using a special escape character
            like `\\` (instead of escaping quotes by doubling them).

            By default, writing these idiosyncratic escapes is disabled, and is only
            used when double_quote is disabled. Defaults to None.
        quote: Set the CSV file's quote character as a byte character. Defaults to None.
        date_format: Set the CSV file's date format. Defaults to None.
        datetime_format: Set the CSV file's datetime format. Defaults to None.
        time_format: Set the CSV file's time format. Defaults to None.
        timestamp_format: Set the CSV file's timestamp format. Defaults to None.
        timestamp_tz_format: Set the CSV file's timestamp tz format. Defaults to None.
        null: Set the value to represent null in output. Defaults to None.
    """

#### JSON

def infer_json_schema(
    file: IO[bytes] | Path | str,
    *,
    max_records: int | None = None,
) -> core.Schema:
    """
    Infer the schema of a JSON file by reading the first n records of the buffer, with
    `max_records` controlling the maximum number of records to read.

    Args:
        file: The input JSON path or buffer.
        max_records: The maximum number of records to read to infer schema. If not provided, will read the entire file to deduce field types. Defaults to None.

    Returns:
        Inferred Arrow Schema
    """

def read_json(
    file: IO[bytes] | Path | str,
    schema: types.ArrowSchemaExportable,
    *,
    batch_size: int | None = None,
) -> core.RecordBatchReader:
    """Reads JSON data with a known schema into Arrow

    Args:
        file: The JSON file or buffer to read from.
        schema: The Arrow schema representing the JSON data.
        batch_size: Set the batch size (number of records to load at one time). Defaults to None.

    Returns:
        An arrow RecordBatchReader.
    """

def write_json(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    explicit_nulls: bool | None = None,
) -> None:
    """Write

    By default the writer will skip writing keys with null values for backward
    compatibility.

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
        explicit_nulls: Set whether to keep keys with null values, or to omit writing them. Defaults to skipping nulls.
    """

def write_ndjson(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    explicit_nulls: bool | None = None,
) -> None:
    """

    By default the writer will skip writing keys with null values for backward compatibility.

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
        explicit_nulls: Set whether to keep keys with null values, or to omit writing them. Defaults to skipping nulls.
    """

#### IPC

def read_ipc(file: IO[bytes] | Path | str) -> core.RecordBatchReader:
    """Read an Arrow IPC file into memory

    Args:
        file: The input Arrow IPC file path or buffer.

    Returns:
        An arrow RecordBatchReader.
    """

def read_ipc_stream(file: IO[bytes] | Path | str) -> core.RecordBatchReader:
    """Read an Arrow IPC stream into memory

    Args:
        file: The input Arrow IPC stream path or buffer.

    Returns:
        An arrow RecordBatchReader.
    """

def write_ipc(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
) -> None:
    """Write Arrow data to an Arrow IPC file

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
    """

def write_ipc_stream(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
) -> None:
    """Write Arrow data to an Arrow IPC stream

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
    """

#### Parquet

ParquetColumnPath = str | Sequence[str]
"""Allowed types to refer to a Parquet Column."""

ParquetCompression = (
    Literal["uncompressed", "snappy", "gzip", "lzo", "brotli", "lz4", "zstd", "lz4_raw"]
    | str
)
"""Allowed compression schemes for Parquet."""

ParquetEncoding = Literal[
    "plain",
    "plain_dictionary",
    "rle",
    "bit_packed",
    "delta_binary_packed",
    "delta_length_byte_array",
    "delta_byte_array",
    "rle_dictionary",
    "byte_stream_split",
]
"""Allowed Parquet encodings."""

def read_parquet(file: Path | str) -> core.RecordBatchReader:
    """Read a Parquet file to an Arrow RecordBatchReader

    Args:
        file: The input Parquet file path or buffer.

    Returns:
        The loaded Arrow data.
    """

def write_parquet(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    bloom_filter_enabled: bool | None = None,
    bloom_filter_fpp: float | None = None,
    bloom_filter_ndv: int | None = None,
    column_compression: dict[ParquetColumnPath, ParquetCompression] | None = None,
    column_dictionary_enabled: dict[ParquetColumnPath, bool] | None = None,
    column_encoding: dict[ParquetColumnPath, ParquetEncoding] | None = None,
    column_max_statistics_size: dict[ParquetColumnPath, int] | None = None,
    compression: ParquetCompression | None = None,
    created_by: str | None = None,
    data_page_row_count_limit: int | None = None,
    data_page_size_limit: int | None = None,
    dictionary_enabled: bool | None = None,
    dictionary_page_size_limit: int | None = None,
    encoding: ParquetEncoding | None = None,
    key_value_metadata: dict[str, str] | None = None,
    max_row_group_size: int | None = None,
    max_statistics_size: int | None = None,
    write_batch_size: int | None = None,
    writer_version: Literal["parquet_1_0", "parquet_2_0"] | None = None,
) -> None:
    """Write an Arrow Table or stream to a Parquet file.

    Args:
        data: The Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: The output file.

    Keyword Args:
        bloom_filter_enabled: Sets if bloom filter is enabled by default for all columns (defaults to `false`).
        bloom_filter_fpp: Sets the default target bloom filter false positive probability (fpp) for all columns (defaults to `0.05`).
        bloom_filter_ndv: Sets default number of distinct values (ndv) for bloom filter for all columns (defaults to `1_000_000`).
        column_compression: Sets compression codec for a specific column. Takes precedence over `compression`.
        column_dictionary_enabled: Sets flag to enable/disable dictionary encoding for a specific column. Takes precedence over `dictionary_enabled`.
        column_encoding: Sets encoding for a specific column. Takes precedence over `encoding`.
        column_max_statistics_size: Sets max size for statistics for a specific column. Takes precedence over `max_statistics_size`.
        compression:
            Sets default compression codec for all columns (default to `uncompressed`).
            Note that you can pass in a custom compression level with a string like
            `"zstd(3)"` or `"gzip(9)"` or `"brotli(3)"`.
        created_by: Sets "created by" property (defaults to `parquet-rs version <VERSION>`).
        data_page_row_count_limit:
            Sets best effort maximum number of rows in a data page (defaults to
            `20_000`).

            The parquet writer will attempt to limit the number of rows in each
            `DataPage` to this value. Reducing this value will result in larger parquet
            files, but may improve the effectiveness of page index based predicate
            pushdown during reading.

            Note: this is a best effort limit based on value of `set_write_batch_size`.

        data_page_size_limit:
            Sets best effort maximum size of a data page in bytes (defaults to `1024 *
            1024`).

            The parquet writer will attempt to limit the sizes of each `DataPage` to
            this many bytes. Reducing this value will result in larger parquet files,
            but may improve the effectiveness of page index based predicate pushdown
            during reading.

            Note: this is a best effort limit based on value of `set_write_batch_size`.
        dictionary_enabled: Sets default flag to enable/disable dictionary encoding for all columns (defaults to `True`).
        dictionary_page_size_limit:
            Sets best effort maximum dictionary page size, in bytes (defaults to `1024 *
            1024`).

            The parquet writer will attempt to limit the size of each `DataPage` used to
            store dictionaries to this many bytes. Reducing this value will result in
            larger parquet files, but may improve the effectiveness of page index based
            predicate pushdown during reading.

            Note: this is a best effort limit based on value of `set_write_batch_size`.

        encoding:
            Sets default encoding for all columns.

            If dictionary is not enabled, this is treated as a primary encoding for all
            columns. In case when dictionary is enabled for any column, this value is
            considered to be a fallback encoding for that column.
        key_value_metadata: Sets "key_value_metadata" property (defaults to `None`).
        max_row_group_size: Sets maximum number of rows in a row group (defaults to `1024 * 1024`).
        max_statistics_size: Sets default max statistics size for all columns (defaults to `4096`).
        write_batch_size:
            Sets write batch size (defaults to 1024).

            For performance reasons, data for each column is written in batches of this
            size.

            Additional limits such as such as `set_data_page_row_count_limit` are
            checked between batches, and thus the write batch size value acts as an
            upper-bound on the enforcement granularity of other limits.
        writer_version: Sets the `WriterVersion` written into the parquet metadata (defaults to `"parquet_1_0"`). This value can determine what features some readers will support.

    """
