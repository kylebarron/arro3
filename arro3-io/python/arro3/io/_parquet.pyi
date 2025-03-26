import sys
from pathlib import Path
from typing import IO, Literal, Protocol, Sequence, TypedDict

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

from ._pyo3_object_store import ObjectStore
from ._stream import RecordBatchStream

if sys.version_info >= (3, 11):
    from typing import Unpack
else:
    from typing_extensions import Unpack

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

def read_parquet(file: IO[bytes] | Path | str) -> core.RecordBatchReader:
    """Read a Parquet file to an Arrow RecordBatchReader

    Args:
        file: The input Parquet file path or buffer.

    Returns:
        The loaded Arrow data.
    """

async def read_parquet_async(path: str, *, store: ObjectStore) -> core.Table:
    """Read a Parquet file to an Arrow Table in an async fashion

    Args:
        path: The path to the Parquet file in the given store

    Returns:
        The loaded Arrow data.
    """

class ParquetPredicate(Protocol):
    @property
    def projection(self) -> Sequence[str]:
        """Return the projected columns."""
    def evaluate(self, batch: core.RecordBatch) -> types.ArrowArrayExportable:
        """Evaluate the predicate on a RecordBatch.

        Must return a boolean-typed array.
        """

class ParquetReadOptions(TypedDict, total=False):
    batch_size: int | None
    row_groups: Sequence[int] | None
    columns: Sequence[str] | None
    filter: ParquetPredicate | Sequence[ParquetPredicate] | None
    limit: int | None
    offset: int | None

class ParquetFile:
    @property
    def num_row_groups(self) -> int:
        """Return the number of row groups in the Parquet file."""

    def read(self, **kwargs: Unpack[ParquetReadOptions]) -> core.RecordBatchReader:
        """Read the Parquet file to an Arrow RecordBatchReader.

        Keyword Args:
            batch_size: The number of rows to read in each batch.
            row_groups: The row groups to read.
            columns: The columns to read.
            limit: The number of rows to read.
            offset: The number of rows to skip.

        Returns:
            The loaded Arrow data.
        """
    async def read_async(
        self, **kwargs: Unpack[ParquetReadOptions]
    ) -> RecordBatchStream:
        """Read the Parquet file to an Arrow async RecordBatchStream.

        Keyword Args:
            batch_size: The number of rows to read in each batch.
            row_groups: The row groups to read.
            columns: The columns to read.
            limit: The number of rows to read.
            offset: The number of rows to skip.

        Returns:
            The loaded Arrow data.
        """
    @property
    def schema_arrow(self) -> core.Schema:
        """Return the Arrow schema of the Parquet file."""

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
    skip_arrow_metadata: bool = False,
    write_batch_size: int | None = None,
    writer_version: Literal["parquet_1_0", "parquet_2_0"] | None = None,
) -> None:
    """Write an Arrow Table or stream to a Parquet file.

    Args:
        data: The Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: The output file.

    Keyword Args:
        bloom_filter_enabled: Sets if bloom filter is enabled by default for all columns
            (defaults to `false`).
        bloom_filter_fpp: Sets the default target bloom filter false positive
            probability (fpp) for all columns (defaults to `0.05`).
        bloom_filter_ndv: Sets default number of distinct values (ndv) for bloom filter
            for all columns (defaults to `1_000_000`).
        column_compression: Sets compression codec for a specific column. Takes
            precedence over `compression`.
        column_dictionary_enabled: Sets flag to enable/disable dictionary encoding for a
            specific column. Takes precedence over `dictionary_enabled`.
        column_encoding: Sets encoding for a specific column. Takes precedence over
            `encoding`.
        column_max_statistics_size: Sets max size for statistics for a specific column.
            Takes precedence over `max_statistics_size`.
        compression:
            Sets default compression codec for all columns (default to `uncompressed`).
            Note that you can pass in a custom compression level with a string like
            `"zstd(3)"` or `"gzip(9)"` or `"brotli(3)"`.
        created_by: Sets "created by" property (defaults to `parquet-rs version
            <VERSION>`).
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
        dictionary_enabled: Sets default flag to enable/disable dictionary encoding for
            all columns (defaults to `True`).
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
        max_row_group_size: Sets maximum number of rows in a row group (defaults to
            `1024 * 1024`).
        max_statistics_size: Sets default max statistics size for all columns (defaults
            to `4096`).
        skip_arrow_metadata: Parquet files generated by this writer contain embedded
            arrow schema by default. Set `skip_arrow_metadata` to `True`, to skip
            encoding the embedded metadata (defaults to `False`).
        write_batch_size:
            Sets write batch size (defaults to 1024).

            For performance reasons, data for each column is written in batches of this
            size.

            Additional limits such as such as `set_data_page_row_count_limit` are
            checked between batches, and thus the write batch size value acts as an
            upper-bound on the enforcement granularity of other limits.
        writer_version: Sets the `WriterVersion` written into the parquet metadata
            (defaults to `"parquet_1_0"`). This value can determine what features some
            readers will support.

    """
