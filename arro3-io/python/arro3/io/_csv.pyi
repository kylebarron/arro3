from pathlib import Path
from typing import IO

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

__all__ = ["infer_csv_schema", "read_csv", "write_csv"]

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
        max_records: The maximum number of records to read to infer schema. Defaults to
            None.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to
            None.
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
        schema: The Arrow schema for this CSV file. Use
            [infer_csv_schema][arro3.io.infer_csv_schema] to infer an Arrow schema if
            needed.
        has_header: Set whether the CSV file has a header. Defaults to None.
        batch_size: Set the batch size (number of records to load at one time).
            Defaults to None.
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to
            None.
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
        delimiter: Set the CSV file's column delimiter as a byte character. Defaults to
            None.
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
