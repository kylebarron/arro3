from pathlib import Path
from typing import IO

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

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
        max_records: The maximum number of records to read to infer schema. If not
            provided, will read the entire file to deduce field types. Defaults to None.

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
        batch_size: Set the batch size (number of records to load at one time). Defaults
            to None.

    Returns:
        An arrow RecordBatchReader.
    """

def write_json(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    explicit_nulls: bool | None = None,
) -> None:
    """Write Arrow data to JSON.

    By default the writer will skip writing keys with null values for backward
    compatibility.

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
        explicit_nulls: Set whether to keep keys with null values, or to omit writing
            them. Defaults to skipping nulls.
    """

def write_ndjson(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    explicit_nulls: bool | None = None,
) -> None:
    """Write Arrow data to newline-delimited JSON.

    By default the writer will skip writing keys with null values for backward
    compatibility.

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to
        explicit_nulls: Set whether to keep keys with null values, or to omit writing
            them. Defaults to skipping nulls.
    """
