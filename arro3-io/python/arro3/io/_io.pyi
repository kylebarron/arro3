from ._csv import infer_csv_schema, read_csv, write_csv
from ._ipc import read_ipc, read_ipc_stream, write_ipc, write_ipc_stream
from ._json import infer_json_schema, read_json, write_json, write_ndjson
from ._parquet import (
    ParquetColumnPath,
    ParquetCompression,
    ParquetEncoding,
    ParquetFile,
    ParquetOpenOptions,
    ParquetPredicate,
    ParquetReadOptions,
    read_parquet,
    read_parquet_async,
    write_parquet,
)

__all__ = [
    "infer_csv_schema",
    "read_csv",
    "write_csv",
    "infer_json_schema",
    "read_json",
    "write_json",
    "write_ndjson",
    "read_ipc",
    "read_ipc_stream",
    "write_ipc",
    "write_ipc_stream",
    "read_parquet",
    "read_parquet_async",
    "write_parquet",
    "ParquetColumnPath",
    "ParquetCompression",
    "ParquetEncoding",
    "ParquetFile",
    "ParquetOpenOptions",
    "ParquetPredicate",
    "ParquetReadOptions",
]

def ___version() -> str: ...
