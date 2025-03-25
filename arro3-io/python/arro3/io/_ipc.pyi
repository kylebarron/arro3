from pathlib import Path
from typing import IO, Literal

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

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
    *,
    compression: Literal["LZ4", "lz4", "ZSTD", "zstd"] | None = None,
) -> None:
    """Write Arrow data to an Arrow IPC file

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to

    Other Args:
        compression: Compression to apply to file.
    """

def write_ipc_stream(
    data: types.ArrowStreamExportable | types.ArrowArrayExportable,
    file: IO[bytes] | Path | str,
    *,
    compression: Literal["LZ4", "lz4", "ZSTD", "zstd"] | None = None,
) -> None:
    """Write Arrow data to an Arrow IPC stream

    Args:
        data: the Arrow Table, RecordBatchReader, or RecordBatch to write.
        file: the output file or buffer to write to

    Other Args:
        compression: Compression to apply to file.
    """
