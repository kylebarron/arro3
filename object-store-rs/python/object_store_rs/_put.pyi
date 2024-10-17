from pathlib import Path
from typing import IO

from ._pyo3_object_store import ObjectStore

def put_file(
    store: ObjectStore,
    location: str,
    file: IO[bytes] | Path | bytes,
    *,
    chunk_size: int = 5 * 1024,
    max_concurrency: int = 12,
) -> None:
    """Save the provided bytes to the specified location

    The operation is guaranteed to be atomic, it will either successfully write the
    entirety of `file` to `location`, or fail. No clients should be able to observe a
    partially written object.

    This will use a multipart upload under the hood.

    Args:
        store: The ObjectStore instance to use.
        location: The path within ObjectStore for where to save the file.
        file: The object to upload. Can either be file-like, a `Path` to a local file,
            or a `bytes` object.
        chunk_size: The size of chunks to use within each part of the multipart upload. Defaults to 5 MB.
        max_concurrency: The maximum number of chunks to upload concurrently. Defaults to 12.
    """

async def put_file_async(
    store: ObjectStore,
    location: str,
    file: IO[bytes] | Path | bytes,
    *,
    chunk_size: int = 5 * 1024,
    max_concurrency: int = 12,
) -> None:
    """Call `put_file` asynchronously.

    Refer to the documentation for [put_file][object_store_rs.put_file].
    """
