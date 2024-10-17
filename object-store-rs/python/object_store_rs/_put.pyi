from pathlib import Path
from typing import IO

from ._pyo3_object_store import ObjectStore

def put_file(
    store: ObjectStore,
    location: str,
    file: IO[bytes] | Path | bytes,
    *,
    max_concurrency: int = 12,
) -> None: ...
async def put_file_async(
    store: ObjectStore,
    location: str,
    file: IO[bytes] | Path | bytes,
    *,
    max_concurrency: int = 12,
) -> None: ...
