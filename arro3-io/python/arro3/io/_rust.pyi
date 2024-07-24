from pathlib import Path
from typing import IO, Protocol, Tuple

from arro3.core import Array, ArrayReader, Schema

class ArrowSchemaExportable(Protocol):
    def __arrow_c_schema__(self) -> object: ...

class ArrowArrayExportable(Protocol):
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...

class ArrowStreamExportable(Protocol):
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

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
) -> Schema: ...
def read_csv(
    file: IO[bytes] | Path | str,
    schema: ArrowSchemaExportable,
    *,
    has_header: bool | None = None,
    batch_size: int | None = None,
    delimiter: str | None = None,
    escape: str | None = None,
    quote: str | None = None,
    terminator: str | None = None,
    comment: str | None = None,
) -> Schema: ...
def write_csv(
    data: ArrowStreamExportable,
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
) -> None: ...
