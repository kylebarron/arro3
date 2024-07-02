from __future__ import annotations

from typing import Protocol, Tuple


class ArrowSchemaExportable(Protocol):
    """A C-level reference to an Arrow Schema or Field."""

    def __arrow_c_schema__(self) -> object: ...


class ArrowArrayExportable(Protocol):
    """A C-level reference to an Arrow Array or RecordBatch."""

    def __arrow_c_array__(
        self, requested_schema: object = None
    ) -> Tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """A C-level reference to an Arrow RecordBatchReader, Table, or ChunkedArray."""

    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...
