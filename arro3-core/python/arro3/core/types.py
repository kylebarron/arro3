from __future__ import annotations

from typing import Protocol, Tuple


class ArrowSchemaExportable(Protocol):
    """An Arrow or GeoArrow schema or field."""

    def __arrow_c_schema__(self) -> object: ...


class ArrowArrayExportable(Protocol):
    """An Arrow or GeoArrow array or RecordBatch."""

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """An Arrow or GeoArrow ChunkedArray or Table."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
