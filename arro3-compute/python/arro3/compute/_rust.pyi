from typing import Protocol, Tuple, overload

from arro3.core import Array, ArrayReader

class ArrowSchemaExportable(Protocol):
    def __arrow_c_schema__(self) -> object: ...

class ArrowArrayExportable(Protocol):
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...

class ArrowStreamExportable(Protocol):
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

@overload
def cast(
    input: ArrowArrayExportable,
    to_type: ArrowSchemaExportable,
) -> Array: ...
@overload
def cast(
    input: ArrowStreamExportable,
    to_type: ArrowSchemaExportable,
) -> ArrayReader: ...
def cast(
    input: ArrowArrayExportable | ArrowStreamExportable,
    to_type: ArrowSchemaExportable,
) -> Array | ArrayReader: ...
