from typing import Protocol, Sequence, Tuple, overload

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
@overload
def list_flatten(input: ArrowArrayExportable) -> Array: ...
@overload
def list_flatten(input: ArrowStreamExportable) -> ArrayReader: ...
def list_flatten(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """Unnest this ListArray, LargeListArray or FixedSizeListArray.

    Args:
        input: _description_

    Raises:
        Exception if not a list-typed array.

    Returns:
        _description_
    """

@overload
def list_offsets(input: ArrowArrayExportable, *, logical: bool = True) -> Array: ...
@overload
def list_offsets(
    input: ArrowStreamExportable, *, logical: bool = True
) -> ArrayReader: ...
def list_offsets(
    input: ArrowArrayExportable | ArrowStreamExportable, *, logical: bool = True
) -> Array | ArrayReader:
    """Access the offsets of this ListArray or LargeListArray

    Args:
        input: _description_
        physical: If False, return the physical (unsliced) offsets of the provided list array. If True, adjust the list offsets for the current array slicing. Defaults to `True`.

    Raises:
        Exception if not a list-typed array.

    Returns:
        _description_
    """

def struct_field(
    values: ArrowArrayExportable,
    /,
    indices: int | Sequence[int],
) -> Array:
    """Access a column within a StructArray by index

    Args:
        values: Argument to compute function.
        indices: List of indices for chained field lookup, for example [4, 1] will look up the second nested field in the fifth outer field.

    Raises:
        Exception if not a struct-typed array.

    Returns:
        _description_
    """

def take(values: ArrowArrayExportable, indices: ArrowArrayExportable) -> Array: ...
