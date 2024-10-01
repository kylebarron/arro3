from typing import overload

from arro3.core import Array, ArrayReader
from arro3.core.types import (
    ArrayInput,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

@overload
def cast(
    input: ArrayInput,
    to_type: ArrowSchemaExportable,
) -> Array: ...
@overload
def cast(
    input: ArrowStreamExportable,
    to_type: ArrowSchemaExportable,
) -> ArrayReader: ...
def cast(
    input: ArrayInput | ArrowStreamExportable,
    to_type: ArrowSchemaExportable,
) -> Array | ArrayReader:
    """
    Cast `input` to the provided data type and return a new Array with type `to_type`, if possible.

    If `input` is an Array, an `Array` will be returned. If `input` is a `ChunkedArray` or `ArrayReader`, an `ArrayReader` will be returned.

    Args:
        input: Input data to cast.
        to_type: The target data type to cast to. You may pass in a `Field` here if you wish to include Arrow extension metadata on the output array.

    Returns:
        The casted Arrow data.
    """

def can_cast_types(
    from_type: ArrowSchemaExportable, to_type: ArrowSchemaExportable
) -> bool:
    """Return true if a value of type `from_type` can be cast into a value of `to_type`.

    Args:
        from_type: Source type
        to_type: Destination type

    Returns:
        True if can be casted.
    """
