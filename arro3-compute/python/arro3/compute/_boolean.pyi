from typing import overload

from arro3.core import Array, ArrayReader
from arro3.core.types import ArrayInput, ArrowStreamExportable

@overload
def is_null(input: ArrayInput) -> Array: ...
@overload
def is_null(input: ArrowStreamExportable) -> ArrayReader: ...
def is_null(
    input: ArrayInput | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Returns a non-null boolean-typed array with whether each value of the array is null.

    If `input` is an Array, an `Array` will be returned. If `input` is a `ChunkedArray` or `ArrayReader`, an `ArrayReader` will be returned.

    Args:
        input: Input data

    Returns:
        Output
    """

@overload
def is_not_null(input: ArrayInput) -> Array: ...
@overload
def is_not_null(input: ArrowStreamExportable) -> ArrayReader: ...
def is_not_null(
    input: ArrayInput | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Returns a non-null boolean-typed array with whether each value of the array is not null.

    If `input` is an Array, an `Array` will be returned. If `input` is a `ChunkedArray` or `ArrayReader`, an `ArrayReader` will be returned.

    Args:
        input: Input data

    Returns:
        Output
    """
