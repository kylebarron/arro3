from typing import overload

from arro3.core import Array, ArrayReader
from arro3.core.types import ArrayInput, ArrowStreamExportable

@overload
def filter(
    values: ArrayInput,
    predicate: ArrayInput,
) -> Array: ...
@overload
def filter(
    values: ArrowStreamExportable,
    predicate: ArrowStreamExportable,
) -> ArrayReader: ...
def filter(
    values: ArrayInput | ArrowStreamExportable,
    predicate: ArrayInput | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Returns a filtered `values` array where the corresponding elements of
    `predicate` are `true`.

    If `input` is an Array, an `Array` will be returned. If `input` is a `ChunkedArray`
    or `ArrayReader`, an `ArrayReader` will be returned.
    """
