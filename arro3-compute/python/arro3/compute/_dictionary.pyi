from typing import overload

from arro3.core import Array, ArrayReader
from arro3.core.types import ArrayInput, ArrowStreamExportable

@overload
def dictionary_encode(array: ArrayInput) -> Array: ...
@overload
def dictionary_encode(array: ArrowStreamExportable) -> ArrayReader: ...
def dictionary_encode(
    array: ArrayInput | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Dictionary-encode array.

    Return a dictionary-encoded version of the input array. This function does nothing if the input is already a dictionary array.

    Note: for stream input, each output array will not necessarily have the same dictionary.

    Args:
        array: Argument to compute function.

    Returns:
        The dictionary-encoded array.
    """
