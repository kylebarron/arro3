from typing import overload

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

@overload
def cast(
    input: types.ArrowArrayExportable,
    to_type: types.ArrowSchemaExportable,
) -> core.Array: ...
@overload
def cast(
    input: types.ArrowStreamExportable,
    to_type: types.ArrowSchemaExportable,
) -> core.ArrayReader: ...
def cast(
    input: types.ArrowArrayExportable | types.ArrowStreamExportable,
    to_type: types.ArrowSchemaExportable,
) -> core.Array | core.ArrayReader:
    """
    Cast `input` to the provided data type and return a new Array with type `to_type`, if possible.

    If `input` is an Array, an `Array` will be returned. If `input` is a `ChunkedArray` or `ArrayReader`, an `ArrayReader` will be returned.

    Args:
        input: Input data to cast.
        to_type: The target data type to cast to. You may pass in a `Field` here if you wish to include Arrow extension metadata on the output array.

    Returns:
        The casted Arrow data.
    """

@overload
def dictionary_encode(array: types.ArrowArrayExportable) -> core.Array: ...
@overload
def dictionary_encode(array: types.ArrowStreamExportable) -> core.ArrayReader: ...
def dictionary_encode(
    array: types.ArrowArrayExportable | types.ArrowStreamExportable,
) -> core.Array | core.ArrayReader:
    """
    Dictionary-encode array.

    Return a dictionary-encoded version of the input array. This function does nothing if the input is already a dictionary array.

    Note: for stream input, each output array will not necessarily have the same dictionary.

    Args:
        array: Argument to compute function.

    Returns:
        The dictionary-encoded array.
    """

def take(
    values: types.ArrowArrayExportable, indices: types.ArrowArrayExportable
) -> core.Array:
    """Take elements by index from Array, creating a new Array from those indexes.

    ```
    ┌─────────────────┐      ┌─────────┐                              ┌─────────────────┐
    │        A        │      │    0    │                              │        A        │
    ├─────────────────┤      ├─────────┤                              ├─────────────────┤
    │        D        │      │    2    │                              │        B        │
    ├─────────────────┤      ├─────────┤   take(values, indices)      ├─────────────────┤
    │        B        │      │    3    │ ─────────────────────────▶   │        C        │
    ├─────────────────┤      ├─────────┤                              ├─────────────────┤
    │        C        │      │    1    │                              │        D        │
    ├─────────────────┤      └─────────┘                              └─────────────────┘
    │        E        │
    └─────────────────┘
    values array             indices array                            result
    ```

    Args:
        values: The input Arrow data to select from.
        indices: The indices within `values` to take. This must be a numeric array.

    Returns:
        The selected arrow data.
    """
