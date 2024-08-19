from typing import Sequence, overload

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

@overload
def dictionary_dictionary(array: types.ArrowArrayExportable) -> core.Array: ...
@overload
def dictionary_dictionary(array: types.ArrowStreamExportable) -> core.ArrayReader: ...
def dictionary_dictionary(
    array: types.ArrowArrayExportable | types.ArrowStreamExportable,
) -> core.Array | core.ArrayReader:
    """
    Access the `dictionary` of a dictionary array.

    This is equivalent to the [`.dictionary`][pyarrow.DictionaryArray.dictionary]
    attribute on a PyArrow [DictionaryArray][pyarrow.DictionaryArray].

    Args:
        array: Argument to compute function.

    Returns:
        The keys of a dictionary-encoded array.
    """

@overload
def dictionary_indices(array: types.ArrowArrayExportable) -> core.Array: ...
@overload
def dictionary_indices(array: types.ArrowStreamExportable) -> core.ArrayReader: ...
def dictionary_indices(
    array: types.ArrowArrayExportable | types.ArrowStreamExportable,
) -> core.Array | core.ArrayReader:
    """
    Access the indices of a dictionary array.

    This is equivalent to the [`.indices`][pyarrow.DictionaryArray.indices]
    attribute on a PyArrow [DictionaryArray][pyarrow.DictionaryArray].

    Args:
        array: Argument to compute function.

    Returns:
        The indices of a dictionary-encoded array.
    """

@overload
def list_flatten(input: types.ArrowArrayExportable) -> core.Array: ...
@overload
def list_flatten(input: types.ArrowStreamExportable) -> core.ArrayReader: ...
def list_flatten(
    input: types.ArrowArrayExportable | types.ArrowStreamExportable,
) -> core.Array | core.ArrayReader:
    """Unnest this ListArray, LargeListArray or FixedSizeListArray.

    Args:
        input: Input data.

    Raises:
        Exception if not a list-typed array.

    Returns:
        The flattened Arrow data.
    """

@overload
def list_offsets(
    input: types.ArrowArrayExportable, *, logical: bool = True
) -> core.Array: ...
@overload
def list_offsets(
    input: types.ArrowStreamExportable, *, logical: bool = True
) -> core.ArrayReader: ...
def list_offsets(
    input: types.ArrowArrayExportable | types.ArrowStreamExportable,
    *,
    logical: bool = True,
) -> core.Array | core.ArrayReader:
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
    values: types.ArrowArrayExportable,
    /,
    indices: int | Sequence[int],
) -> core.Array:
    """Access a column within a StructArray by index

    Args:
        values: Argument to compute function.
        indices: List of indices for chained field lookup, for example [4, 1] will look up the second nested field in the fifth outer field.

    Raises:
        Exception if not a struct-typed array.

    Returns:
        _description_
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
