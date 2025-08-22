from typing import Sequence, overload

from ._array import Array
from ._array_reader import ArrayReader
from ._buffer import Buffer
from ._chunked_array import ChunkedArray
from ._data_type import DataType
from ._field import Field
from ._record_batch import RecordBatch
from ._record_batch_reader import RecordBatchReader
from ._scalar import Scalar
from ._schema import Schema
from ._table import Table
from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

__all__ = [
    "Array",
    "ArrayReader",
    "Buffer",
    "ChunkedArray",
    "DataType",
    "Field",
    "RecordBatch",
    "RecordBatchReader",
    "Scalar",
    "Schema",
    "Table",
    "dictionary_dictionary",
    "dictionary_indices",
    "fixed_size_list_array",
    "list_array",
    "list_flatten",
    "list_offsets",
    "struct_array",
    "struct_field",
]

@overload
def dictionary_dictionary(array: ArrowArrayExportable) -> Array: ...
@overload
def dictionary_dictionary(array: ArrowStreamExportable) -> ArrayReader: ...
def dictionary_dictionary(
    array: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
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
def dictionary_indices(array: ArrowArrayExportable) -> Array: ...
@overload
def dictionary_indices(array: ArrowStreamExportable) -> ArrayReader: ...
def dictionary_indices(
    array: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
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
def list_flatten(input: ArrowArrayExportable) -> Array: ...
@overload
def list_flatten(input: ArrowStreamExportable) -> ArrayReader: ...
def list_flatten(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """Unnest this ListArray, LargeListArray or FixedSizeListArray.

    Args:
        input: Input data.

    Raises:
        Exception: if not a list-typed array.

    Returns:
        The flattened Arrow data.
    """

@overload
def list_offsets(input: ArrowArrayExportable, *, logical: bool = True) -> Array: ...
@overload
def list_offsets(
    input: ArrowStreamExportable, *, logical: bool = True
) -> ArrayReader: ...
def list_offsets(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    logical: bool = True,
) -> Array | ArrayReader:
    """Access the offsets of this ListArray or LargeListArray

    Args:
        input: _description_
        logical: If `False`, return the physical (unsliced) offsets of the provided list array. If `True`, adjust the list offsets for the current array slicing. Defaults to `True`.

    Raises:
        Exception: if not a list-typed array.

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
        Exception: if not a struct-typed array.

    Returns:
        _description_
    """

def fixed_size_list_array(
    values: ArrayInput,
    list_size: int,
    *,
    type: ArrowSchemaExportable | None = None,
    mask: ArrowArrayExportable | None = None,
) -> Array:
    """Construct a new fixed size list array

    Args:
        values: the values of the new fixed size list array
        list_size: the number of elements in each item of the list.

    Keyword Args:
        type: the type of output array. This must have fixed size list type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred.
        mask: Indicate which values are null (`True`) or not null (`False`).

    Returns:
        a new Array with fixed size list type
    """

def list_array(
    offsets: ArrayInput,
    values: ArrayInput,
    *,
    type: ArrowSchemaExportable | None = None,
    mask: ArrowArrayExportable | None = None,
) -> Array:
    """Construct a new list array

    Args:
        offsets: the offsets for the output list array. This array must have type int32 or int64, depending on whether you wish to create a list array or large list array.
        values: the values for the output list array.

    Keyword Args:
        type: the type of output array. This must have list or large list type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred.
        mask: Indicate which values are null (`True`) or not null (`False`).

    Returns:
        a new Array with list or large list type
    """

def struct_array(
    arrays: Sequence[ArrayInput],
    *,
    fields: Sequence[ArrowSchemaExportable],
    type: ArrowSchemaExportable | None = None,
    mask: ArrowArrayExportable | None = None,
) -> Array:
    """Construct a new struct array

    Args:
        arrays: a sequence of arrays for the struct children

    Keyword Args:
        fields: a sequence of fields that represent each of the struct children
        type: the type of output array. This must have struct type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred .
        mask: Indicate which values are null (`True`) or not null (`False`).


    Returns:
        a new Array with struct type
    """
