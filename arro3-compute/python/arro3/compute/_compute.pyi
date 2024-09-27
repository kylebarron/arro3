from typing import overload

# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types
from arro3.compute._arith import add as add
from arro3.compute._arith import add_wrapping as add_wrapping
from arro3.compute._arith import div as div
from arro3.compute._arith import mul as mul
from arro3.compute._arith import mul_wrapping as mul_wrapping
from arro3.compute._arith import neg as neg
from arro3.compute._arith import neg_wrapping as neg_wrapping
from arro3.compute._arith import rem as rem
from arro3.compute._arith import sub as sub
from arro3.compute._arith import sub_wrapping as sub_wrapping
from arro3.compute._boolean import is_not_null as is_not_null
from arro3.compute._boolean import is_null as is_null
from arro3.compute._cast import can_cast_types as can_cast_types
from arro3.compute._cast import cast as cast
from arro3.compute._filter import filter as filter

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

def max(input: types.ArrowArrayExportable | types.ArrowStreamExportable) -> core.Scalar:
    """
    Returns the max of values in the array.
    """

def min(input: types.ArrowArrayExportable | types.ArrowStreamExportable) -> core.Scalar:
    """
    Returns the min of values in the array.
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

def sum(input: types.ArrowArrayExportable | types.ArrowStreamExportable) -> core.Scalar:
    """
    Returns the sum of values in the array.
    """
