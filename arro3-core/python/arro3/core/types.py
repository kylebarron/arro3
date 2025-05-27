from __future__ import annotations

import array as _array
import mmap
import sys
from typing import TYPE_CHECKING, Protocol, Tuple, Union

if sys.version_info >= (3, 12):
    from collections.abc import Buffer as _Buffer
else:
    from typing_extensions import Buffer as _Buffer

if TYPE_CHECKING:
    import numpy as np


class ArrowSchemaExportable(Protocol):
    """
    An object with an `__arrow_c_schema__` method implementing the [Arrow C Data Interface
    interface](https://arrow.apache.org/docs/format/CDataInterface.html) via the
    [Arrow PyCapsule
    Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).

    Such objects include:

    - arro3 [`Schema`][arro3.core.Schema], [`Field`][arro3.core.Field], or [`DataType`][arro3.core.DataType] objects.
    - pyarrow [`Schema`][pyarrow.Schema], [`Field`][pyarrow.Field], or [`DataType`][pyarrow.DataType] objects.

    This allows for zero-copy Arrow data interchange across libraries.
    """

    def __arrow_c_schema__(self) -> object: ...


class ArrowArrayExportable(Protocol):
    """
    An object with an `__arrow_c_array__` method implementing the [Arrow C Data Interface
    interface](https://arrow.apache.org/docs/format/CDataInterface.html) via the
    [Arrow PyCapsule
    Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).

    Such objects include:

    - arro3 [`Array`][arro3.core.Array] or [`RecordBatch`][arro3.core.RecordBatch] objects.
    - pyarrow [`Array`][pyarrow.Array] or [`RecordBatch`][pyarrow.RecordBatch] objects

    This allows for zero-copy Arrow data interchange across libraries.
    """

    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> Tuple[object, object]: ...


class ArrowStreamExportable(Protocol):
    """
    An object with an `__arrow_c_stream__` method implementing the [Arrow C Stream
    interface](https://arrow.apache.org/docs/format/CStreamInterface.html) via the
    [Arrow PyCapsule
    Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).

    Supported objects include:

    - arro3 [`Table`][arro3.core.Table], [`RecordBatchReader`][arro3.core.RecordBatchReader], [`ChunkedArray`][arro3.core.ChunkedArray], or [`ArrayReader`][arro3.core.ArrayReader] objects.
    - Polars `Series` or `DataFrame` objects (polars v1.2 or higher)
    - DuckDB table-like objects, such as [`DuckDBPyRelation`][duckdb.DuckDBPyRelation] or [`DuckDBPyConnection`][duckdb.DuckDBPyConnection].
    - pyarrow [`RecordBatchReader`][pyarrow.RecordBatchReader], [`Table`][pyarrow.Table], or [`ChunkedArray`][pyarrow.ChunkedArray] objects (pyarrow v14 or
        higher)
    - pandas [`DataFrame`][pandas.DataFrame]s  (pandas v2.2 or higher)
    - ibis `Table` objects.

    This allows for zero-copy Arrow data interchange across libraries.

    For an up to date list of supported objects, see [this
    issue](https://github.com/apache/arrow/issues/39195#issuecomment-2245718008).
    """

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


# From numpy
# https://github.com/numpy/numpy/blob/961b70f6aaeed67147245b56ddb3f12ed1a050b5/numpy/__init__.pyi#L1772C1-L1785C1
if sys.version_info >= (3, 12):
    from collections.abc import Buffer as _SupportsBuffer
else:
    _SupportsBuffer = Union[
        bytes,
        bytearray,
        memoryview,
        _array.array,
        mmap.mmap,
        "np.ndarray",
        _Buffer,
    ]


# Numpy arrays don't yet declare `__buffer__` (or maybe just on a very recent version)
ArrayInput = Union[ArrowArrayExportable, _SupportsBuffer]
"""Accepted input as an Arrow array.

Buffer protocol input (such as numpy arrays) will be interpreted zero-copy except in the
case of boolean-typed input, which must be copied to the Arrow format.
"""
