from typing import Any, Iterable, Sequence, overload

import numpy as np
from numpy.typing import NDArray

from ._data_type import DataType
from ._field import Field
from ._scalar import Scalar
from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    _SupportsBuffer,
)

class Array:
    """An Arrow Array."""
    @overload
    def __init__(self, obj: ArrayInput, /, type: None = None) -> None: ...
    @overload
    def __init__(self, obj: Sequence[Any], /, type: ArrowSchemaExportable) -> None: ...
    def __init__(
        self,
        obj: ArrayInput | Sequence[Any],
        /,
        type: ArrowSchemaExportable | None = None,
    ) -> None:
        """Create arro3.Array instance from a sequence of Python objects.

        Args:
            obj: A sequence of input objects.
            type: Explicit type to attempt to coerce to. You may pass in a `Field` to `type` in order to associate extension metadata with this array.
        """
    def __array__(self, dtype=None, copy=None) -> NDArray:
        """
        An implementation of the Array interface, for interoperability with numpy and
        other array libraries.
        """
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this array. Then the
        consumer can ask the producer (in `__arrow_c_array__`) to cast the exported data
        to a supported data type.
        """
    def __eq__(self, other) -> bool: ...
    def __getitem__(self, i: int) -> Scalar: ...
    # Note: we don't actually implement this, but it's inferred by having a __getitem__
    # key
    def __iter__(self) -> Iterable[Scalar]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable | ArrowStreamExportable) -> Array:
        """
        Construct this object from an existing Arrow object.

        It can be called on anything that exports the Arrow data interface
        (`__arrow_c_array__`).

        Args:
            input: Arrow array to use for constructing this object

        Returns:
            Self
        """

    @classmethod
    def from_arrow_pycapsule(cls, schema_capsule, array_capsule) -> Array:
        """Construct this object from bare Arrow PyCapsules"""

    # We allow Any here because not many types have updated to expose __buffer__ yet
    @classmethod
    def from_buffer(cls, buffer: _SupportsBuffer) -> Array:
        """Construct an Array from an object implementing the Python Buffer Protocol."""

    @classmethod
    def from_numpy(cls, array: np.ndarray) -> Array:
        """Construct an Array from a numpy ndarray"""

    def cast(self, target_type: ArrowSchemaExportable) -> Array:
        """Cast array values to another data type

        Args:
            target_type: Type to cast array to.
        """

    @property
    def field(self) -> Field:
        """Access the field stored on this Array.

        Note that this field usually will not have a name associated, but it may have
        metadata that signifies that this array is an extension (user-defined typed)
        array.
        """
    @property
    def nbytes(self) -> int:
        """The number of bytes in this Array."""
    @property
    def null_count(self) -> int:
        """The number of null values in this Array."""
    def slice(self, offset: int = 0, length: int | None = None) -> Array:
        """Compute zero-copy slice of this array.

        Args:
            offset: Defaults to 0.
            length: Defaults to None.

        Returns:
            The sliced array
        """
    def take(self, indices: ArrayInput) -> Array:
        """Take specific indices from this Array."""
    def to_numpy(self) -> NDArray:
        """Return a numpy copy of this array."""
    def to_pylist(self) -> NDArray:
        """Convert to a list of native Python objects."""

    @property
    def type(self) -> DataType:
        """The data type of this array."""
