from typing import Iterable, Sequence, overload

from numpy.typing import NDArray

from ._array import Array
from ._data_type import DataType
from ._field import Field
from ._scalar import Scalar
from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

class ChunkedArray:
    """An Arrow ChunkedArray."""
    @overload
    def __init__(
        self, arrays: ArrayInput | ArrowStreamExportable, type: None = None
    ) -> None: ...
    @overload
    def __init__(
        self,
        arrays: Sequence[ArrayInput],
        type: ArrowSchemaExportable | None = None,
    ) -> None: ...
    def __init__(
        self,
        arrays: ArrayInput | ArrowStreamExportable | Sequence[ArrayInput],
        type: ArrowSchemaExportable | None = None,
    ) -> None:
        """Construct a new ChunkedArray.

        Args:
            arrays: _description_
            type: _description_. Defaults to None.
        """
    def __array__(self, dtype=None, copy=None) -> NDArray:
        """
        An implementation of the Array interface, for interoperability with numpy and
        other array libraries.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this ChunkedArray. Then
        the consumer can ask the producer (in `__arrow_c_stream__`) to cast the exported
        data to a supported data type.
        """
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example (as of pyarrow v16), you can call
        [`pyarrow.chunked_array()`][pyarrow.chunked_array] to convert this array into a
        pyarrow array, without copying memory.
        """
    def __eq__(self, other) -> bool: ...
    def __getitem__(self, i: int) -> Scalar: ...
    # Note: we don't actually implement this, but it's inferred by having a __getitem__
    # key
    def __iter__(self) -> Iterable[Scalar]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(
        cls, input: ArrowArrayExportable | ArrowStreamExportable
    ) -> ChunkedArray:
        """Construct this from an existing Arrow object.

        It can be called on anything that exports the Arrow stream interface (has an
        `__arrow_c_stream__` method). All batches from the stream will be materialized
        in memory.
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> ChunkedArray:
        """Construct this object from a bare Arrow PyCapsule"""
    def cast(self, target_type: ArrowSchemaExportable) -> ChunkedArray:
        """Cast array values to another data type

        Args:
            target_type: Type to cast array to.
        """
    def chunk(self, i: int) -> Array:
        """Select a chunk by its index.

        Args:
            i: chunk index.

        Returns:
            new Array.
        """
    @property
    def chunks(self) -> list[Array]:
        """Convert to a list of single-chunked arrays."""
    def combine_chunks(self) -> Array:
        """Flatten this ChunkedArray into a single non-chunked array."""
    def equals(self, other: ArrowStreamExportable) -> bool:
        """Return whether the contents of two chunked arrays are equal."""
    @property
    def field(self) -> Field:
        """Access the field stored on this ChunkedArray.

        Note that this field usually will not have a name associated, but it may have
        metadata that signifies that this array is an extension (user-defined typed)
        array.
        """
    def length(self) -> int:
        """Return length of a ChunkedArray."""
    @property
    def nbytes(self) -> int:
        """Total number of bytes consumed by the elements of the chunked array."""
    @property
    def null_count(self) -> int:
        """Number of null entries"""
    @property
    def num_chunks(self) -> int:
        """Number of underlying chunks."""
    def rechunk(self, *, max_chunksize: int | None = None) -> ChunkedArray:
        """Rechunk a ChunkedArray with a maximum number of rows per chunk.

        Args:
            max_chunksize: The maximum number of rows per internal array. Defaults to None, which rechunks into a single array.

        Returns:
            The rechunked ChunkedArray.
        """
    def slice(self, offset: int = 0, length: int | None = None) -> ChunkedArray:
        """Compute zero-copy slice of this ChunkedArray

        Args:
            offset: Offset from start of array to slice. Defaults to 0.
            length: Length of slice (default is until end of batch starting from offset).

        Returns:
            New ChunkedArray
        """
    def to_numpy(self) -> NDArray:
        """Copy this array to a `numpy` NDArray"""
    def to_pylist(self) -> NDArray:
        """Convert to a list of native Python objects."""
    @property
    def type(self) -> DataType:
        """Return data type of a ChunkedArray."""
