from typing import Sequence

from ._array import Array
from ._chunked_array import ChunkedArray
from ._field import Field
from .types import ArrowArrayExportable, ArrowSchemaExportable, ArrowStreamExportable

class ArrayReader:
    """A stream of Arrow `Array`s.

    This is similar to the [`RecordBatchReader`][arro3.core.RecordBatchReader] but each
    item yielded from the stream is an [`Array`][arro3.core.Array], not a
    [`RecordBatch`][arro3.core.RecordBatch].
    """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this ArrayReader. Then
        the consumer can ask the producer (in `__arrow_c_stream__`) to cast the exported
        data to a supported data type.
        """
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.chunked_array()`][pyarrow.chunked_array] to
        convert this ArrayReader to a pyarrow ChunkedArray, without copying memory.
        """
    def __iter__(self) -> ArrayReader: ...
    def __next__(self) -> Array: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(
        cls, input: ArrowArrayExportable | ArrowStreamExportable
    ) -> ArrayReader:
        """Construct this from an existing Arrow object.

        It can be called on anything that exports the Arrow stream interface
        (has an `__arrow_c_stream__` method), such as a `Table` or `ArrayReader`.
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> ArrayReader:
        """Construct this object from a bare Arrow PyCapsule"""
    @classmethod
    def from_arrays(
        cls, field: ArrowSchemaExportable, arrays: Sequence[ArrowArrayExportable]
    ) -> ArrayReader:
        """Construct an ArrayReader from existing data.

        Args:
            field: The Arrow field that describes the sequence of array data.
            arrays: A sequence (list or tuple) of Array data.
        """
    @classmethod
    def from_stream(cls, data: ArrowStreamExportable) -> ArrayReader:
        """Construct this from an existing Arrow object.

        This is an alias of and has the same behavior as
        [`from_arrow`][arro3.core.ArrayReader.from_arrow], but is included for parity
        with [`pyarrow.RecordBatchReader`][pyarrow.RecordBatchReader].
        """
    @property
    def closed(self) -> bool:
        """Returns `true` if this reader has already been consumed."""
    def read_all(self) -> ChunkedArray:
        """Read all batches from this stream into a ChunkedArray."""
    def read_next_array(self) -> Array:
        """Read the next array from this stream."""
    @property
    def field(self) -> Field:
        """Access the field of this reader."""
