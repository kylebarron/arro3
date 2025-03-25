from typing import Sequence

from ._record_batch import RecordBatch
from ._schema import Schema
from ._table import Table
from .types import ArrowArrayExportable, ArrowSchemaExportable, ArrowStreamExportable

class RecordBatchReader:
    """An Arrow RecordBatchReader.

    A RecordBatchReader holds a stream of [`RecordBatch`][arro3.core.RecordBatch].
    """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this RecordBatchReader.
        Then the consumer can ask the producer (in `__arrow_c_stream__`) to cast the
        exported data to a supported data type.
        """
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call
        [`pyarrow.RecordBatchReader.from_stream`][pyarrow.RecordBatchReader.from_stream]
        to convert this stream to a pyarrow `RecordBatchReader`. Alternatively, you can
        call [`pyarrow.table()`][pyarrow.table] to consume this stream to a pyarrow
        table or [`Table.from_arrow()`][arro3.core.Table] to consume this stream to an
        arro3 Table.
        """
    def __iter__(self) -> RecordBatchReader: ...
    def __next__(self) -> RecordBatch: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(
        cls, input: ArrowArrayExportable | ArrowStreamExportable
    ) -> RecordBatchReader:
        """
        Construct this from an existing Arrow object.

        It can be called on anything that exports the Arrow stream interface
        (has an `__arrow_c_stream__` method), such as a `Table` or `RecordBatchReader`.
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> RecordBatchReader:
        """Construct this object from a bare Arrow PyCapsule"""
    @classmethod
    def from_batches(
        cls, schema: ArrowSchemaExportable, batches: Sequence[ArrowArrayExportable]
    ) -> RecordBatchReader:
        """Construct a new RecordBatchReader from existing data.

        Args:
            schema: The schema of the Arrow batches.
            batches: The existing batches.
        """
    @classmethod
    def from_stream(cls, data: ArrowStreamExportable) -> RecordBatchReader:
        """Import a RecordBatchReader from an object that exports an Arrow C Stream."""
    @property
    def closed(self) -> bool:
        """Returns `true` if this reader has already been consumed."""
    def read_all(self) -> Table:
        """Read all batches into a Table."""
    def read_next_batch(self) -> RecordBatch:
        """Read the next batch in the stream."""
    @property
    def schema(self) -> Schema:
        """Access the schema of this table."""
