from typing import Sequence, overload

from ._array import Array
from ._field import Field
from ._schema import Schema
from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

class RecordBatch:
    """
    A two-dimensional batch of column-oriented data with a defined
    [schema][arro3.core.Schema].

    A `RecordBatch` is a two-dimensional dataset of a number of contiguous arrays, each
    the same length. A record batch has a schema which must match its arrays' datatypes.

    Record batches are a convenient unit of work for various serialization and
    computation functions, possibly incremental.
    """
    @overload
    def __init__(
        self,
        data: ArrowArrayExportable,
        *,
        schema: None = None,
        metadata: None = None,
    ) -> None: ...
    # @overload
    # def __init__(
    #     self,
    #     data: Sequence[ArrowArrayExportable],
    #     *,
    #     names: Sequence[str],
    #     schema: None = None,
    #     metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    # ) -> None: ...
    @overload
    def __init__(
        self,
        data: Sequence[ArrayInput],
        *,
        # names: None = None,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: dict[str, ArrayInput],
        *,
        # names: None = None,
        schema: None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: dict[str, ArrayInput],
        *,
        # names: None = None,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> None: ...
    def __init__(
        self,
        data: ArrayInput | dict[str, ArrayInput],
        *,
        schema: ArrowSchemaExportable | None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.record_batch()`][pyarrow.record_batch] to
        convert this RecordBatch into a pyarrow RecordBatch, without copying memory.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this RecordBatch. Then
        the consumer can ask the producer (in `__arrow_c_array__`) to cast the exported
        data to a supported data type.
        """
    def __eq__(self, other) -> bool: ...
    def __getitem__(self, key: int | str) -> Array: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrays(
        cls, arrays: Sequence[ArrayInput], *, schema: ArrowSchemaExportable
    ) -> RecordBatch:
        """Construct a RecordBatch from multiple Arrays

        Args:
            arrays: One for each field in RecordBatch
            schema: Schema for the created batch. If not passed, names must be passed

        Returns:
            _description_
        """
    @classmethod
    def from_pydict(
        cls,
        mapping: dict[str, ArrayInput],
        *,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> RecordBatch:
        """Construct a Table or RecordBatch from Arrow arrays or columns.

        Args:
            mapping: A mapping of strings to Arrays.
            metadata: Optional metadata for the schema (if inferred). Defaults to None.

        Returns:
            _description_
        """
    @classmethod
    def from_struct_array(cls, struct_array: ArrowArrayExportable) -> RecordBatch:
        """Construct a RecordBatch from a StructArray.

        Each field in the StructArray will become a column in the resulting RecordBatch.

        Args:
            struct_array: Array to construct the record batch from.

        Returns:
            New RecordBatch
        """
    @classmethod
    def from_arrow(
        cls, input: ArrowArrayExportable | ArrowStreamExportable
    ) -> RecordBatch:
        """

        Construct this from an existing Arrow RecordBatch.


        It can be called on anything that exports the Arrow data interface
        (has a `__arrow_c_array__` method) and returns a StructArray..


        Args:
            input: Arrow array to use for constructing this object


        Returns:
            new RecordBatch
        """
    @classmethod
    def from_arrow_pycapsule(cls, schema_capsule, array_capsule) -> RecordBatch:
        """Construct this object from bare Arrow PyCapsules"""
    def add_column(
        self, i: int, field: str | ArrowSchemaExportable, column: ArrayInput
    ) -> RecordBatch:
        """Add column to RecordBatch at position.

        A new RecordBatch is returned with the column added, the original RecordBatch
        object is left unchanged.

        Args:
            i: Index to place the column at.
            field: _description_
            column: Column data.

        Returns:
            New RecordBatch with the passed column added.
        """
    def append_column(
        self, field: str | ArrowSchemaExportable, column: ArrayInput
    ) -> RecordBatch:
        """Append column at end of columns.

        Args:
            field: If a string is passed then the type is deduced from the column data.
            column: Column data

        Returns:
            _description_
        """

    def column(self, i: int | str) -> Array:
        """Select single column from Table or RecordBatch.

        Args:
            i: The index or name of the column to retrieve.

        Returns:
            _description_
        """
    @property
    def column_names(self) -> list[str]:
        """Names of the RecordBatch columns."""
    @property
    def columns(self) -> list[Array]:
        """List of all columns in numerical order."""
    def equals(self, other: ArrowArrayExportable) -> bool:
        """Check if contents of two record batches are equal.

        Args:
            other: RecordBatch to compare against.

        Returns:
            _description_
        """

    def field(self, i: int | str) -> Field:
        """Select a schema field by its column name or numeric index.

        Args:
            i: The index or name of the field to retrieve.

        Returns:
            _description_
        """
    @property
    def nbytes(self) -> int:
        """Total number of bytes consumed by the elements of the record batch."""
    @property
    def num_columns(self) -> int:
        """Number of columns."""
    @property
    def num_rows(self) -> int:
        """Number of rows

        Due to the definition of a RecordBatch, all columns have the same number of
        rows.
        """
    def remove_column(self, i: int) -> RecordBatch:
        """Create new RecordBatch with the indicated column removed.

        Args:
            i: Index of column to remove.

        Returns:
            New record batch without the column.
        """
    @property
    def schema(self) -> Schema:
        """Access the schema of this RecordBatch"""
    def select(self, columns: list[int] | list[str]) -> RecordBatch:
        """
        Select columns of the RecordBatch.

        Returns a new RecordBatch with the specified columns, and metadata preserved.


        Args:
            columns: The column names or integer indices to select.

        Returns:
            New RecordBatch.
        """
    def set_column(
        self, i: int, field: str | ArrowSchemaExportable, column: ArrayInput
    ) -> RecordBatch:
        """Replace column in RecordBatch at position.

        Args:
            i: Index to place the column at.
            field: If a string is passed then the type is deduced from the column data.
            column: Column data.

        Returns:
            New RecordBatch.
        """
    @property
    def shape(self) -> tuple[int, int]:
        """
        Dimensions of the table or record batch: (number of rows, number of columns).
        """
    def slice(self, offset: int = 0, length: int | None = None) -> RecordBatch:
        """Compute zero-copy slice of this RecordBatch

        Args:
            offset: Offset from start of record batch to slice. Defaults to 0.
            length: Length of slice (default is until end of batch starting from offset). Defaults to None.

        Returns:
            New RecordBatch.
        """
    def take(self, indices: ArrayInput) -> RecordBatch:
        """Select rows from a Table or RecordBatch.

        Args:
            indices: The indices in the tabular object whose rows will be returned.

        Returns:
            _description_
        """
    def to_struct_array(self) -> Array:
        """Convert to a struct array.

        Returns:
            _description_
        """
    def with_schema(self, schema: ArrowSchemaExportable) -> RecordBatch:
        """Return a RecordBatch with the provided schema."""
