from typing import Sequence, overload

from ._chunked_array import ChunkedArray
from ._field import Field
from ._record_batch import RecordBatch
from ._record_batch_reader import RecordBatchReader
from ._schema import Schema
from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

class Table:
    """A collection of top-level named, equal length Arrow arrays."""
    @overload
    def __init__(
        self,
        data: ArrowArrayExportable | ArrowStreamExportable,
        *,
        names: None = None,
        schema: None = None,
        metadata: None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: Sequence[ArrayInput | ArrowStreamExportable],
        *,
        names: Sequence[str],
        schema: None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: Sequence[ArrayInput | ArrowStreamExportable],
        *,
        names: None = None,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: dict[str, ArrayInput | ArrowStreamExportable],
        *,
        names: None = None,
        schema: None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    @overload
    def __init__(
        self,
        data: dict[str, ArrayInput | ArrowStreamExportable],
        *,
        names: None = None,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> None: ...
    def __init__(
        self,
        data: ArrowArrayExportable
        | ArrowStreamExportable
        | Sequence[ArrayInput | ArrowStreamExportable]
        | dict[str, ArrayInput | ArrowStreamExportable],
        *,
        names: Sequence[str] | None = None,
        schema: ArrowSchemaExportable | None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None:
        """Create a Table from a Python data structure or sequence of arrays.

        Args:
            data: A mapping of strings to Arrow Arrays, a list of arrays or chunked arrays, or any tabular object implementing the Arrow PyCapsule Protocol (has an __arrow_c_array__ or __arrow_c_stream__ method).
            names: Column names if list of arrays passed as data. Mutually exclusive with 'schema' argument. Defaults to None.
            schema: The expected schema of the Arrow Table. If not passed, will be inferred from the data. Mutually exclusive with 'names' argument. Defaults to None.
            metadata: Optional metadata for the schema (if schema not passed). Defaults to None.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this Table. Then the
        consumer can ask the producer (in `__arrow_c_stream__`) to cast the exported
        data to a supported data type.
        """
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this
        array into a pyarrow table, without copying memory.
        """
    def __eq__(self, other) -> bool: ...
    def __getitem__(self, key: int | str) -> ChunkedArray: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @overload
    @classmethod
    def from_arrays(
        cls,
        arrays: Sequence[ArrayInput | ArrowStreamExportable],
        *,
        names: Sequence[str],
        schema: None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> Table: ...
    @overload
    @classmethod
    def from_arrays(
        cls,
        arrays: Sequence[ArrayInput | ArrowStreamExportable],
        *,
        names: None = None,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> Table: ...
    @classmethod
    def from_arrays(
        cls,
        arrays: Sequence[ArrayInput | ArrowStreamExportable],
        *,
        names: Sequence[str] | None = None,
        schema: ArrowSchemaExportable | None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> Table:
        """Construct a Table from Arrow arrays.

        Args:
            arrays: Equal-length arrays that should form the table.
            names: Names for the table columns. If not passed, `schema` must be passed. Defaults to None.
            schema: Schema for the created table. If not passed, `names` must be passed. Defaults to None.
            metadata: Optional metadata for the schema (if inferred). Defaults to None.

        Returns:
            new table
        """
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable | ArrowStreamExportable) -> Table:
        """
        Construct this object from an existing Arrow object.

        It can be called on anything that exports the Arrow stream interface
        (`__arrow_c_stream__`) and yields a StructArray for each item. This Table will
        materialize all items from the iterator in memory at once. Use
        [`RecordBatchReader`] if you don't wish to materialize all batches in memory at
        once.

        Args:
            input: Arrow stream to use for constructing this object

        Returns:
            Self
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule: object) -> Table:
        """Construct this object from a bare Arrow PyCapsule

        Args:
            capsule: raw Arrow PyCapsule.
        """
    @classmethod
    def from_batches(
        cls,
        batches: Sequence[ArrowArrayExportable],
        *,
        schema: ArrowSchemaExportable | None = None,
    ) -> Table:
        """Construct a Table from a sequence of Arrow RecordBatches.

        Args:
            batches: Sequence of RecordBatch to be converted, all schemas must be equal.
            schema: If not passed, will be inferred from the first RecordBatch. Defaults to None.

        Returns:
            New Table.
        """
    @overload
    @classmethod
    def from_pydict(
        cls,
        mapping: dict[str, ArrayInput | ArrowStreamExportable],
        *,
        schema: None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> Table: ...
    @overload
    @classmethod
    def from_pydict(
        cls,
        mapping: dict[str, ArrayInput | ArrowStreamExportable],
        *,
        schema: ArrowSchemaExportable,
        metadata: None = None,
    ) -> Table: ...
    @classmethod
    def from_pydict(
        cls,
        mapping: dict[str, ArrayInput | ArrowStreamExportable],
        *,
        schema: ArrowSchemaExportable | None = None,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> Table:
        """Construct a Table or RecordBatch from Arrow arrays or columns.

        Args:
            mapping: A mapping of strings to Arrays.
            schema: If not passed, will be inferred from the Mapping values. Defaults to None.
            metadata: Optional metadata for the schema (if inferred). Defaults to None.

        Returns:
            new table
        """
    def add_column(
        self, i: int, field: str | ArrowSchemaExportable, column: ArrowStreamExportable
    ) -> Table:
        """Add column to Table at position.

        A new table is returned with the column added, the original table object is left unchanged.

        Args:
            i: Index to place the column at.
            field: _description_
            column: Column data.

        Returns:
            New table with the passed column added.
        """
    def append_column(
        self, field: str | ArrowSchemaExportable, column: ArrowStreamExportable
    ) -> Table:
        """Append column at end of columns.

        Args:
            field: _description_
            column: Column data.

        Returns:
            New table or record batch with the passed column added.
        """
    @property
    def chunk_lengths(self) -> list[int]:
        """The number of rows in each internal chunk."""
    def column(self, i: int | str) -> ChunkedArray:
        """Select single column from Table or RecordBatch.

        Args:
            i: The index or name of the column to retrieve.

        Returns:
            _description_
        """
    @property
    def column_names(self) -> list[str]:
        """Names of the Table or RecordBatch columns.

        Returns:
            _description_
        """
    @property
    def columns(self) -> list[ChunkedArray]:
        """List of all columns in numerical order.

        Returns:
            _description_
        """
    def combine_chunks(self) -> Table:
        """Make a new table by combining the chunks this table has.

        All the underlying chunks in the ChunkedArray of each column are concatenated
        into zero or one chunk.

        Returns:
            new Table with one or zero chunks.
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
        """Total number of bytes consumed by the elements of the table."""
    @property
    def num_columns(self) -> int:
        """Number of columns in this table."""
    @property
    def num_rows(self) -> int:
        """Number of rows in this table.

        Due to the definition of a table, all columns have the same number of rows.
        """
    def rechunk(self, *, max_chunksize: int | None = None) -> Table:
        """Rechunk a table with a maximum number of rows per chunk.

        Args:
            max_chunksize: The maximum number of rows per internal RecordBatch. Defaults to None, which rechunks into a single batch.

        Returns:
            The rechunked table.
        """
    def remove_column(self, i: int) -> Table:
        """Create new Table with the indicated column removed.

        Args:
            i: Index of column to remove.

        Returns:
            New table without the column.
        """
    def rename_columns(self, names: Sequence[str]) -> Table:
        """Create new table with columns renamed to provided names.

        Args:
            names: List of new column names.

        Returns:
            _description_
        """
    @property
    def schema(self) -> Schema:
        """Schema of the table and its columns.

        Returns:
            _description_
        """
    def select(self, columns: Sequence[int] | Sequence[str]) -> Table:
        """Select columns of the Table.

        Returns a new Table with the specified columns, and metadata preserved.

        Args:
            columns: The column names or integer indices to select.

        Returns:
            _description_
        """
    def set_column(
        self, i: int, field: str | ArrowSchemaExportable, column: ArrowStreamExportable
    ) -> Table:
        """Replace column in Table at position.

        Args:
            i: Index to place the column at.
            field: _description_
            column: Column data.

        Returns:
            _description_
        """
    @property
    def shape(self) -> tuple[int, int]:
        """Dimensions of the table or record batch

        Returns:
            (number of rows, number of columns)
        """
    def slice(self, offset: int = 0, length: int | None = None) -> Table:
        """Compute zero-copy slice of this table.

        Args:
            offset: Defaults to 0.
            length: Defaults to None.

        Returns:
            The sliced table
        """
    def to_batches(self) -> list[RecordBatch]:
        """Convert Table to a list of RecordBatch objects.

        Note that this method is zero-copy, it merely exposes the same data under a
        different API.

        Returns:
            _description_
        """
    def to_reader(self) -> RecordBatchReader:
        """Convert the Table to a RecordBatchReader.

        Note that this method is zero-copy, it merely exposes the same data under a
        different API.

        Returns:
            _description_
        """
    def to_struct_array(self) -> ChunkedArray:
        """Convert to a chunked array of struct type.

        Returns:
            _description_
        """
    def with_schema(self, schema: ArrowSchemaExportable) -> Table:
        """Assign a different schema onto this table.

        The new schema must be compatible with the existing data; this does not cast the
        underlying data to the new schema. This is primarily useful for changing the
        schema metadata.

        Args:
            schema: _description_

        Returns:
            _description_
        """
