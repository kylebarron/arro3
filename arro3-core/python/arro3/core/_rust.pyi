from typing import Any, Sequence
import numpy as np
from numpy.typing import NDArray

from .types import (
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
)

class Array:
    def __init__(self, obj: Sequence[Any], /, type: ArrowSchemaExportable) -> None:
        """Create arro3.core.Array instance from a sequence of Python objects.

        Args:
            obj: A sequence of input objects.
            type: Explicit type to attempt to coerce to.
        """
    def __array__(self) -> NDArray: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...
    def __eq__(self, other) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Array:
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

    @classmethod
    def from_numpy(cls, array: np.ndarray, type: ArrowSchemaExportable) -> Array:
        """Construct an Array from a numpy ndarray"""

    def to_numpy(self) -> NDArray:
        """Return a numpy copy of this array."""

    def slice(self, offset: int = 0, length: int | None = None) -> Array:
        """Compute zero-copy slice of this array.

        Args:
            offset: Defaults to 0.
            length: Defaults to None.

        Returns:
            The sliced array
        """

    @property
    def type(self) -> DataType:
        """The data type of this array."""

class ArrayReader:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowStreamExportable) -> ArrayReader: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> ArrayReader:
        """Construct this object from a bare Arrow PyCapsule"""
    @classmethod
    def from_arrays(
        cls, schema: ArrowSchemaExportable, arrays: Sequence[ArrowArrayExportable]
    ) -> ArrayReader: ...
    @classmethod
    def from_stream(cls, data: ArrowStreamExportable) -> ArrayReader: ...
    @property
    def closed(self) -> bool: ...
    def read_all(self) -> ChunkedArray: ...
    def read_next_array(self) -> Array: ...
    def field(self) -> Field: ...

class ChunkedArray:
    def __init__(
        self,
        arrays: Sequence[ArrowArrayExportable],
        type: ArrowSchemaExportable | None = None,
    ) -> None: ...
    def __array__(self) -> NDArray: ...
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __eq__(self, other) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowStreamExportable) -> ChunkedArray: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> ChunkedArray:
        """Construct this object from a bare Arrow PyCapsule"""
    def chunk(self, i: int) -> Array: ...
    @property
    def chunks(self) -> list[Array]: ...
    def combine_chunks(self) -> Array: ...
    def equals(self, other: ArrowStreamExportable) -> bool: ...
    def length(self) -> int: ...
    @property
    def null_count(self) -> int: ...
    @property
    def num_chunks(self) -> int: ...
    def slice(self, offset: int = 0, length: int | None = None) -> ChunkedArray: ...
    def to_numpy(self) -> NDArray: ...
    @property
    def type(self) -> DataType: ...

class DataType:
    def __arrow_c_schema__(self) -> object: ...
    def __eq__(self, other) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowSchemaExportable) -> DataType: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> DataType:
        """Construct this object from a bare Arrow PyCapsule"""
    def bit_width(self) -> int | None: ...

class Field:
    def __init__(
        self,
        name: str,
        type: ArrowSchemaExportable,
        nullable: bool = True,
        *,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    def __arrow_c_schema__(self) -> object: ...
    def __eq__(self, other) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowSchemaExportable) -> Field: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> Field:
        """Construct this object from a bare Arrow PyCapsule"""

    def equals(self, other: ArrowSchemaExportable) -> bool: ...
    @property
    def metadata(self) -> dict[bytes, bytes]: ...
    @property
    def metadata_str(self) -> dict[str, str]: ...
    @property
    def name(self) -> str: ...
    @property
    def nullable(self) -> bool: ...
    def remove_metadata(self) -> Field: ...
    @property
    def type(self) -> DataType: ...
    def with_metadata(self, metadata: dict[str, str] | dict[bytes, bytes]) -> Field: ...
    def with_name(self, name: str) -> Field: ...
    def with_nullable(self, nullable: bool) -> Field: ...
    def with_type(self, new_type: ArrowSchemaExportable) -> Field: ...

class RecordBatch:
    def __init__(
        self,
        data: ArrowArrayExportable | dict[str, ArrowArrayExportable],
        *,
        metadata: ArrowSchemaExportable | None = None,
    ) -> None: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...
    def __eq__(self, other) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrays(
        cls, arrays: Sequence[ArrowArrayExportable], *, schema: ArrowSchemaExportable
    ) -> RecordBatch: ...
    @classmethod
    def from_pydict(
        cls,
        mapping: dict[str, ArrowArrayExportable],
        *,
        metadata: ArrowSchemaExportable | None = None,
    ) -> RecordBatch: ...
    @classmethod
    def from_struct_array(cls, struct_array: ArrowArrayExportable) -> RecordBatch: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> RecordBatch: ...
    @classmethod
    def from_arrow_pycapsule(cls, schema_capsule, array_capsule) -> RecordBatch:
        """Construct this object from bare Arrow PyCapsules"""
    def add_column(
        self, i: int, field: ArrowSchemaExportable, column: ArrowArrayExportable
    ) -> RecordBatch: ...
    def append_column(
        self, field: ArrowSchemaExportable, column: ArrowArrayExportable
    ) -> RecordBatch: ...
    def column(self, i: int) -> ChunkedArray: ...
    @property
    def column_names(self) -> list[str]: ...
    @property
    def columns(self) -> list[Array]: ...
    def equals(self, other: ArrowArrayExportable) -> bool: ...
    def field(self, i: int) -> Field: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def num_rows(self) -> int: ...
    def remove_column(self, i: int) -> RecordBatch: ...
    @property
    def schema(self) -> Schema: ...
    def select(self, columns: list[int]) -> RecordBatch: ...
    def set_column(
        self, i: int, field: ArrowSchemaExportable, column: ArrowArrayExportable
    ) -> RecordBatch: ...
    @property
    def shape(self) -> tuple[int, int]: ...
    def slice(self, offset: int = 0, length: int | None = None) -> RecordBatch: ...
    def to_struct_array(self) -> Array: ...
    def with_schema(self, schema: ArrowSchemaExportable) -> RecordBatch: ...

class RecordBatchReader:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowStreamExportable) -> RecordBatchReader: ...
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> RecordBatchReader:
        """Construct this object from a bare Arrow PyCapsule"""
    @classmethod
    def from_batches(
        cls, schema: ArrowSchemaExportable, batches: Sequence[ArrowArrayExportable]
    ) -> RecordBatchReader: ...
    @classmethod
    def from_stream(cls, data: ArrowStreamExportable) -> RecordBatchReader: ...
    @property
    def closed(self) -> bool: ...
    def read_all(self) -> Table: ...
    def read_next_batch(self) -> RecordBatch: ...
    def schema(self) -> Schema: ...

class Schema:
    def __init__(
        self,
        fields: Sequence[ArrowSchemaExportable],
        *,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this
        array into a pyarrow schema, without copying memory.


        Returns:
            _description_
        """

    def __eq__(self, other) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowSchemaExportable) -> Schema:
        """Construct this from an existing Arrow object

        Args:
            input: Arrow schema to use for constructing this object

        Returns:
            _description_
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> Schema:
        """Construct this object from a bare Arrow PyCapsule"""
    def append(self, field: ArrowSchemaExportable) -> Schema:
        """Append a field at the end of the schema.

        In contrast to Python's `list.append()` it does return a new object, leaving the
        original Schema unmodified.

        Args:
            field: new field

        Returns:
            New Schema
        """
    def empty_table(self) -> Table:
        """Provide an empty table according to the schema.

        Returns:
            Table
        """

    def equals(self, other: ArrowSchemaExportable) -> bool:
        """Test if this schema is equal to the other

        Args:
            other: _description_

        Returns:
            _description_
        """

    def field(self, i: int | str) -> Field:
        """Select a field by its column name or numeric index.

        Args:
            i: other

        Returns:
            _description_
        """
    def get_all_field_indices(self, name: str) -> list[int]:
        """Return sorted list of indices for the fields with the given name.

        Args:
            name: _description_

        Returns:
            _description_
        """
    def get_field_index(self, name: str) -> int:
        """Return index of the unique field with the given name.

        Args:
            name: _description_

        Returns:
            _description_
        """
    def insert(self, i: int, field: ArrowSchemaExportable) -> Schema:
        """Add a field at position `i` to the schema.

        Args:
            i: _description_
            field: _description_

        Returns:
            _description_
        """
    @property
    def metadata(self) -> dict[bytes, bytes]:
        """The schema's metadata.

        Returns:
            _description_
        """

    @property
    def metadata_str(self) -> dict[str, str]:
        """The schema's metadata where keys and values are `str`, not `bytes`.

        Returns:
            _description_
        """
    @property
    def names(self) -> list[str]:
        """The schema's field names."""

    def remove(self, i: int) -> Schema:
        """Remove the field at index i from the schema.

        Args:
            i: _description_

        Returns:
            _description_
        """
    def remove_metadata(self) -> Schema:
        """Create new schema without metadata, if any


        Returns:
            _description_
        """
    def set(self, i: int, field: ArrowSchemaExportable) -> Schema:
        """Replace a field at position `i` in the schema.

        Args:
            i: _description_
            field: _description_

        Returns:
            _description_
        """
    @property
    def types(self) -> list[DataType]:
        """The schema's field types.

        Returns:
            _description_
        """
    def with_metadata(self, metadata: dict[str, str] | dict[bytes, bytes]) -> Schema:
        """Add metadata as dict of string keys and values to Schema.

        Args:
            metadata: _description_

        Returns:
            _description_
        """

class Table:
    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this
        array into a pyarrow table, without copying memory.

        Args:
            requested_schema: _description_. Defaults to None.

        Returns:
            _description_
        """
    def __eq__(self, other) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowStreamExportable) -> Table:
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
    def from_arrow_pycapsule(cls, capsule) -> Table:
        """Construct this object from a bare Arrow PyCapsule

        Args:
            capsule: _description_

        Returns:
            _description_
        """
    def add_column(
        self, i: int, field: ArrowSchemaExportable, column: ArrowStreamExportable
    ) -> RecordBatch:
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
        self, field: ArrowSchemaExportable, column: ArrowStreamExportable
    ) -> RecordBatch:
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
    def column(self, i: int) -> ChunkedArray:
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
    def num_columns(self) -> int:
        """Number of columns in this table."""
    @property
    def num_rows(self) -> int:
        """Number of rows in this table.

        Due to the definition of a table, all columns have the same number of rows.
        """
    def set_column(
        self, i: int, field: ArrowSchemaExportable, column: ArrowStreamExportable
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
    def schema(self) -> Schema:
        """Schema of the table and its columns.

        Returns:
            _description_
        """
    @property
    def shape(self) -> tuple[int, int]:
        """Dimensions of the table or record batch

        Returns:
            (number of rows, number of columns)
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

def fixed_size_list_array(
    values: ArrowArrayExportable,
    list_size: int,
    *,
    type: ArrowSchemaExportable | None = None,
) -> Array:
    """_summary_

    Args:
        values: _description_
        list_size: _description_
        type: _description_. Defaults to None.

    Returns:
        _description_
    """

def list_array(
    offsets: ArrowArrayExportable,
    values: ArrowArrayExportable,
    *,
    type: ArrowSchemaExportable | None = None,
) -> Array:
    """_summary_

    Args:
        offsets: _description_
        values: _description_
        type: _description_. Defaults to None.

    Returns:
        _description_
    """

def struct_array(
    arrays: Sequence[ArrowArrayExportable],
    *,
    fields: Sequence[ArrowSchemaExportable],
) -> Array:
    """_summary_

    Args:
        arrays: _description_
        fields: _description_

    Returns:
        _description_
    """
