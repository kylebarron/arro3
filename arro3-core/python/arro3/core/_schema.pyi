from typing import Sequence

from ._data_type import DataType
from ._field import Field
from ._table import Table
from .types import ArrowSchemaExportable

class Schema:
    """An arrow Schema."""
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
        """

    def __eq__(self, other) -> bool: ...
    def __getitem__(self, key: int | str) -> Field: ...
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
