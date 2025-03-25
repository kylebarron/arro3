from ._data_type import DataType
from .types import ArrowSchemaExportable

class Field:
    """An Arrow Field."""
    def __init__(
        self,
        name: str,
        type: ArrowSchemaExportable,
        nullable: bool = True,
        *,
        metadata: dict[str, str] | dict[bytes, bytes] | None = None,
    ) -> None: ...
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.field()`][pyarrow.field] to convert this
        array into a pyarrow field, without copying memory.
        """
    def __eq__(self, other) -> bool: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowSchemaExportable) -> Field:
        """Construct this from an existing Arrow object.

        It can be called on anything that exports the Arrow schema interface
        (has an `__arrow_c_schema__` method).
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> Field:
        """Construct this object from a bare Arrow PyCapsule"""

    def equals(self, other: ArrowSchemaExportable) -> bool:
        """Test if this field is equal to the other."""
    @property
    def metadata(self) -> dict[bytes, bytes]:
        """The schema's metadata."""
    @property
    def metadata_str(self) -> dict[str, str]:
        """The schema's metadata where keys and values are `str`, not `bytes`."""
    @property
    def name(self) -> str:
        """The field name."""
    @property
    def nullable(self) -> bool:
        """The field nullability."""
    def remove_metadata(self) -> Field:
        """Create new field without metadata, if any."""
    @property
    def type(self) -> DataType:
        """Access the data type of this field."""
    def with_metadata(self, metadata: dict[str, str] | dict[bytes, bytes]) -> Field:
        """Add metadata as dict of string keys and values to Field."""
    def with_name(self, name: str) -> Field:
        """A copy of this field with the replaced name."""
    def with_nullable(self, nullable: bool) -> Field:
        """A copy of this field with the replaced nullability."""
    def with_type(self, new_type: ArrowSchemaExportable) -> Field:
        """A copy of this field with the replaced type"""
