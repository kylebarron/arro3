from typing import Any, overload

from ._data_type import DataType
from ._field import Field
from .types import ArrayInput, ArrowArrayExportable, ArrowSchemaExportable

class Scalar:
    """An arrow Scalar."""
    @overload
    def __init__(self, obj: ArrayInput, /, type: None = None) -> None: ...
    @overload
    def __init__(self, obj: Any, /, type: ArrowSchemaExportable) -> None: ...
    def __init__(
        self,
        obj: ArrayInput | Any,
        /,
        type: ArrowSchemaExportable | None = None,
    ) -> None:
        """Create arro3.Scalar instance from a Python object.

        Args:
            obj: An input object.
            type: Explicit type to attempt to coerce to. You may pass in a `Field` to `type` in order to associate extension metadata with this array.
        """
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to
        convert this Scalar into a pyarrow Array, without copying memory. The generated
        array is guaranteed to have length 1.
        """
    def __eq__(self, other) -> bool:
        """Check for equality with other Python objects (`==`)

        If `other` is not an Arrow scalar, `self` will be converted to a Python object
        (with `as_py`), and then its `__eq__` method will be called.
        """
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable) -> Scalar:
        """Construct this from an existing Arrow Scalar.

        It can be called on anything that exports the Arrow data interface (has a
        `__arrow_c_array__` method) and returns an array with a single element.

        Args:
            input: Arrow scalar to use for constructing this object

        Returns:
            new Scalar
        """
    @classmethod
    def from_arrow_pycapsule(cls, schema_capsule, array_capsule) -> Scalar:
        """Construct this object from bare Arrow PyCapsules"""
    def as_py(self) -> Any:
        """Convert this scalar to a pure-Python object."""
    def cast(self, target_type: ArrowSchemaExportable) -> Scalar:
        """Cast scalar to another data type

        Args:
            target_type: Type to cast to.
        """

    @property
    def field(self) -> Field:
        """Access the field stored on this Scalar.

        Note that this field usually will not have a name associated, but it may have
        metadata that signifies that this scalar is an extension (user-defined typed)
        scalar.
        """

    @property
    def is_valid(self) -> bool:
        """Return `True` if this scalar is not null."""
    @property
    def type(self) -> DataType:
        """Access the type of this scalar."""
