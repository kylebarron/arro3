from typing import Literal, Sequence

from ._field import Field
from .types import ArrowSchemaExportable

class DataType:
    """An Arrow DataType."""
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
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowSchemaExportable) -> DataType:
        """Construct this from an existing Arrow object.

        It can be called on anything that exports the Arrow schema interface
        (has an `__arrow_c_schema__` method).
        """
    @classmethod
    def from_arrow_pycapsule(cls, capsule) -> DataType:
        """Construct this object from a bare Arrow PyCapsule"""
    @property
    def bit_width(self) -> Literal[8, 16, 32, 64] | None:
        """Returns the bit width of this type if it is a primitive type

        Returns `None` if not a primitive type
        """
    def equals(
        self, other: ArrowSchemaExportable, *, check_metadata: bool = False
    ) -> bool:
        """Return true if type is equivalent to passed value.

        Args:
            other: _description_
            check_metadata: Whether nested Field metadata equality should be checked as well. Defaults to False.

        Returns:
            _description_
        """
    @property
    def list_size(self) -> int | None:
        """The size of the list in the case of fixed size lists.

        This will return `None` if the data type is not a fixed size list.

        Examples:

        ```py
        from arro3.core import DataType
        DataType.list(DataType.int32(), 2).list_size
        # 2
        ```

        Returns:
            _description_
        """
    @property
    def num_fields(self) -> int:
        """The number of child fields."""
    @property
    def time_unit(self) -> Literal["s", "ms", "us", "ns"] | None:
        """The time unit, if the data type has one."""
    @property
    def tz(self) -> str | None:
        """The timestamp time zone, if any, or None."""
    @property
    def value_type(self) -> DataType | None:
        """The child type, if it exists."""

    @property
    def value_field(self) -> Field | None:
        """The child field, if it exists. Only applicable to list types."""

    @property
    def fields(self) -> Sequence[Field]:
        """The inner fields, if they exists. Only applicable to Struct type."""

    #################
    #### Constructors
    #################
    @classmethod
    def null(cls) -> DataType:
        """Create instance of null type."""
    @classmethod
    def bool(cls) -> DataType:
        """Create instance of boolean type."""
    @classmethod
    def int8(cls) -> DataType:
        """Create instance of signed int8 type."""
    @classmethod
    def int16(cls) -> DataType:
        """Create instance of signed int16 type."""
    @classmethod
    def int32(cls) -> DataType:
        """Create instance of signed int32 type."""
    @classmethod
    def int64(cls) -> DataType:
        """Create instance of signed int64 type."""
    @classmethod
    def uint8(cls) -> DataType:
        """Create instance of unsigned int8 type."""
    @classmethod
    def uint16(cls) -> DataType:
        """Create instance of unsigned int16 type."""
    @classmethod
    def uint32(cls) -> DataType:
        """Create instance of unsigned int32 type."""
    @classmethod
    def uint64(cls) -> DataType:
        """Create instance of unsigned int64 type."""
    @classmethod
    def float16(cls) -> DataType:
        """Create half-precision floating point type."""
    @classmethod
    def float32(cls) -> DataType:
        """Create single-precision floating point type."""
    @classmethod
    def float64(cls) -> DataType:
        """Create double-precision floating point type."""
    @classmethod
    def time32(cls, unit: Literal["s", "ms"]) -> DataType:
        """Create instance of 32-bit time (time of day) type with unit resolution.

        Args:
            unit: one of `'s'` [second], or `'ms'` [millisecond]

        Returns:
            _description_
        """
    @classmethod
    def time64(cls, unit: Literal["us", "ns"]) -> DataType:
        """Create instance of 64-bit time (time of day) type with unit resolution.

        Args:
            unit: One of `'us'` [microsecond], or `'ns'` [nanosecond].

        Returns:
            _description_
        """
    @classmethod
    def timestamp(
        cls, unit: Literal["s", "ms", "us", "ns"], *, tz: str | None = None
    ) -> DataType:
        """Create instance of timestamp type with resolution and optional time zone.

        Args:
            unit: one of `'s'` [second], `'ms'` [millisecond], `'us'` [microsecond], or `'ns'` [nanosecond]
            tz: Time zone name. None indicates time zone naive. Defaults to None.

        Returns:
            _description_
        """
    @classmethod
    def date32(cls) -> DataType:
        """Create instance of 32-bit date (days since UNIX epoch 1970-01-01)."""
    @classmethod
    def date64(cls) -> DataType:
        """Create instance of 64-bit date (milliseconds since UNIX epoch 1970-01-01)."""
    @classmethod
    def duration(cls, unit: Literal["s", "ms", "us", "ns"]) -> DataType:
        """Create instance of a duration type with unit resolution.

        Args:
            unit: one of `'s'` [second], `'ms'` [millisecond], `'us'` [microsecond], or `'ns'` [nanosecond]

        Returns:
            _description_
        """
    @classmethod
    def month_day_nano_interval(cls) -> DataType:
        """
        Create instance of an interval type representing months, days and nanoseconds
        between two dates.
        """
    @classmethod
    def binary(cls, length: int | None = None) -> DataType:
        """Create variable-length or fixed size binary type.

        Args:
            length: If length is `None` then return a variable length binary type. If length is provided, then return a fixed size binary type of width `length`. Defaults to None.

        Returns:
            _description_
        """
    @classmethod
    def string(cls) -> DataType:
        """Create UTF8 variable-length string type."""
    @classmethod
    def utf8(cls) -> DataType:
        """Alias for string()."""
    @classmethod
    def large_binary(cls) -> DataType:
        """Create large variable-length binary type."""
    @classmethod
    def large_string(cls) -> DataType:
        """Create large UTF8 variable-length string type."""
    @classmethod
    def large_utf8(cls) -> DataType:
        """Alias for large_string()."""
    @classmethod
    def binary_view(cls) -> DataType:
        """Create a variable-length binary view type."""
    @classmethod
    def string_view(cls) -> DataType:
        """Create UTF8 variable-length string view type."""
    @classmethod
    def decimal128(cls, precision: int, scale: int) -> DataType:
        """Create decimal type with precision and scale and 128-bit width.

        Arrow decimals are fixed-point decimal numbers encoded as a scaled integer. The
        precision is the number of significant digits that the decimal type can
        represent; the scale is the number of digits after the decimal point (note the
        scale can be negative).

        As an example, `decimal128(7, 3)` can exactly represent the numbers 1234.567 and
        -1234.567 (encoded internally as the 128-bit integers 1234567 and -1234567,
        respectively), but neither 12345.67 nor 123.4567.

        `decimal128(5, -3)` can exactly represent the number 12345000 (encoded
        internally as the 128-bit integer 12345), but neither 123450000 nor 1234500.

        If you need a precision higher than 38 significant digits, consider using
        `decimal256`.

        Args:
            precision: Must be between 1 and 38 scale: _description_
        """
    @classmethod
    def decimal256(cls, precision: int, scale: int) -> DataType:
        """Create decimal type with precision and scale and 256-bit width."""
    @classmethod
    def list(
        cls, value_type: ArrowSchemaExportable, list_size: int | None = None
    ) -> DataType:
        """Create ListType instance from child data type or field.

        Args:
            value_type: _description_
            list_size: If length is `None` then return a variable length list type. If length is provided then return a fixed size list type.

        Returns:
            _description_
        """
    @classmethod
    def large_list(cls, value_type: ArrowSchemaExportable) -> DataType:
        """Create LargeListType instance from child data type or field.

        This data type may not be supported by all Arrow implementations. Unless you
        need to represent data larger than 2**31 elements, you should prefer `list()`.

        Args:
            value_type: _description_

        Returns:
            _description_
        """
    @classmethod
    def list_view(cls, value_type: ArrowSchemaExportable) -> DataType:
        """
        Create ListViewType instance from child data type or field.

        This data type may not be supported by all Arrow implementations because it is
        an alternative to the ListType.

        """
    @classmethod
    def large_list_view(cls, value_type: ArrowSchemaExportable) -> DataType:
        """Create LargeListViewType instance from child data type or field.

        This data type may not be supported by all Arrow implementations because it is
        an alternative to the ListType.

        Args:
            value_type: _description_

        Returns:
            _description_
        """

    @classmethod
    def map(
        cls,
        key_type: ArrowSchemaExportable,
        item_type: ArrowSchemaExportable,
        keys_sorted: bool,
    ) -> DataType:
        """Create MapType instance from key and item data types or fields.

        Args:
            key_type: _description_
            item_type: _description_
            keys_sorted: _description_

        Returns:
            _description_
        """

    @classmethod
    def struct(cls, fields: Sequence[ArrowSchemaExportable]) -> DataType:
        """Create StructType instance from fields.

        A struct is a nested type parameterized by an ordered sequence of types (which
        can all be distinct), called its fields.

        Args:
            fields: Each field must have a UTF8-encoded name, and these field names are part of the type metadata.

        Returns:
            _description_
        """

    @classmethod
    def dictionary(
        cls, index_type: ArrowSchemaExportable, value_type: ArrowSchemaExportable
    ) -> DataType:
        """Dictionary (categorical, or simply encoded) type.

        Args:
            index_type: _description_
            value_type: _description_

        Returns:
            _description_
        """

    @classmethod
    def run_end_encoded(
        cls, run_end_type: ArrowSchemaExportable, value_type: ArrowSchemaExportable
    ) -> DataType:
        """Create RunEndEncodedType from run-end and value types.

        Args:
            run_end_type: The integer type of the run_ends array. Must be `'int16'`, `'int32'`, or `'int64'`.
            value_type: The type of the values array.

        Returns:
            _description_
        """

    ##################
    #### Type Checking
    ##################
    @staticmethod
    def is_boolean(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_integer(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_signed_integer(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_unsigned_integer(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_int8(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_int16(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_int32(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_int64(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_uint8(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_uint16(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_uint32(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_uint64(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_floating(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_float16(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_float32(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_float64(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_decimal(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_decimal128(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_decimal256(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_list(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_large_list(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_fixed_size_list(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_list_view(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_large_list_view(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_struct(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_union(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_nested(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_run_end_encoded(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_temporal(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_timestamp(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_date(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_date32(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_date64(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_time(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_time32(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_time64(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_duration(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_interval(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_null(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_binary(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_unicode(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_string(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_large_binary(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_large_unicode(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_large_string(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_binary_view(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_string_view(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_fixed_size_binary(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_map(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_dictionary(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_primitive(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_numeric(t: ArrowSchemaExportable) -> bool: ...
    @staticmethod
    def is_dictionary_key_type(t: ArrowSchemaExportable) -> bool: ...
