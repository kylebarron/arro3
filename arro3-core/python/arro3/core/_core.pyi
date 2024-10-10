from typing import Any, Iterable, Literal, Sequence, overload

import numpy as np
from numpy.typing import NDArray

from .types import (
    ArrayInput,
    ArrowArrayExportable,
    ArrowSchemaExportable,
    ArrowStreamExportable,
    BufferProtocolExportable,
)

class Array:
    """An Arrow Array."""
    @overload
    def __init__(self, obj: ArrayInput, /, type: None = None) -> None: ...
    @overload
    def __init__(self, obj: Sequence[Any], /, type: ArrowSchemaExportable) -> None: ...
    def __init__(
        self,
        obj: ArrayInput | Sequence[Any],
        /,
        type: ArrowSchemaExportable | None = None,
    ) -> None:
        """Create arro3.Array instance from a sequence of Python objects.

        Args:
            obj: A sequence of input objects.
            type: Explicit type to attempt to coerce to. You may pass in a `Field` to `type` in order to associate extension metadata with this array.
        """
    def __array__(self, dtype=None, copy=None) -> NDArray:
        """
        An implementation of the Array interface, for interoperability with numpy and
        other array libraries.
        """
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this
        array into a pyarrow array, without copying memory.
        """
    def __arrow_c_schema__(self) -> object:
        """
        An implementation of the [Arrow PyCapsule
        Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
        This dunder method should not be called directly, but enables zero-copy data
        transfer to other Python libraries that understand Arrow memory.

        This allows Arrow consumers to inspect the data type of this array. Then the
        consumer can ask the producer (in `__arrow_c_array__`) to cast the exported data
        to a supported data type.
        """
    def __eq__(self, other) -> bool: ...
    def __getitem__(self, i: int) -> Scalar: ...
    # Note: we don't actually implement this, but it's inferred by having a __getitem__
    # key
    def __iter__(self) -> Iterable[Scalar]: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    @classmethod
    def from_arrow(cls, input: ArrowArrayExportable | ArrowStreamExportable) -> Array:
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

    # We allow Any here because not many types have updated to expose __buffer__ yet
    @classmethod
    def from_buffer(cls, buffer: BufferProtocolExportable | Any) -> Array:
        """Construct an Array from an object implementing the Python Buffer Protocol."""

    @classmethod
    def from_numpy(cls, array: np.ndarray) -> Array:
        """Construct an Array from a numpy ndarray"""

    def cast(self, target_type: ArrowSchemaExportable) -> Array:
        """Cast array values to another data type

        Args:
            target_type: Type to cast array to.
        """

    @property
    def field(self) -> Field:
        """Access the field stored on this Array.

        Note that this field usually will not have a name associated, but it may have
        metadata that signifies that this array is an extension (user-defined typed)
        array.
        """
    @property
    def nbytes(self) -> int:
        """The number of bytes in this Array."""
    @property
    def null_count(self) -> int:
        """The number of null values in this Array."""
    def slice(self, offset: int = 0, length: int | None = None) -> Array:
        """Compute zero-copy slice of this array.

        Args:
            offset: Defaults to 0.
            length: Defaults to None.

        Returns:
            The sliced array
        """
    def take(self, indices: ArrayInput) -> Array:
        """Take specific indices from this Array."""
    def to_numpy(self) -> NDArray:
        """Return a numpy copy of this array."""
    def to_pylist(self) -> NDArray:
        """Convert to a list of native Python objects."""

    @property
    def type(self) -> DataType:
        """The data type of this array."""

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
        [`from_arrow`][arro3.ArrayReader.from_arrow], but is included for parity
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
    def from_arrow_pycapsule(cls, capsule) -> Table:
        """Construct this object from a bare Arrow PyCapsule

        Args:
            capsule: _description_

        Returns:
            _description_
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

@overload
def dictionary_dictionary(array: ArrowArrayExportable) -> Array: ...
@overload
def dictionary_dictionary(array: ArrowStreamExportable) -> ArrayReader: ...
def dictionary_dictionary(
    array: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Access the `dictionary` of a dictionary array.

    This is equivalent to the [`.dictionary`][pyarrow.DictionaryArray.dictionary]
    attribute on a PyArrow [DictionaryArray][pyarrow.DictionaryArray].

    Args:
        array: Argument to compute function.

    Returns:
        The keys of a dictionary-encoded array.
    """

@overload
def dictionary_indices(array: ArrowArrayExportable) -> Array: ...
@overload
def dictionary_indices(array: ArrowStreamExportable) -> ArrayReader: ...
def dictionary_indices(
    array: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """
    Access the indices of a dictionary array.

    This is equivalent to the [`.indices`][pyarrow.DictionaryArray.indices]
    attribute on a PyArrow [DictionaryArray][pyarrow.DictionaryArray].

    Args:
        array: Argument to compute function.

    Returns:
        The indices of a dictionary-encoded array.
    """

@overload
def list_flatten(input: ArrowArrayExportable) -> Array: ...
@overload
def list_flatten(input: ArrowStreamExportable) -> ArrayReader: ...
def list_flatten(
    input: ArrowArrayExportable | ArrowStreamExportable,
) -> Array | ArrayReader:
    """Unnest this ListArray, LargeListArray or FixedSizeListArray.

    Args:
        input: Input data.

    Raises:
        Exception if not a list-typed array.

    Returns:
        The flattened Arrow data.
    """

@overload
def list_offsets(input: ArrowArrayExportable, *, logical: bool = True) -> Array: ...
@overload
def list_offsets(
    input: ArrowStreamExportable, *, logical: bool = True
) -> ArrayReader: ...
def list_offsets(
    input: ArrowArrayExportable | ArrowStreamExportable,
    *,
    logical: bool = True,
) -> Array | ArrayReader:
    """Access the offsets of this ListArray or LargeListArray

    Args:
        input: _description_
        physical: If False, return the physical (unsliced) offsets of the provided list array. If True, adjust the list offsets for the current array slicing. Defaults to `True`.

    Raises:
        Exception if not a list-typed array.

    Returns:
        _description_
    """

def struct_field(
    values: ArrowArrayExportable,
    /,
    indices: int | Sequence[int],
) -> Array:
    """Access a column within a StructArray by index

    Args:
        values: Argument to compute function.
        indices: List of indices for chained field lookup, for example [4, 1] will look up the second nested field in the fifth outer field.

    Raises:
        Exception if not a struct-typed array.

    Returns:
        _description_
    """

def fixed_size_list_array(
    values: ArrayInput,
    list_size: int,
    *,
    type: ArrowSchemaExportable | None = None,
) -> Array:
    """Construct a new fixed size list array

    Args:
        values: the values of the new fixed size list array
        list_size: the number of elements in each item of the list.

    Keyword Args:
        type: the type of output array. This must have fixed size list type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred.

    Returns:
        a new Array with fixed size list type
    """

def list_array(
    offsets: ArrayInput,
    values: ArrayInput,
    *,
    type: ArrowSchemaExportable | None = None,
) -> Array:
    """Construct a new list array

    Args:
        offsets: the offsets for the output list array. This array must have type int32 or int64, depending on whether you wish to create a list array or large list array.
        values: the values for the output list array.

    Keyword Args:
        type: the type of output array. This must have list or large list type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred.

    Returns:
        a new Array with list or large list type
    """

def struct_array(
    arrays: Sequence[ArrayInput],
    *,
    fields: Sequence[ArrowSchemaExportable],
    type: ArrowSchemaExportable | None = None,
) -> Array:
    """Construct a new struct array

    Args:
        arrays: a sequence of arrays for the struct children

    Keyword Args:
        fields: a sequence of fields that represent each of the struct children
        type: the type of output array. This must have struct type. You may pass a `Field` into this parameter to associate extension metadata with the created array. Defaults to None, in which case it is inferred .


    Returns:
        a new Array with struct type
    """
