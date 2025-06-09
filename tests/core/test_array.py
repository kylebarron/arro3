from datetime import date, datetime
from textwrap import dedent

import pyarrow as pa
import pytest
from arro3.core import Array, DataType, Table


def test_constructor():
    arr = Array([1, 2, 3], DataType.int16())
    assert pa.array(arr) == pa.array([1, 2, 3], pa.int16())

    arr = Array((1, 2, 3), DataType.int16())
    assert pa.array(arr) == pa.array([1, 2, 3], pa.int16())

    arr = Array([1, 2, 3], DataType.float64())
    assert pa.array(arr) == pa.array([1, 2, 3], pa.float64())

    arr = Array(["1", "2", "3"], DataType.string())
    assert pa.array(arr) == pa.array(["1", "2", "3"], pa.string())

    arr = Array([b"1", b"2", b"3"], DataType.binary())
    assert pa.array(arr) == pa.array([b"1", b"2", b"3"], pa.binary())

    arr = Array([b"1", b"2", b"3"], DataType.binary(1))
    assert pa.array(arr) == pa.array([b"1", b"2", b"3"], pa.binary(1))


def test_constructor_null():
    arr = Array([1, None, 3], DataType.int16())
    assert pa.array(arr) == pa.array([1, None, 3], pa.int16())

    arr = Array((1, None, 3), DataType.int16())
    assert pa.array(arr) == pa.array([1, None, 3], pa.int16())

    arr = Array([1, None, 3], DataType.float64())
    assert pa.array(arr) == pa.array([1, None, 3], pa.float64())

    arr = Array(["1", None, "3"], DataType.string())
    assert pa.array(arr) == pa.array(["1", None, "3"], pa.string())

    arr = Array([b"1", None, b"3"], DataType.binary())
    assert pa.array(arr) == pa.array([b"1", None, b"3"], pa.binary())

    arr = Array([b"1", None, b"3"], DataType.binary(1))
    assert pa.array(arr) == pa.array([b"1", None, b"3"], pa.binary(1))


def test_extension_array_meta_persists():
    arr = pa.array([1, 2, 3])
    input_metadata = {"hello": "world"}
    field = pa.field("arr", type=arr.type, metadata=input_metadata)
    pa_table = pa.Table.from_arrays([arr], schema=pa.schema([field]))
    table = Table.from_arrow(pa_table)
    assert table[0].chunks[0].field.metadata_str == input_metadata


def test_getitem():
    arr = Array([1, 2, 3], DataType.int16())
    assert arr[0].as_py() == 1
    assert arr[-1].as_py() == 3


def test_string_view():
    arr = pa.array(
        ["foo", "bar", "baz", "foooooobarrrrrrbazzzzzzzz"], type=pa.string_view()
    )
    assert Array(arr)[0].as_py() == "foo"
    assert Array(arr)[1].as_py() == "bar"
    assert Array(arr)[2].as_py() == "baz"
    assert Array(arr)[3].as_py() == "foooooobarrrrrrbazzzzzzzz"
    assert pa.array(Array(arr)) == arr


def test_repr():
    arr = Array([1, 2, 3], DataType.int16())
    expected = """\
        arro3.core.Array<Int16>
        [
          1,
          2,
          3,
        ]
        """
    assert repr(arr) == dedent(expected)

    arr = Array([1.0, 2.0, 3.0], DataType.float64())
    expected = """\
        arro3.core.Array<Float64>
        [
          1.0,
          2.0,
          3.0,
        ]
        """
    assert repr(arr) == dedent(expected)

    arr = Array(["foo", "bar", "baz"], DataType.string())
    expected = """\
        arro3.core.Array<Utf8>
        [
          foo,
          bar,
          baz,
        ]
        """
    assert repr(arr) == dedent(expected)

    arr = Array([b"foo", b"bar", b"baz"], DataType.binary())
    expected = """\
        arro3.core.Array<Binary>
        [
          666f6f,
          626172,
          62617a,
        ]
        """
    assert repr(arr) == dedent(expected)

    arr = pa.array(
        [datetime(2020, 1, 1), datetime(2020, 1, 2)],
        type=pa.timestamp("us", tz="UTC"),
    )
    arr2 = Array.from_arrow(arr)
    expected = """\
        arro3.core.Array<Timestamp(Microsecond, Some("UTC"))>
        [
          2020-01-01T00:00:00Z,
          2020-01-02T00:00:00Z,
        ]
        """
    assert repr(arr2) == dedent(expected)

    arr = pa.array(
        [date(2020, 1, 1), date(2020, 1, 2)],
        type=pa.date32(),
    )
    arr2 = Array.from_arrow(arr)
    expected = """\
        arro3.core.Array<Date32>
        [
          2020-01-01,
          2020-01-02,
        ]
        """
    assert repr(arr2) == dedent(expected)


def test_fixed_size_binary():
    values = [b"foo", None, b"bar", None, b"baz"]
    arr = Array(values, DataType.binary(3))
    assert arr.type == DataType.binary(3)
    assert arr[0].as_py() == b"foo"
    assert arr[1].as_py() is None
    assert arr[2].as_py() == b"bar"


def test_fixed_size_binary_invalid_length():
    values = [b"foo", None, b"barbaz"]
    with pytest.raises(Exception):
        Array(values, DataType.binary(3))
