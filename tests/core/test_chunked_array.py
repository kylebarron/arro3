from textwrap import dedent

import pyarrow as pa
from arro3.core import Array, ChunkedArray, DataType


def test_constructor():
    arr = Array([1, 2, 3], DataType.int16())
    arr2 = Array([4, 5, 6], DataType.int16())
    ca = ChunkedArray([arr, arr2])
    assert pa.chunked_array(ca) == pa.chunked_array([arr, arr2])


def test_repr():
    arr = Array([1, 2, 3], DataType.int16())
    arr2 = Array([4, 5, 6], DataType.int16())
    ca = ChunkedArray([arr, arr2])
    expected = """\
        arro3.core.ChunkedArray<Int16>
        [
          [
            1,
            2,
            3,
          ]
          [
            4,
            5,
            6,
          ]
        ]
        """
    assert repr(ca) == dedent(expected)

    arr = Array([1.0, 2.0, 3.0], DataType.float64())
    arr2 = Array([4.0, 5.0, 6.0], DataType.float64())
    ca = ChunkedArray([arr, arr2])
    expected = """\
        arro3.core.ChunkedArray<Float64>
        [
          [
            1.0,
            2.0,
            3.0,
          ]
          [
            4.0,
            5.0,
            6.0,
          ]
        ]
        """
    assert repr(ca) == dedent(expected)

    arr = Array(["foo"], DataType.string())
    arr2 = Array(["bar"], DataType.string())
    arr3 = Array(["baz"], DataType.string())
    ca = ChunkedArray([arr, arr2, arr3])
    expected = """\
        arro3.core.ChunkedArray<Utf8>
        [
          [
            foo,
          ]
          [
            bar,
          ]
          [
            baz,
          ]
        ]
        """
    assert repr(ca) == dedent(expected)
