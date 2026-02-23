from textwrap import dedent

import pyarrow as pa
import pytest
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


class CustomException(Exception):
    pass


class ArrowCStreamFails:
    def __arrow_c_stream__(self, requested_schema=None):
        raise CustomException


class ArrowCArrayFails:
    def __arrow_c_array__(self, requested_schema=None):
        raise CustomException


def test_chunked_array_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCStreamFails()
    with pytest.raises(CustomException):
        ChunkedArray.from_arrow(c_stream_obj)

    with pytest.raises(CustomException):
        ChunkedArray(c_stream_obj)

    c_array_obj = ArrowCArrayFails()
    with pytest.raises(CustomException):
        ChunkedArray.from_arrow(c_array_obj)

    with pytest.raises(CustomException):
        ChunkedArray(c_array_obj)


def test_pyarrow_equality():
    arr = Array([1, 2, 3], DataType.int16())
    arr2 = Array([4, 5, 6], DataType.int16())
    ca = ChunkedArray([arr, arr2])
    pa_ca = pa.chunked_array([arr, arr2])
    assert ca == pa_ca
    assert pa_ca == ca
