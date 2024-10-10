import numpy as np
import pyarrow as pa
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

    # arr = Array([b"1", b"2", b"3"], DataType.binary(1))
    # assert pa.array(arr) == pa.array([b"1", b"2", b"3"], pa.binary(1))


def test_from_numpy():
    arr = np.array([1, 2, 3, 4], dtype=np.uint8)
    assert Array.from_numpy(arr).type == DataType.uint8()

    arr = np.array([1, 2, 3, 4], dtype=np.float64)
    assert Array.from_numpy(arr).type == DataType.float64()

    # arr = np.array([b"1", b"2", b"3"], np.object_)
    # Array.from_numpy(arr)


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
