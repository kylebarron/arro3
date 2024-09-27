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


def test_from_buffer():
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    mv = memoryview(arr)
    assert pa.array(mv) == pa.array(Array.from_buffer(mv))

    arr = np.array([True, False, True], dtype=np.bool_)
    mv = memoryview(arr)
    assert pa.array(mv) == pa.array(Array.from_buffer(mv))

    arr = np.array([1, 2, 3], dtype=np.int64)
    mv = memoryview(arr)
    assert pa.array(mv) == pa.array(Array.from_buffer(mv))

    # pyarrow applies some casting; this is weird
    # According to joris, this may be because pyarrow doesn't implement direct import of
    # buffer protocol objects, and instead infers from `pa.array(list(memoryview()))`
    # float32 -> float64
    # int32 -> int64
    # uint64 -> int64

    arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    assert pa.array(Array.from_buffer(memoryview(arr))).type == pa.float32()

    arr = np.array([1, 2, 3], dtype=np.int32)
    assert pa.array(Array.from_buffer(memoryview(arr))).type == pa.int32()

    arr = np.array([1, 2, 3], dtype=np.int64)
    assert pa.array(Array.from_buffer(memoryview(arr))).type == pa.int64()

    arr = np.array([1, 2, 3], dtype=np.uint64)
    assert pa.array(Array.from_buffer(memoryview(arr))).type == pa.uint64()


def test_getitem():
    arr = Array([1, 2, 3], DataType.int16())
    assert arr[0].as_py() == 1
    assert arr[-1].as_py() == 3
