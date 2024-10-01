import arro3.compute as ac
import numpy as np
import pyarrow as pa
from arro3.core import Array


def test_from_buffer():
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
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


def test_operation_on_buffer():
    np_arr = np.arange(1000, dtype=np.uint64)
    assert np.max(np_arr) == 999
    assert ac.max(np_arr).as_py() == 999

    indices = np.array([2, 3, 4], dtype=np.uint64)
    out = ac.take(np_arr, indices)
    assert pa.array(out) == pa.array(indices)
