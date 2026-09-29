from datetime import datetime, timedelta

import arro3.compute as ac
import numpy as np
import pyarrow as pa
import pytest
from arro3.core import Array, Buffer, fixed_size_list_array


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

    # Datetime array
    # https://stackoverflow.com/a/34325416
    arr = np.arange(datetime(1985, 7, 1), datetime(2015, 7, 1), timedelta(days=1))
    with pytest.raises(ValueError):
        Array.from_buffer(arr)


def test_operation_on_buffer():
    np_arr = np.arange(1000, dtype=np.uint64)
    assert np.max(np_arr) == 999
    assert ac.max(np_arr).as_py() == 999

    indices = np.array([2, 3, 4], dtype=np.uint64)
    out = ac.take(np_arr, indices)
    assert pa.array(out) == pa.array(indices)


def test_multi_dimensional():
    np_arr = np.arange(6, dtype=np.uint8).reshape((2, 3))
    arro3_arr = Array(np_arr)
    pa_arr = pa.array(arro3_arr)
    assert pa_arr.type.list_size == 3
    assert pa_arr.type.value_type == pa.uint8()

    np_arr = np.arange(12, dtype=np.uint8).reshape((1, 2, 3, 2))
    arro3_arr = Array(np_arr)
    pa_arr = pa.array(arro3_arr)
    assert pa_arr.type.list_size == 2
    assert pa_arr.type.value_type.list_size == 3
    assert pa_arr.type.value_type.value_type.list_size == 2
    assert pa_arr.type.value_type.value_type.value_type == pa.uint8()


@pytest.mark.parametrize("constructor", [Array.from_buffer, Array.from_numpy])
@pytest.mark.parametrize("dtype", [np.float64, np.uint8])
@pytest.mark.parametrize("shape", [(0,), (0, 2), (0, 2, 3)])
def test_zero_length_first_dimension(constructor, dtype, shape):
    # Should have the same type as a non-empty array with the same trailing dimensions
    expected_type = constructor(np.zeros((1, *shape[1:]), dtype=dtype)).type

    arr = constructor(np.zeros(shape, dtype=dtype))

    assert len(arr) == 0
    assert arr.type == expected_type


def test_zero_length_trailing_dimension_raises():
    with pytest.raises(ValueError, match="0-length"):
        Array.from_buffer(np.zeros((2, 0)))


def test_fixed_size_list_array_from_zero_length_numpy():
    expected_type = fixed_size_list_array(np.zeros(2), 2).type

    arr = fixed_size_list_array(np.zeros(0), 2)

    assert len(arr) == 0
    assert arr.type == expected_type


def test_round_trip_buffer():
    arr = np.arange(5, dtype=np.uint8)
    buffer = Buffer(arr)
    # Restore when upgrading to pyo3-arrow 0.6
    # assert len(buffer) == arr.nbytes
    retour = np.frombuffer(buffer, dtype=np.uint8)
    assert np.array_equal(arr, retour)

    assert np.array_equal(arr, Array(buffer).to_numpy())
