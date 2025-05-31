import numpy as np
from arro3.core import Array, DataType


def test_from_numpy():
    arr = np.array([1, 2, 3, 4], dtype=np.uint8)
    assert Array.from_numpy(arr).type == DataType.uint8()

    arr = np.array([1, 2, 3, 4], dtype=np.float64)
    assert Array.from_numpy(arr).type == DataType.float64()

    # arr = np.array([b"1", b"2", b"3"], np.object_)
    # Array.from_numpy(arr)


def test_binary_to_numpy():
    bytes_list = [b"1", b"2", b"3"]
    expected = np.array(bytes_list, dtype=np.object_)

    arr = Array(bytes_list, DataType.binary())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(bytes_list, DataType.large_binary())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(bytes_list, DataType.binary_view())
    assert np.array_equal(arr.to_numpy(), expected)


def test_string_to_numpy():
    string_list = ["1", "2", "3"]
    expected = np.array(string_list, dtype=np.object_)

    arr = Array(string_list, DataType.string())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(string_list, DataType.large_string())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(string_list, DataType.string_view())
    assert np.array_equal(arr.to_numpy(), expected)
