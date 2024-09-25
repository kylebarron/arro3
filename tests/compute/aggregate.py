import arro3.compute as ac
from arro3.core import Array, ChunkedArray, DataType


def test_min():
    arr1 = Array([1, 2, 3], DataType.int16())
    assert ac.min(arr1).as_py() == 1

    arr2 = Array([3, 2, 0], DataType.int16())
    assert ac.min(arr2).as_py() == 0

    ca = ChunkedArray([arr1, arr2])
    assert ac.min(ca).as_py() == 0

    arr = Array(["c", "a", "b"], DataType.string())
    assert ac.min(arr).as_py() == "a"
