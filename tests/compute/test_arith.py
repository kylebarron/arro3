import arro3.compute as ac
import pyarrow as pa
from arro3.core import Array, DataType


def test_add():
    arr1 = Array([1, 2, 3], DataType.int16())
    assert ac.min(arr1).as_py() == 1

    arr2 = Array([3, 2, 0], DataType.int16())
    assert ac.min(arr2).as_py() == 0

    add1 = ac.add(arr1, arr2)
    assert pa.array(add1) == pa.array(Array([4, 4, 3], DataType.int16()))

    s = arr1[0]
    add2 = ac.add(arr1, s)
    assert pa.array(add2) == pa.array(Array([2, 3, 4], DataType.int16()))
