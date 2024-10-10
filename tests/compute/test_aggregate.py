from datetime import datetime, timezone

import arro3.compute as ac
import pyarrow as pa
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


def test_max():
    arr1 = Array([1, 2, 3], DataType.int16())
    assert ac.max(arr1).as_py() == 3

    arr2 = Array([4, 2, 0], DataType.int16())
    assert ac.max(arr2).as_py() == 4

    ca = ChunkedArray([arr1, arr2])
    assert ac.max(ca).as_py() == 4

    arr = Array(["c", "a", "b"], DataType.string())
    assert ac.max(arr).as_py() == "c"


def test_sum():
    arr1 = Array([1, 2, 3], DataType.int16())
    assert ac.sum(arr1).as_py() == 6

    arr2 = Array([4, 2, 0], DataType.int16())
    assert ac.sum(arr2).as_py() == 6

    ca = ChunkedArray([arr1, arr2])
    assert ac.sum(ca).as_py() == 12


def test_min_max_datetime():
    dt1 = datetime.now()
    dt2 = datetime.now()
    dt3 = datetime.now()

    pa_arr = pa.array([dt1, dt2, dt3], type=pa.timestamp("ns", None))
    arro3_arr = Array(pa_arr)
    assert ac.min(arro3_arr).as_py() == dt1
    assert ac.max(arro3_arr).as_py() == dt3


def test_min_max_datetime_with_timezone():
    dt1 = datetime.now(timezone.utc)
    dt2 = datetime.now(timezone.utc)
    dt3 = datetime.now(timezone.utc)
    arr = pa.array([dt1, dt2, dt3])
    assert arr.type.tz == "UTC"

    assert ac.min(arr).as_py() == dt1
    assert ac.min(arr).type.tz == "UTC"
    assert ac.max(arr).as_py() == dt3
    assert ac.max(arr).type.tz == "UTC"
