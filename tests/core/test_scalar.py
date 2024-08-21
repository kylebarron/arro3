from __future__ import annotations

import itertools
from datetime import date, datetime
from time import sleep

import pyarrow as pa
import pytest
from arro3.core import (
    Array,
    DataType,
    Field,
    fixed_size_list_array,
    list_array,
    struct_array,
)


def test_as_py():
    int_arr = Array([1, 2, 3, 4], DataType.int16())
    assert int_arr[0].as_py() == 1
    assert int_arr[3].as_py() == 4

    str_arr = Array(["1", "2", "3", "4"], DataType.string())
    assert str_arr[0].as_py() == "1"
    assert str_arr[3].as_py() == "4"

    bytes_arr = Array([b"1", b"2", b"3", b"4"], DataType.binary())
    assert bytes_arr[0].as_py() == b"1"
    assert bytes_arr[3].as_py() == b"4"

    struct_arr = struct_array(
        [int_arr, str_arr, bytes_arr],
        fields=[
            Field("int_arr", int_arr.type),
            Field("str_arr", str_arr.type),
            Field("bytes_arr", bytes_arr.type),
        ],
    )
    assert struct_arr[0].as_py() == {"int_arr": 1, "str_arr": "1", "bytes_arr": b"1"}
    assert struct_arr[3].as_py() == {"int_arr": 4, "str_arr": "4", "bytes_arr": b"4"}

    list_arr = list_array(Array([0, 2, 4], DataType.int32()), int_arr)
    assert list_arr[0].as_py() == [1, 2]
    assert list_arr[1].as_py() == [3, 4]

    fixed_list_arr = fixed_size_list_array(int_arr, 2)
    assert fixed_list_arr[0].as_py() == [1, 2]
    assert fixed_list_arr[1].as_py() == [3, 4]


time_units = ["s", "ms", "us", "ns"]
time_zones = [None, "UTC", "America/New_York"]


@pytest.mark.parametrize(
    "time_unit,time_zone", list(itertools.product(time_units, time_zones))
)
def test_as_py_datetime(time_unit: str, time_zone: str | None):
    now = datetime.now()

    pa_arr = pa.array([now], type=pa.timestamp(time_unit, None))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()


def test_as_py_date():
    today = date.today()

    pa_arr = pa.array([today], type=pa.date32())
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([today], type=pa.date64())
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()


def test_as_py_time():
    now = datetime.now().time()

    pa_arr = pa.array([now], type=pa.time32("s"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([now], type=pa.time32("ms"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([now], type=pa.time64("us"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([now], type=pa.time64("ns"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()


def test_as_py_duration():
    now = datetime.now()
    sleep(0.001)
    later = datetime.now()
    delta = later - now

    pa_arr = pa.array([delta], type=pa.duration("s"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([delta], type=pa.duration("ms"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([delta], type=pa.duration("us"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()

    pa_arr = pa.array([delta], type=pa.duration("ns"))
    arro3_arr = Array(pa_arr)
    assert arro3_arr[0].as_py() == pa_arr[0].as_py()


def test_as_py_dictionary():
    pa_arr = pa.array([0, 0, 1, 1, 2, 1, 0]).dictionary_encode()
    arro3_arr = Array(pa_arr)
    for i in range(len(pa_arr)):
        assert arro3_arr[i].as_py() == pa_arr[i].as_py()


def test_map_array():
    # This comes from the MapArray docstring
    # https://arrow.apache.org/docs/python/generated/pyarrow.MapArray.html#pyarrow.MapArray.from_arrays
    offsets = [
        0,  #  -- row 1 start
        1,  #  -- row 2 start
        4,  #  -- row 3 start
        6,  #  -- row 4 start
        6,  #  -- row 5 start
        6,  #  -- row 5 end
    ]
    movies = [
        "Dark Knight",  #  ---------------------------------- row 1
        "Dark Knight",
        "Meet the Parents",
        "Superman",  #  -- row 2
        "Meet the Parents",
        "Superman",  #  ----------------- row 3
    ]
    likings = [
        10,  #  -------- row 1
        8,
        4,
        5,  #  --- row 2
        10,
        3,  #  ------ row 3
    ]
    pa_arr = pa.MapArray.from_arrays(offsets, movies, likings)
    arro3_arr = Array(pa_arr)
    for i in range(len(pa_arr)):
        assert pa_arr[i].as_py() == arro3_arr[i].as_py()
