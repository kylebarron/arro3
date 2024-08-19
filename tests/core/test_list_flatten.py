import pyarrow as pa
from arro3.core import list_flatten


def test_list_flatten():
    list_arr = pa.array([[1, 2], [3, 4]])
    out = pa.array(list_flatten(list_arr))
    assert out == pa.array([1, 2, 3, 4])


def test_list_flatten_sliced_end():
    list_arr = pa.array([[1, 2], [3, 4]])
    sliced = list_arr.slice(1, 2)
    out = pa.array(list_flatten(sliced))
    assert out == pa.array([3, 4])


def test_list_flatten_sliced_start():
    list_arr = pa.array([[1, 2], [3, 4]])
    sliced = list_arr.slice(0, 1)
    out = pa.array(list_flatten(sliced))
    assert out == pa.array([1, 2])
