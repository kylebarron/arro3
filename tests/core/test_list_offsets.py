import pyarrow as pa
from arro3.core import list_offsets


def test_list_flatten():
    list_arr = pa.array([[1, 2], [3, 4]])
    out = pa.array(list_offsets(list_arr))
    assert out == list_arr.offsets


def test_list_flatten_sliced_end():
    list_arr = pa.array([[1, 2], [3, 4]])
    sliced = list_arr.slice(1, 1)

    out = pa.array(list_offsets(sliced, logical=False))
    assert out == pa.array([2, 4], type=pa.int32())

    out = pa.array(list_offsets(sliced, logical=True))
    assert out == pa.array([0, 2], type=pa.int32())


def test_list_flatten_sliced_start():
    list_arr = pa.array([[1, 2], [3, 4]])
    sliced = list_arr.slice(0, 1)

    out = pa.array(list_offsets(sliced, logical=False))
    assert out == pa.array([0, 2], type=pa.int32())

    out = pa.array(list_offsets(sliced, logical=True))
    assert out == pa.array([0, 2], type=pa.int32())
