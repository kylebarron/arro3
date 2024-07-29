import pyarrow as pa
from arro3.compute import struct_field


def test_struct_field():
    a = pa.array([1, 2, 3])
    b = pa.array([3, 4, 5])
    struct_arr = pa.StructArray.from_arrays([a, b], names=["a", "b"])
    assert pa.array(struct_field(struct_arr, [0])) == a


def test_struct_field_sliced_end():
    a = pa.array([1, 2, 3])
    b = pa.array([3, 4, 5])
    struct_arr = pa.StructArray.from_arrays([a, b], names=["a", "b"])
    sliced = struct_arr.slice(1, 2)
    out = pa.array(struct_field(sliced, [0]))
    assert out == sliced.field(0)


def test_struct_field_sliced_start():
    a = pa.array([1, 2, 3])
    b = pa.array([3, 4, 5])
    struct_arr = pa.StructArray.from_arrays([a, b], names=["a", "b"])
    sliced = struct_arr.slice(0, 1)
    out = pa.array(struct_field(sliced, [0]))
    assert out == sliced.field(0)
