import numpy as np
import pyarrow as pa
from arro3.core import (
    Array,
    DataType,
    fixed_size_list_array,
    Field,
    list_array,
    struct_array,
)
from arro3.core import list_offsets


def test_fixed_size_list_array():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)
    array = fixed_size_list_array(flat_array, 2)
    pa_array = pa.array(array)
    assert pa.types.is_fixed_size_list(pa_array.type)
    assert pa_array.type.list_size == 2


def test_fixed_size_list_array_with_type():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)
    list_type = DataType.list(Field("inner", DataType.float64()), 2)
    array = fixed_size_list_array(flat_array, 2, type=list_type)
    pa_array = pa.array(array)
    assert pa.types.is_fixed_size_list(pa_array.type)
    assert pa_array.type.list_size == 2
    assert pa_array.type.field(0).name == "inner"


def test_list_array():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)
    offsets_array = Array.from_numpy(np.array([0, 2, 5, 6], dtype=np.int32))
    array = list_array(offsets_array, flat_array)
    pa_array = pa.array(array)
    assert pa.types.is_list(pa_array.type)
    assert list_offsets(array) == offsets_array


def test_list_array_with_type():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)
    offsets_array = Array.from_numpy(np.array([0, 2, 5, 6], dtype=np.int32))

    list_type = DataType.list(Field("inner", DataType.float64()))
    array = list_array(offsets_array, flat_array, type=list_type)
    pa_array = pa.array(array)
    assert pa.types.is_list(pa_array.type)
    assert list_offsets(array) == offsets_array
    assert pa_array.type.field(0).name == "inner"


def test_struct_array():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])

    arr = struct_array([a, b], fields=[Field("a", a.type), Field("b", b.type)])
    pa_type = pa.array(arr).type
    assert pa.types.is_struct(pa_type)
    assert pa_type.field(0).name == "a"
    assert pa_type.field(1).name == "b"
