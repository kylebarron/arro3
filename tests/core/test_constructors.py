import numpy as np
import pyarrow as pa
from arro3.core import (
    Array,
    DataType,
    Field,
    fixed_size_list_array,
    list_array,
    list_offsets,
    struct_array,
)


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


def test_fixed_size_list_array_with_mask():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)

    np_mask = np.array([True, False, True], dtype=bool)
    mask = Array.from_numpy(np_mask)

    arro3_array = fixed_size_list_array(flat_array, 2, mask=mask)

    # Note that we don't exactly match the pyarrow array because we still allocate for
    # null values.
    pa_arr = pa.array(
        [[1, 2], [3, 4], [5, 6]],
        type=pa.field(arro3_array.type).type,
        mask=np_mask,
    )

    assert arro3_array[0].is_valid == pa_arr[0].is_valid
    assert arro3_array[1].is_valid == pa_arr[1].is_valid
    assert arro3_array[1] == Array(pa_arr)[1]
    assert arro3_array[2].is_valid == pa_arr[2].is_valid


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


def test_list_array_with_mask():
    np_arr = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
    flat_array = Array.from_numpy(np_arr)
    offsets_array = Array.from_numpy(np.array([0, 2, 5, 6], dtype=np.int32))

    np_mask = np.array([True, False, True], dtype=bool)
    mask = Array.from_numpy(np_mask)

    arro3_array = list_array(offsets_array, flat_array, mask=mask)

    # Note that we don't exactly match the pyarrow array because we still allocate for
    # null values.
    pa_arr = pa.array(
        [[1, 2], [3, 4, 5], [6]], type=pa.field(arro3_array.type).type, mask=np_mask
    )

    assert arro3_array[0].is_valid == pa_arr[0].is_valid
    assert arro3_array[1].is_valid == pa_arr[1].is_valid
    assert arro3_array[1] == Array(pa_arr)[1]
    assert arro3_array[2].is_valid == pa_arr[2].is_valid


def test_struct_array():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])

    arr = struct_array([a, b], fields=[Field("a", a.type), Field("b", b.type)])
    pa_type = pa.array(arr).type
    assert pa.types.is_struct(pa_type)
    assert pa_type.field(0).name == "a"
    assert pa_type.field(1).name == "b"


def test_struct_array_with_mask():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])

    np_mask = np.array([True, False, True, False], dtype=bool)
    mask = Array.from_numpy(np_mask)

    arro3_arr = struct_array(
        [a, b],
        fields=[Field("a", a.type), Field("b", b.type)],
        mask=mask,
    )

    pa_arr = pa.array(
        [
            {"a": 1, "b": "a"},
            {"a": 2, "b": "b"},
            {"a": 3, "b": "c"},
            {"a": 4, "b": "d"},
        ],
        type=pa.field(arro3_arr.type).type,
        mask=np_mask,
    )

    for i in range(len(arro3_arr)):
        assert arro3_arr[i].is_valid == pa_arr[i].is_valid
        assert arro3_arr[i] == Array(pa_arr)[i]
