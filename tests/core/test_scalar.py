from arro3.core import (
    Array,
    Field,
    DataType,
    struct_array,
    list_array,
    fixed_size_list_array,
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
