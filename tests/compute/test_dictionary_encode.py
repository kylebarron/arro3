import pyarrow as pa
import pyarrow.compute as pc
from arro3.compute import dictionary_encode


def test_dictionary_encode():
    arr = pa.array([1, 2, 3, 1, 2, 2, 3, 1, 1, 1], type=pa.uint16())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore
    assert pa.array(out) == out_pc

    arr = pa.array(["1", "2", "3", "1", "2", "2", "3", "1", "1", "1"], type=pa.utf8())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore
    assert pa.array(out) == out_pc

    arr = arr.cast(pa.large_utf8())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore
    assert pa.array(out) == out_pc

    arr = arr.cast(pa.binary())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore
    assert pa.array(out) == out_pc

    arr = arr.cast(pa.large_binary())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore
    assert pa.array(out) == out_pc
