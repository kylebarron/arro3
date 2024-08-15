import pyarrow as pa
import pyarrow.compute as pc
from arro3.core import ChunkedArray
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


def test_dictionary_encode_chunked():
    arr = pa.chunked_array([[3, 2, 3], [1, 2, 2], [3, 1, 1, 1]], type=pa.uint16())
    out = ChunkedArray(dictionary_encode(arr))

    out_retour = pa.chunked_array(out)
    out_pc = pc.dictionary_encode(arr)  # type: ignore

    # Since these arrays have different dictionaries, array and arrow scalar comparison
    # will fail.
    assert len(out_retour) == len(out_pc)
    for i in range(len(out_retour)):
        assert out_retour[i].as_py() == out_pc[i].as_py()
