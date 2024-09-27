from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
from arro3.compute import dictionary_encode
from arro3.core import ChunkedArray, dictionary_dictionary, dictionary_indices


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

    now = datetime.now()
    later = datetime.now()
    arr = pa.array([now, later, now, now, later])
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


def test_dictionary_access():
    arr = pa.array([1, 2, 3, 1, 2, 2, 3, 1, 1, 1], type=pa.uint16())
    out = dictionary_encode(arr)
    out_pc = pc.dictionary_encode(arr)  # type: ignore

    keys = dictionary_dictionary(out)
    assert pa.array(keys) == out_pc.dictionary

    indices = dictionary_indices(out)
    assert pa.array(indices) == out_pc.indices


def test_dictionary_access_chunked():
    arr = pa.chunked_array([[3, 2, 3], [1, 2, 2], [3, 1, 1, 1]], type=pa.uint16())
    out = ChunkedArray(dictionary_encode(arr))
    out_pa = pa.chunked_array(out)

    dictionary = ChunkedArray(dictionary_dictionary(out))
    assert pa.chunked_array(dictionary).chunks[0] == out_pa.chunks[0].dictionary

    indices = ChunkedArray(dictionary_indices(out))
    assert pa.chunked_array(indices).chunks[0] == out_pa.chunks[0].indices
