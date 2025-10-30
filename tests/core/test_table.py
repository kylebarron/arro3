import geoarrow.types as gt
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from arro3.core import Array, ArrayReader, ChunkedArray, DataType, Field, Table


def test_table_getitem():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    assert a == pa.chunked_array(table["a"])
    assert b == pa.chunked_array(table["b"])
    assert a == pa.chunked_array(table[0])
    assert b == pa.chunked_array(table[1])

    with pytest.raises(KeyError):
        table["foo"]

    with pytest.raises(IndexError):
        table[10]


def test_table_from_arrays():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])
    arro3_table = Table.from_arrays([a, b], names=["a", "b"])
    pa_table = pa.Table.from_arrays([a, b], names=["a", "b"])
    assert pa.table(arro3_table) == pa_table


def test_table_from_pydict():
    mapping = {"a": pa.array([1, 2, 3, 4]), "b": pa.array(["a", "b", "c", "d"])}
    arro3_table = Table.from_pydict(mapping)
    pa_table = pa.Table.from_pydict(mapping)
    assert pa.table(arro3_table) == pa_table


def test_table_constructor_ext_array():
    typ = DataType.uint8()
    metadata = {"ARROW:extension:name": "ext_name"}
    field = Field("", type=typ, nullable=True, metadata=metadata)
    arr = Array([1, 2, 3, 4], field)
    t = Table({"a": arr})
    assert t.schema.field("a").metadata_str["ARROW:extension:name"] == "ext_name"

    ca = ChunkedArray([arr], field)
    t = Table({"a": ca})
    assert t.schema.field("a").metadata_str["ARROW:extension:name"] == "ext_name"


def test_table_append_array_extension_type():
    """
    Test that extension metadata gets propagated from an array to a column on a table.
    """
    # Test that extension
    extension_type = gt.point(dimensions="xy", coord_type="interleaved").to_pyarrow()
    coords = np.array([1, 2, 3, 4], dtype=np.float64)
    ext_array = pa.FixedSizeListArray.from_arrays(coords, 2).cast(extension_type)

    table = Table.from_arrays([pa.array(["a", "b"])], names=["a"])
    geo_table = table.append_column("geometry", ChunkedArray([ext_array]))

    meta = geo_table.schema["geometry"].metadata
    assert b"ARROW:extension:name" in meta.keys()
    assert meta[b"ARROW:extension:name"] == b"geoarrow.point"

def test_table_append_column():
    """
    Test that Table.append_column appends columns of different types.
    """
    table = Table.from_arrays([pa.array(["a", "b"])], names=["c0"])

    # PyArray
    c_name, c_value = 'c1', [1,2]
    table = table.append_column(c_name, Array(pa.array(c_value)))
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    # PyChunkedArray
    c_name, c_value = 'c2', [3, 4]
    table = table.append_column(c_name, ChunkedArray(pa.array(c_value)))
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    # PyArrayReader
    c_name, c_value = 'c3', [5, 6]
    reader = ArrayReader.from_arrays(pa.field("_", pa.int64()), arrays=[pa.array(c_value)])
    table = table.append_column(c_name, reader)
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    assert len(table.columns) == 4


def test_table_from_batches_empty_columns_with_len():
    df = pd.DataFrame({"a": [1, 2, 3]})
    no_columns = df[[]]
    pa_table = pa.Table.from_pandas(no_columns)
    table = Table.from_batches(pa_table.to_batches())
    assert table.num_columns == 0
    assert table.num_rows == 3


def test_rechunk():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    rechunked1 = table.rechunk(max_chunksize=1)
    assert rechunked1.chunk_lengths == [1, 1, 1, 1]

    rechunked2 = rechunked1.rechunk(max_chunksize=2)
    assert rechunked2.chunk_lengths == [2, 2]
    assert rechunked2.rechunk().chunk_lengths == [4]


def test_slice():
    a = pa.chunked_array([[1, 2], [3, 4]])
    b = pa.chunked_array([["a", "b"], ["c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    sliced1 = table.slice(0, 1)
    assert sliced1.num_rows == 1
    assert sliced1.chunk_lengths == [1]

    sliced2 = table.slice(1, 2)
    assert sliced2.num_rows == 2
    assert sliced2.chunk_lengths == [1, 1]


def test_nonempty_table_no_columns():
    table = pa.table({"a": [1, 2, 3, 4]}).select([])
    assert len(table) == 4
    assert table.num_columns == 0
    arro3_table = Table.from_arrow(table)
    retour = pa.table(arro3_table)
    assert table == retour


class CustomException(Exception):
    pass


class ArrowCStreamFails:
    def __arrow_c_stream__(self, requested_schema=None):
        raise CustomException


def test_table_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCStreamFails()
    with pytest.raises(CustomException):
        Table.from_arrow(c_stream_obj)

    with pytest.raises(CustomException):
        Table(c_stream_obj)
