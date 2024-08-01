import geoarrow.types as gt
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from arro3.core import ChunkedArray, Table


def test_table_getitem():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})
    assert a == pa.chunked_array(table["a"])
    assert b == pa.chunked_array(table["b"])
    assert a == pa.chunked_array(table[0])
    assert b == pa.chunked_array(table[1])


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


@pytest.mark.xfail("from_batches fails on empty column with positive length")
def test_table_from_batches_empty_columns_with_len():
    df = pd.DataFrame({"a": [1, 2, 3]})
    no_columns = df[[]]
    pa_table = pa.Table.from_pandas(no_columns)
    _table = Table.from_batches(pa_table.to_batches())
