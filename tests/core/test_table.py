import pyarrow as pa
from arro3.core import Table


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
