import pyarrow as pa
from arro3.core import Field, Table


def test_schema_iterable():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})
    schema = table.schema
    for field in schema:
        assert isinstance(field, Field)
        assert field.name in ["a", "b"]
