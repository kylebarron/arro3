import pyarrow as pa
import pytest
from arro3.core import DataType, Field, Schema, Table


def test_schema_iterable():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})
    schema = table.schema
    for field in schema:
        assert isinstance(field, Field)
        assert field.name in ["a", "b"]


class CustomException(Exception):
    pass


class ArrowCSchemaFails:
    def __arrow_c_schema__(self):
        raise CustomException


def test_schema_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCSchemaFails()
    with pytest.raises(CustomException):
        Schema.from_arrow(c_stream_obj)


def test_pyarrow_equality():
    schema = Schema([Field("a", DataType.int64()), Field("b", DataType.string())])
    pa_schema = pa.schema(schema)
    assert schema == pa_schema
    assert pa_schema == schema
