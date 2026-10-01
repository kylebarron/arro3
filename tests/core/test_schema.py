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


class ArrowCSchemaExporter:
    """An object exporting ``__arrow_c_schema__`` that is not itself iterable."""

    def __init__(self, schema: pa.Schema):
        self._schema = schema

    def __arrow_c_schema__(self):
        return self._schema.__arrow_c_schema__()


def test_schema_init_from_arrow_c_schema():
    """https://github.com/kylebarron/arro3/issues/435"""
    pa_schema = pa.schema([pa.field("a", pa.int32())], metadata={"k": "v"})
    schema = Schema(ArrowCSchemaExporter(pa_schema))
    assert schema == pa_schema
    assert schema.metadata == {b"k": b"v"}


def test_schema_init_from_pyarrow_schema_preserves_metadata():
    pa_schema = pa.schema([pa.field("a", pa.int32())], metadata={"k": "v"})
    schema = Schema(pa_schema)
    assert schema.metadata == {b"k": b"v"}


def test_schema_init_from_arrow_c_schema_replaces_metadata():
    pa_schema = pa.schema([pa.field("a", pa.int32())], metadata={"k": "v"})
    schema = Schema(ArrowCSchemaExporter(pa_schema), metadata={"k2": "v2"})
    expected = pa.schema(ArrowCSchemaExporter(pa_schema), metadata={"k2": "v2"})
    assert schema.metadata == expected.metadata
