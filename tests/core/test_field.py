import pyarrow as pa
from arro3.core import DataType, Field


def test_pyarrow_equality():
    field = Field("a", DataType.int64())
    pa_field = pa.field(field)
    assert field == pa_field
    assert pa_field == field


def test_pyarrow_equality_nullability():
    field = Field("a", DataType.int64(), nullable=True)
    pa_field = pa.field(field)
    assert field == pa_field
    assert pa_field == field

    field2 = Field("a", DataType.int64(), nullable=False)
    pa_field2 = pa.field(field2)
    assert field2 == pa_field2
    assert pa_field2 == field2

    assert field != field2
    assert pa_field != pa_field2
    assert field != pa_field2
    assert pa_field != field2


def test_pyarrow_equality_metadata():
    metadata = {"key": "value"}
    field = Field("a", DataType.int64(), metadata=metadata)
    pa_field = pa.field(field)
    assert field == pa_field
    assert pa_field == field
