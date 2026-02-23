import pyarrow as pa
import pytest
from arro3.core import Array, DataType, Field


def test_value_type_fixed_size_list_type():
    value_type = DataType.int8()
    list_dt = DataType.list(Field("inner", value_type), 2)
    assert list_dt.value_type == value_type


def test_value_field_list_type():
    value_type = DataType.int8()
    value_field = Field("inner", value_type, nullable=True)
    list_dt = DataType.list(
        value_field,
        2,
    )
    assert list_dt.value_field == value_field


def test_fields_struct_type():
    field_foo = Field("foo", DataType.int8(), nullable=True)
    field_bar = Field("bar", DataType.string(), nullable=False)
    struct_type = DataType.struct([field_foo, field_bar])
    assert struct_type.fields == [field_foo, field_bar]


def test_list_data_type_construction_with_dt():
    DataType.list(DataType.int16())


def test_hashable():
    # We should be able to use DataType as a key in a dict
    _dtype_map = {
        DataType.uint8(): DataType.int8(),
        DataType.uint16(): DataType.int16(),
        DataType.uint32(): DataType.int32(),
        DataType.uint64(): DataType.int64(),
    }


class CustomException(Exception):
    pass


class ArrowCSchemaFails:
    def __arrow_c_schema__(self):
        raise CustomException


def test_schema_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCSchemaFails()
    with pytest.raises(CustomException):
        DataType.from_arrow(c_stream_obj)


def test_pyarrow_equality():
    assert DataType.int64() == pa.int64()
    assert pa.int64() == DataType.int64()


def test_data_type_exports_as_nullable():
    # Test that data type exports through `__arrow_c_schema__` as nullable
    # https://github.com/kylebarron/arro3/pull/483
    assert Array([1, 2, 3], DataType.int64()).field.nullable == True
