import pytest
from arro3.core import DataType, Field


def test_value_type_fixed_size_list_type():
    value_type = DataType.int8()
    list_dt = DataType.list(Field("inner", value_type), 2)
    assert list_dt.value_type == value_type


@pytest.mark.xfail
def test_list_data_type_construction_with_dt():
    _ = DataType.list(DataType.int16())
