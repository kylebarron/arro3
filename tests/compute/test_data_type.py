from arro3.compute import parse_data_type
from arro3.core import DataType
import pyarrow as pa

def test_parse_data_type():
    assert parse_data_type(DataType.int32()) == DataType.int32()

def test_pyarrow():
    assert parse_data_type(pa.int32()) == DataType.int32()
