import numpy as np
import pyarrow as pa
from arro3.core import Array, DataType, Table


def test_from_numpy():
    arr = np.array([1, 2, 3, 4], dtype=np.uint8)
    assert Array.from_numpy(arr).type == DataType.uint8()

    arr = np.array([1, 2, 3, 4], dtype=np.float64)
    assert Array.from_numpy(arr).type == DataType.float64()


def test_extension_array_meta_persists():
    arr = pa.array([1, 2, 3])
    input_metadata = {"hello": "world"}
    field = pa.field("arr", type=arr.type, metadata=input_metadata)
    pa_table = pa.Table.from_arrays([arr], schema=pa.schema([field]))
    table = Table.from_arrow(pa_table)
    assert table[0].chunks[0].field.metadata_str == input_metadata
