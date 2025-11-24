import pyarrow as pa
import pytest
from arro3.core import RecordBatch, Schema


def test_nonempty_batch_no_columns():
    batch = pa.record_batch({"a": [1, 2, 3, 4]}).select([])
    assert len(batch) == 4
    assert batch.num_columns == 0
    arro3_batch = RecordBatch.from_arrow(batch)
    retour = pa.record_batch(arro3_batch)
    assert batch == retour


def test_batch_from_arrays():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])
    arro3_batch = RecordBatch.from_arrays([a, b], names=["int", "str"])
    pa_batch = pa.RecordBatch.from_arrays([a, b], names=["int", "str"])
    assert pa.record_batch(arro3_batch) == pa_batch

    # With metadata
    metadata = {b"key": b"value"}
    arro3_batch = RecordBatch.from_arrays(
        [a, b], names=["int", "str"], metadata=metadata
    )
    pa_batch = pa.RecordBatch.from_arrays(
        [a, b], names=["int", "str"], metadata=metadata
    )
    assert pa.record_batch(arro3_batch) == pa_batch
    assert arro3_batch.schema.metadata == metadata

    # With schema
    schema = Schema(
        [
            pa.field("int", type=pa.int64()),
            pa.field("str", type=pa.utf8()),
        ]
    )
    arro3_batch = RecordBatch.from_arrays([a, b], schema=schema)
    pa_batch = pa.RecordBatch.from_arrays([a, b], schema=pa.schema(schema))
    assert pa.record_batch(arro3_batch) == pa_batch

    # Empty batch
    pa_arr_empty = pa.array([], type=pa.int64())
    arro3_batch = RecordBatch.from_arrays([pa_arr_empty], names=["int"])
    pa_batch = pa.RecordBatch.from_arrays([pa_arr_empty], names=["int"])
    assert pa.record_batch(arro3_batch) == pa_batch

    # No names nor schema
    with pytest.raises(
        ValueError, match="names must be passed if schema is not passed"
    ):
        RecordBatch.from_arrays([a, b])


class CustomException(Exception):
    pass


class ArrowCArrayFails:
    def __arrow_c_array__(self, requested_schema=None):
        raise CustomException


def test_record_batch_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCArrayFails()
    with pytest.raises(CustomException):
        RecordBatch.from_arrow(c_stream_obj)

    with pytest.raises(CustomException):
        RecordBatch(c_stream_obj)
