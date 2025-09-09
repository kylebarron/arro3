import pyarrow as pa
import pytest
from arro3.core import RecordBatch


def test_nonempty_batch_no_columns():
    batch = pa.record_batch({"a": [1, 2, 3, 4]}).select([])
    assert len(batch) == 4
    assert batch.num_columns == 0
    arro3_batch = RecordBatch.from_arrow(batch)
    retour = pa.record_batch(arro3_batch)
    assert batch == retour


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
