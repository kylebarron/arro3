import pytest
from arro3.core import RecordBatchReader


class CustomException(Exception):
    pass


class ArrowCStreamFails:
    def __arrow_c_stream__(self, requested_schema=None):
        raise CustomException


def test_record_batch_reader_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCStreamFails()
    with pytest.raises(CustomException):
        RecordBatchReader.from_arrow(c_stream_obj)
