import pyarrow as pa
from arro3.core import RecordBatch


def test_nonempty_batch_no_columns():
    batch = pa.record_batch({"a": [1, 2, 3, 4]}).select([])
    assert len(batch) == 4
    assert batch.num_columns == 0
    arro3_batch = RecordBatch.from_arrow(batch)
    retour = pa.record_batch(arro3_batch)
    assert batch == retour
