import pyarrow as pa
import pyarrow.parquet as pq
from arro3.io import read_parquet, write_parquet


def test_parquet_round_trip():
    table = pa.table({"a": [1, 2, 3, 4]})
    write_parquet(table, "test.parquet")
    table_retour = pa.table(read_parquet("test.parquet"))
    assert table == table_retour


def test_copy_parquet_kv_metadata():
    metadata = {"hello": "world"}
    table = pa.table({"a": [1, 2, 3]})
    write_parquet(
        table,
        "test.parquet",
        key_value_metadata=metadata,
        skip_arrow_metadata=True,
    )

    # Assert metadata was written, but arrow schema was not
    pq_meta = pq.read_metadata("test.parquet").metadata
    assert pq_meta[b"hello"] == b"world"
    assert b"ARROW:schema" not in pq_meta.keys()

    # When reading with pyarrow, kv meta gets assigned to table
    pa_table = pq.read_table("test.parquet")
    assert pa_table.schema.metadata[b"hello"] == b"world"

    reader = read_parquet("test.parquet")
    assert reader.schema.metadata[b"hello"] == b"world"
