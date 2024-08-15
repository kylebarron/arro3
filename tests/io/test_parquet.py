import pyarrow.parquet as pq
import pyarrow as pa
from arro3.io import read_parquet
# from arro3


# It seems pyarrow only copies parquet metadata when it wasn't written by an Arrow implementation (i.e. when there's no b'ARROW:schema') key in the parquet meta.
def test_parquet_kv_metadata():
    table = pa.table({"a": [1, 2, 3]})
    with pq.ParquetWriter("test.parquet", table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({"hello": "world"})

    # pq.ParquetFile("test.parquet").metadata.metadata
    # pq.read_table("test.parquet").schema.metadata
    # pa.table(read_parquet("test.parquet")).schema.metadata
    reader = read_parquet("test.parquet")
    reader.schema.metadata

    pq
