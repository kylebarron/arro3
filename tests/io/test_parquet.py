import pyarrow.parquet as pq
import pyarrow as pa
from arro3.io import read_parquet
from arro3.core import RecordBatchReader
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
    # Ok so it's probably on export, not import
    pa_reader = pa.RecordBatchReader.from_stream(reader)

    pa_reader.schema.metadata
    reader.schema.metadata

    pq


def other_test():
    table = pa.table({"a": [1, 2, 3]})
    table = table.replace_schema_metadata({"hello": "world"})
    assert table.schema.metadata[b"hello"] == b"world"

    # Ok so consuming works
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    arro3_reader = RecordBatchReader.from_stream(reader)

    # This loses metadata, so it's a problem when exporting schema metadata
    pa_reader_retour = pa.RecordBatchReader.from_stream(arro3_reader)
    pa_reader_retour.schema.metadata
