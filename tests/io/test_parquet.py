from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
from arro3.io import read_parquet, read_parquet_async, write_parquet
from arro3.io.store import HTTPStore


def test_parquet_round_trip():
    table = pa.table({"a": [1, 2, 3, 4]})
    write_parquet(table, "test.parquet")
    table_retour = pa.table(read_parquet("test.parquet"))
    assert table == table_retour


def test_parquet_round_trip_bytes_io():
    table = pa.table({"a": [1, 2, 3, 4]})
    with BytesIO() as bio:
        write_parquet(table, bio)
        bio.seek(0)
        table_retour = pa.table(read_parquet(bio))
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


async def test_stream_parquet():
    from time import time

    t0 = time()
    url = "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00217-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet"
    store = HTTPStore.from_url(url)
    stream = await read_parquet_async("", store=store)
    t1 = time()
    first = await stream.__anext__()
    t2 = time()

    print(t1 - t0)
    print(t2 - t1)

    test = await stream.collect_async()
    len(test)
    async for batch in stream:
        break

    batch.num_rows
    x = await stream.__anext__()

    pass
