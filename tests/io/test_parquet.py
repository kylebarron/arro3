from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from arro3.core import Array, DataType, Table
from arro3.io import read_parquet, write_parquet


def test_parquet_round_trip():
    table = pa.table({"a": [1, 2, 3, 4]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        write_parquet(table, tmp_path / "test.parquet")
        table_retour = pa.table(read_parquet(tmp_path / "test.parquet"))
        assert table == table_retour


def test_parquet_round_trip_bytes_io():
    table = pa.table({"a": [1, 2, 3, 4]})
    with BytesIO() as bio:
        write_parquet(table, bio)
        bio.seek(0)
        table_retour = pa.table(read_parquet(bio))
    assert table == table_retour


@pytest.mark.parametrize(
    "wrap",
    [bytes, bytearray, memoryview, lambda b: np.frombuffer(b, dtype=np.uint8)],
    ids=["bytes", "bytearray", "memoryview", "numpy"],
)
def test_parquet_round_trip_buffer_protocol(wrap):
    """https://github.com/kylebarron/arro3/issues/228"""
    table = pa.table({"a": [1, 2, 3, 4]})
    bio = BytesIO()
    write_parquet(table, bio)
    table_retour = pa.table(read_parquet(wrap(bio.getvalue())))
    assert table == table_retour


def test_copy_parquet_kv_metadata():
    metadata = {"hello": "world"}
    table = pa.table({"a": [1, 2, 3]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        pq_path = tmp_path / "test.parquet"
        write_parquet(
            table,
            pq_path,
            key_value_metadata=metadata,
            skip_arrow_metadata=True,
        )

        # Assert metadata was written, but arrow schema was not
        pq_meta = pq.read_metadata(pq_path).metadata
        assert pq_meta[b"hello"] == b"world"
        assert b"ARROW:schema" not in pq_meta.keys()

        # When reading with pyarrow, kv meta gets assigned to table
        pa_table = pq.read_table(pq_path)
        assert pa_table.schema.metadata[b"hello"] == b"world"

        reader = read_parquet(pq_path)
        assert reader.schema.metadata[b"hello"] == b"world"


def test_read_parquet_batch_size():
    table = pa.table({"a": list(range(10_000)), "b": ["x"] * 10_000})
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        pq_path = tmp_path / "test.parquet"
        write_parquet(table, pq_path)

        # Default batch_size (1024) produces many batches
        batches_default = list(read_parquet(pq_path))
        assert len(batches_default) > 1

        # Large batch_size produces fewer batches
        batches_large = list(read_parquet(pq_path, batch_size=10_000))
        assert len(batches_large) == 1
        assert batches_large[0].num_rows == 10_000

        # Data is identical regardless of batch_size
        table_default = pa.table(read_parquet(pq_path).read_all())
        table_batched = pa.table(read_parquet(pq_path, batch_size=5_000).read_all())
        assert table_default == table_batched


def test_string_view():
    arr = Array(["foo", "bar", "baz"], type=DataType.string_view())
    table = Table.from_arrays([arr], names=["a"])

    bio = BytesIO()
    write_parquet(table, bio)
    bio.seek(0)

    table_retour = read_parquet(bio).read_all()
    assert table == table_retour
