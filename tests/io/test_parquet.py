from pathlib import Path
from tempfile import TemporaryDirectory
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
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
