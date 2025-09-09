from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
from arro3.io import read_ipc, read_ipc_stream, write_ipc, write_ipc_stream


def test_ipc_round_trip_string():
    table = pa.table({"a": [1, 2, 3, 4]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        # Explicitly test with passing string input
        ipc_path = f"{tmp_path}/test.arrow"
        ipc_stream_path = f"{tmp_path}/test.arrows"

        write_ipc(table, ipc_path)
        table_retour = pa.table(read_ipc(ipc_path))
        assert table == table_retour

        write_ipc_stream(table, ipc_stream_path)
        table_retour = pa.table(read_ipc_stream(ipc_stream_path))
        assert table == table_retour


def test_ipc_round_trip_path():
    table = pa.table({"a": [1, 2, 3, 4]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        ipc_path = tmp_path / "test.arrow"
        ipc_stream_path = tmp_path / "test.arrows"

        write_ipc(table, ipc_path)
        table_retour = pa.table(read_ipc(ipc_path))
        assert table == table_retour

        write_ipc_stream(table, ipc_stream_path)
        table_retour = pa.table(read_ipc_stream(ipc_stream_path))
        assert table == table_retour


def test_ipc_round_trip_buffer():
    table = pa.table({"a": [1, 2, 3, 4]})
    bio = BytesIO()
    write_ipc(table, bio)
    table_retour = pa.table(read_ipc(bio))
    assert table == table_retour

    bio = BytesIO()
    write_ipc_stream(table, bio)
    bio.seek(0)
    table_retour = pa.table(read_ipc_stream(bio))
    assert table == table_retour


def test_ipc_round_trip_compression():
    table = pa.table({"a": [1, 2, 3, 4]})
    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        ipc_path = tmp_path / "test.arrow"
        write_ipc(table, ipc_path, compression="lz4")
        table_retour = pa.table(read_ipc(ipc_path))
        assert table == table_retour

        table = pa.table({"a": [1, 2, 3, 4]})
        write_ipc(table, ipc_path, compression="zstd")
        table_retour = pa.table(read_ipc(ipc_path))
        assert table == table_retour

        table = pa.table({"a": [1, 2, 3, 4]})
        write_ipc(table, ipc_path, compression=None)
        table_retour = pa.table(read_ipc(ipc_path))
        assert table == table_retour
