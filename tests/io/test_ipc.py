from io import BytesIO
from pathlib import Path

import pyarrow as pa
from arro3.io import read_ipc, read_ipc_stream, write_ipc, write_ipc_stream

from . import pytestmark  # noqa: F401


def test_ipc_round_trip_string():
    table = pa.table({"a": [1, 2, 3, 4]})
    write_ipc(table, "test.arrow")
    table_retour = pa.table(read_ipc("test.arrow"))
    assert table == table_retour

    write_ipc_stream(table, "test.arrows")
    table_retour = pa.table(read_ipc_stream("test.arrows"))
    assert table == table_retour


def test_ipc_round_trip_path():
    table = pa.table({"a": [1, 2, 3, 4]})
    write_ipc(table, Path("test.arrow"))
    table_retour = pa.table(read_ipc(Path("test.arrow")))
    assert table == table_retour

    write_ipc_stream(table, Path("test.arrows"))
    table_retour = pa.table(read_ipc_stream(Path("test.arrows")))
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
    write_ipc(table, "test.arrow", compression="lz4")
    table_retour = pa.table(read_ipc("test.arrow"))
    assert table == table_retour

    table = pa.table({"a": [1, 2, 3, 4]})
    write_ipc(table, "test.arrow", compression="zstd")
    table_retour = pa.table(read_ipc("test.arrow"))
    assert table == table_retour

    table = pa.table({"a": [1, 2, 3, 4]})
    write_ipc(table, "test.arrow", compression=None)
    table_retour = pa.table(read_ipc("test.arrow"))
    assert table == table_retour
