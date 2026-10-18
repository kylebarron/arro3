import mmap
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pytest
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


@pytest.mark.parametrize(
    "wrap",
    [bytes, bytearray, memoryview, lambda b: np.frombuffer(b, dtype=np.uint8)],
    ids=["bytes", "bytearray", "memoryview", "numpy"],
)
def test_ipc_round_trip_buffer_protocol(wrap):
    """https://github.com/kylebarron/arro3/issues/228"""
    table = pa.table({"a": [1, 2, 3, 4]})
    bio = BytesIO()
    write_ipc(table, bio)
    table_retour = pa.table(read_ipc(wrap(bio.getvalue())))
    assert table == table_retour

    bio = BytesIO()
    write_ipc_stream(table, bio)
    table_retour = pa.table(read_ipc_stream(wrap(bio.getvalue())))
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


@pytest.mark.parametrize("fmt", ["file", "stream"])
def test_ipc_buffer_input_is_zero_copy(fmt):
    table = pa.table({"a": pa.array(range(100_000), pa.int64())})
    write, read = (
        (write_ipc, read_ipc) if fmt == "file" else (write_ipc_stream, read_ipc_stream)
    )
    bio = BytesIO()
    write(table, bio, compression=None)

    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        (tmp_path / "data.arrow").write_bytes(bio.getvalue())
        with open(tmp_path / "data.arrow", "rb") as f:
            m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            with memoryview(m) as view:
                start = pa.py_buffer(view).address
            end = start + len(m)

            table_retour = pa.table(read(m))
            assert table == table_retour
            for chunk in table_retour.column("a").chunks:
                assert start <= chunk.buffers()[1].address < end

            # The batches reference the mapping
            with pytest.raises(BufferError):
                m.close()
        finally:
            # Windows can't remove the directory while the file is mapped
            table_retour = chunk = None
            m.close()


@pytest.mark.parametrize("fmt", ["file", "stream"])
def test_ipc_writable_buffer_input_is_copied(fmt):
    table = pa.table({"a": pa.array(range(1000), pa.int64())})
    write, read = (
        (write_ipc, read_ipc) if fmt == "file" else (write_ipc_stream, read_ipc_stream)
    )
    bio = BytesIO()
    write(table, bio, compression=None)
    data = bio.getvalue()

    for buf in (bytearray(data), np.frombuffer(bytearray(data), dtype=np.uint8)):
        table_retour = pa.table(read(buf))
        memoryview(buf)[:] = bytes(len(data))
        assert table == table_retour

    # We can't use tmp_path fixture with pytest-freethreading
    with TemporaryDirectory() as tmp_path:
        tmp_path = Path(tmp_path)
        (tmp_path / "data.arrow").write_bytes(data)
        with open(tmp_path / "data.arrow", "r+b") as f:
            m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)
        try:
            table_retour = pa.table(read(m))
        finally:
            # Succeeds because the table doesn't reference the mapping
            m.close()
        assert table == table_retour


def test_ipc_stream_buffer_reads_schema_lazily():
    table = pa.table({"a": [1, 2, 3]})
    bio = BytesIO()
    write_ipc_stream(table, bio, compression=None)
    data = bio.getvalue()
    # Put a second schema message where the first batch should be
    schema_len = 8 + int.from_bytes(data[4:8], "little")
    data = data[:schema_len] + data
    for file in (data, BytesIO(data)):
        reader = read_ipc_stream(file)
        assert pa.schema(reader.schema) == table.schema
        with pytest.raises(Exception, match="schema"):
            pa.table(reader)


@pytest.mark.parametrize("n_rows", [3, 0])
def test_ipc_stream_buffer_ignores_bytes_after_eos(n_rows):
    table = pa.table({"a": pa.array(range(n_rows), pa.int64())})
    bio = BytesIO()
    write_ipc_stream(table, bio, compression=None)
    for trailing in (bytes(4096), bio.getvalue()):
        data = bio.getvalue() + trailing
        assert pa.table(read_ipc_stream(data)) == table
        assert pa.table(read_ipc_stream(BytesIO(data))) == table


def test_ipc_stream_buffer_rejects_truncation():
    table = pa.table({"a": list(range(1000))})
    bio = BytesIO()
    write_ipc_stream(table, bio, compression=None)
    data = bio.getvalue()[:-100]
    reader = read_ipc_stream(data)
    with pytest.raises(Exception, match="Unexpected End of Stream"):
        pa.table(reader)


def test_ipc_stream_buffer_schema_only():
    table = pa.table({"a": [1, 2, 3]})
    bio = BytesIO()
    write_ipc_stream(table, bio, compression=None)
    data = bio.getvalue()
    # Only the schema message, without the end-of-stream marker
    data = data[: 8 + int.from_bytes(data[4:8], "little")]
    for file in (data, BytesIO(data)):
        assert pa.table(read_ipc_stream(file)).schema == table.schema


def test_ipc_buffer_dictionary_round_trip():
    table = pa.table({"d": pa.array(["x", "y", "x", "z"]).dictionary_encode()})
    for write, read in ((write_ipc, read_ipc), (write_ipc_stream, read_ipc_stream)):
        bio = BytesIO()
        write(table, bio, compression=None)
        assert pa.table(read(bio.getvalue())) == table
