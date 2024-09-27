import pyarrow as pa
from arro3.core import Array, ChunkedArray, DataType, RecordBatchReader, Schema, Table


def test_table_stream_export_schema_request():
    a = pa.array(["a", "b", "c"], type=pa.utf8())
    table = Table.from_pydict({"a": a})

    requested_schema = Schema([pa.field("a", type=pa.large_utf8())])
    requested_schema_capsule = requested_schema.__arrow_c_schema__()
    stream_capsule = table.__arrow_c_stream__(requested_schema_capsule)

    retour = Table.from_arrow_pycapsule(stream_capsule)
    assert retour.schema.field("a").type == DataType.large_utf8()


def test_record_batch_reader_stream_export_schema_request():
    a = pa.array(["a", "b", "c"], type=pa.utf8())
    table = Table.from_pydict({"a": a})
    reader = RecordBatchReader.from_batches(table.schema, table.to_batches())

    requested_schema = Schema([pa.field("a", type=pa.large_utf8())])
    requested_schema_capsule = requested_schema.__arrow_c_schema__()
    stream_capsule = reader.__arrow_c_stream__(requested_schema_capsule)

    retour = Table.from_arrow_pycapsule(stream_capsule)
    assert retour.schema.field("a").type == DataType.large_utf8()


def test_chunked_array_stream_export_schema_request():
    a = pa.array(["a", "b", "c"], type=pa.utf8())
    ca = ChunkedArray([a, a])

    requested_schema_capsule = pa.large_utf8().__arrow_c_schema__()
    stream_capsule = ca.__arrow_c_stream__(requested_schema_capsule)

    retour = ChunkedArray.from_arrow_pycapsule(stream_capsule)
    assert retour.type == DataType.large_utf8()


def test_array_export_schema_request():
    a = pa.array(["a", "b", "c"], type=pa.utf8())
    arr = Array(a)

    requested_schema_capsule = pa.large_utf8().__arrow_c_schema__()
    capsules = arr.__arrow_c_array__(requested_schema_capsule)

    retour = Array.from_arrow_pycapsule(*capsules)
    assert retour.type == DataType.large_utf8()


def test_table_metadata_preserved():
    metadata = {b"hello": b"world"}
    pa_table = pa.table({"a": [1, 2, 3]})
    pa_table = pa_table.replace_schema_metadata(metadata)

    arro3_table = Table(pa_table)
    assert arro3_table.schema.metadata == metadata

    pa_table_retour = pa.table(arro3_table)
    assert pa_table_retour.schema.metadata == metadata


def test_record_batch_reader_metadata_preserved():
    metadata = {b"hello": b"world"}
    pa_table = pa.table({"a": [1, 2, 3]})
    pa_table = pa_table.replace_schema_metadata(metadata)
    pa_reader = pa.RecordBatchReader.from_stream(pa_table)

    arro3_reader = RecordBatchReader.from_stream(pa_reader)
    assert arro3_reader.schema.metadata == metadata

    pa_reader_retour = pa.RecordBatchReader.from_stream(arro3_reader)
    assert pa_reader_retour.schema.metadata == metadata
