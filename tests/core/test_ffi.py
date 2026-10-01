import pyarrow as pa
import pytest
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


def test_record_batch_reader_from_batches_generator():
    """from_batches accepts a generator and consumes it lazily."""
    a = pa.array([1, 2, 3], type=pa.int32())
    table = Table.from_pydict({"a": a})
    schema = table.schema
    batches = table.to_batches()

    consumed = []

    def batch_gen():
        for batch in batches:
            consumed.append(True)
            yield batch

    gen = batch_gen()
    reader = RecordBatchReader.from_batches(schema, gen)

    # Generator not consumed yet
    assert len(consumed) == 0

    result = reader.read_all()
    assert result.num_rows == 3
    assert len(consumed) == 1  # consumed lazily


def test_record_batch_reader_from_batches_list():
    """from_batches still accepts a list (backwards compat)."""
    a = pa.array([1, 2, 3], type=pa.int32())
    table = Table.from_pydict({"a": a})
    reader = RecordBatchReader.from_batches(table.schema, table.to_batches())
    result = reader.read_all()
    assert result.num_rows == 3


def test_record_batch_reader_metadata_preserved():
    metadata = {b"hello": b"world"}
    pa_table = pa.table({"a": [1, 2, 3]})
    pa_table = pa_table.replace_schema_metadata(metadata)
    pa_reader = pa.RecordBatchReader.from_stream(pa_table)

    arro3_reader = RecordBatchReader.from_stream(pa_reader)
    assert arro3_reader.schema.metadata == metadata

    pa_reader_retour = pa.RecordBatchReader.from_stream(arro3_reader)
    assert pa_reader_retour.schema.metadata == metadata


def test_record_batch_reader_from_batches_non_iterable_raises():
    """A non-iterable input fails eagerly with a descriptive TypeError."""
    table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int32())})
    with pytest.raises(TypeError, match="sequence or iterable of record batches"):
        RecordBatchReader.from_batches(table.schema, 123)  # type: ignore


def test_record_batch_reader_from_batches_bad_sequence_element_raises_eagerly():
    """A sequence whose elements are not record batches fails at construction."""
    table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int32())})
    with pytest.raises(Exception):
        RecordBatchReader.from_batches(table.schema, [1, 2])  # type: ignore


def test_record_batch_reader_from_batches_sized_iterable_is_lazy():
    """An iterable with __len__ but no __getitem__ is not materialized eagerly.

    pyo3 only extracts a ``Vec`` from objects passing ``PySequence_Check``, so an
    object that is merely iterable (even if sized) must go down the lazy path.
    """
    table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int32())})
    batches = table.to_batches()
    consumed = []

    class SizedIterable:
        def __len__(self):
            return len(batches)

        def __iter__(self):
            for batch in batches:
                consumed.append(True)
                yield batch

    reader = RecordBatchReader.from_batches(table.schema, SizedIterable())
    assert len(consumed) == 0

    result = reader.read_all()
    assert result.num_rows == 3
    assert len(consumed) == 1


def test_record_batch_reader_from_batches_generator_exception_propagates():
    """An exception raised inside the generator surfaces with its original type."""
    table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int32())})

    def batch_gen():
        yield from table.to_batches()
        raise ValueError("bad row")

    reader = RecordBatchReader.from_batches(table.schema, batch_gen())
    with pytest.raises(ValueError, match="bad row"):
        reader.read_all()

    # An error while iterating leaves the reader usable rather than permanently closed.
    reader = RecordBatchReader.from_batches(table.schema, batch_gen())
    with pytest.raises(ValueError, match="bad row"):
        for _ in reader:
            pass
    assert not reader.closed


def test_record_batch_reader_from_batches_generator_may_touch_reader():
    """The iterator feeding a reader can use the reader itself without deadlocking.

    `read_next_batch` must not hold the reader's lock while running Python code.
    While the stream is being advanced it is checked out of the reader, which then
    reports itself as closed.
    """
    table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int32())})
    reader = None

    def batch_gen():
        for batch in table.to_batches():
            assert reader.closed
            yield batch

    reader = RecordBatchReader.from_batches(table.schema, batch_gen())
    assert reader.read_next_batch().num_rows == 3
    assert not reader.closed


def test_record_batch_reader_from_batches_sequence_schema_mismatch_raises_eagerly():
    """A batch whose schema differs from the declared one is rejected at construction."""
    schema = pa.schema([pa.field("a", pa.int32())])
    batch = pa.record_batch({"a": pa.array([1, 2], pa.int64())})
    with pytest.raises(ValueError, match="Schema at index 0 was different"):
        RecordBatchReader.from_batches(schema, [batch])


def test_record_batch_reader_from_batches_iterator_schema_mismatch_raises_on_read():
    """A lazily yielded batch whose schema differs is rejected when it is pulled."""
    schema = pa.schema([pa.field("a", pa.int32())])

    def batch_gen():
        yield pa.record_batch({"a": pa.array([1, 2], pa.int32())})
        yield pa.record_batch({"a": pa.array([1, 2], pa.int64())})

    reader = RecordBatchReader.from_batches(schema, batch_gen())
    with pytest.raises(ValueError, match="Schema at index 1 was different"):
        reader.read_all()


def test_record_batch_reader_from_batches_schema_check_ignores_metadata():
    """Like pyarrow, schema and field metadata are not part of the comparison."""
    schema = pa.schema(
        [pa.field("a", pa.int32(), metadata={"f": "1"})], metadata={"k": "v"}
    )
    batch = pa.record_batch({"a": pa.array([1, 2], pa.int32())})

    table = RecordBatchReader.from_batches(schema, [batch]).read_all()
    assert table.schema == schema
    table = RecordBatchReader.from_batches(schema, iter([batch])).read_all()
    assert table.schema == schema
