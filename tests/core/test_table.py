import geoarrow.types as gt
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from arro3.core import Array, ArrayReader, ChunkedArray, DataType, Field, Table


def test_table_getitem():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    assert a == pa.chunked_array(table["a"])
    assert b == pa.chunked_array(table["b"])
    assert a == pa.chunked_array(table[0])
    assert b == pa.chunked_array(table[1])

    with pytest.raises(KeyError):
        table["foo"]

    with pytest.raises(IndexError):
        table[10]


def test_table_from_arrays():
    a = pa.array([1, 2, 3, 4])
    b = pa.array(["a", "b", "c", "d"])
    arro3_table = Table.from_arrays([a, b], names=["a", "b"])
    pa_table = pa.Table.from_arrays([a, b], names=["a", "b"])
    assert pa.table(arro3_table) == pa_table


def test_table_from_pydict():
    mapping = {"a": pa.array([1, 2, 3, 4]), "b": pa.array(["a", "b", "c", "d"])}
    arro3_table = Table.from_pydict(mapping)
    pa_table = pa.Table.from_pydict(mapping)
    assert pa.table(arro3_table) == pa_table


def test_table_constructor_ext_array():
    typ = DataType.uint8()
    metadata = {"ARROW:extension:name": "ext_name"}
    field = Field("", type=typ, nullable=True, metadata=metadata)
    arr = Array([1, 2, 3, 4], field)
    t = Table({"a": arr})
    assert t.schema.field("a").metadata_str["ARROW:extension:name"] == "ext_name"

    ca = ChunkedArray([arr], field)
    t = Table({"a": ca})
    assert t.schema.field("a").metadata_str["ARROW:extension:name"] == "ext_name"


def test_table_append_array_extension_type():
    """
    Test that extension metadata gets propagated from an array to a column on a table.
    """
    # Test that extension
    extension_type = gt.point(dimensions="xy", coord_type="interleaved").to_pyarrow()
    coords = np.array([1, 2, 3, 4], dtype=np.float64)
    ext_array = pa.FixedSizeListArray.from_arrays(coords, 2).cast(extension_type)

    table = Table.from_arrays([pa.array(["a", "b"])], names=["a"])
    geo_table = table.append_column("geometry", ChunkedArray([ext_array]))

    meta = geo_table.schema["geometry"].metadata
    assert b"ARROW:extension:name" in meta.keys()
    assert meta[b"ARROW:extension:name"] == b"geoarrow.point"


def test_table_append_column():
    """
    Test that Table.append_column appends columns of different types.
    """
    table = Table.from_arrays([pa.array(["a", "b"])], names=["c0"])
    expected_num_columns = 4

    # PyArray
    c_name, c_value = "c1", [1, 2]
    table = table.append_column(c_name, Array(pa.array(c_value)))
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    # PyChunkedArray
    c_name, c_value = "c2", [3, 4]
    table = table.append_column(c_name, ChunkedArray(pa.array(c_value)))
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    # PyArrayReader
    c_name, c_value = "c3", [5, 6]
    reader = ArrayReader.from_arrays(
        pa.field("_", pa.int64()), arrays=[pa.array(c_value)]
    )
    table = table.append_column(c_name, reader)
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value
    assert len(table.columns) == expected_num_columns


def test_table_append_column_chunked():
    """
    Test that a column can be appended and behaves correctly when the table
    is chunked.
    """
    rbs = [
        pa.record_batch(
            [
                pa.array(
                    [
                        1,
                    ]
                ),
            ],
            names=["c0"],
        ),
        pa.record_batch(
            [
                pa.array(
                    [
                        2,
                    ]
                ),
            ],
            names=["c0"],
        ),
    ]

    table = Table.from_batches(rbs)
    assert table.chunk_lengths == [1, 1]

    c_name, c_value = "c1", [3, 4]
    table = table.append_column(c_name, Array(pa.array(c_value)))

    # Chunks should be the same, we only added a column.
    assert table.chunk_lengths == [1, 1]
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value

    table = table.rechunk(max_chunksize=3)
    # After rechunking _only_ chunk sizes should change.
    assert table.chunk_lengths == [
        2,
    ]
    assert c_name in table.column_names
    assert table[c_name].to_pylist() == c_value


def test_table_add_column():
    """
    Test that Table.add_column appends columns of different types at the
    given index.
    """
    table = Table.from_arrays([pa.array(["a", "b"])], names=["c0"])
    col_id = 0
    expected_num_columns = 4

    # PyArray
    c_name, c_value = "c1", [1, 2]
    table = table.add_column(col_id, c_name, Array(pa.array(c_value)))
    assert c_name in table.column_names
    assert table.column(col_id).to_pylist() == c_value

    # PyChunkedArray
    c_name, c_value = "c2", [3, 4]
    table = table.add_column(col_id, c_name, ChunkedArray(pa.array(c_value)))
    assert c_name in table.column_names
    assert table.column(col_id).to_pylist() == c_value

    # PyArrayReader
    c_name, c_value = "c3", [5, 6]
    reader = ArrayReader.from_arrays(
        pa.field("_", pa.int64()), arrays=[pa.array(c_value)]
    )
    table = table.add_column(col_id, c_name, reader)
    assert c_name in table.column_names
    assert table.column(col_id).to_pylist() == c_value
    assert len(table.columns) == expected_num_columns

    # Just in case, let's test an index different of 0.
    table = table.add_column(col_id + 1, c_name + "extra", Array(pa.array(c_value)))
    assert table.column(col_id).to_pylist() == c_value

    with pytest.raises(
        IndexError, match="Column index out of range, index is 6 but should be <= 5"
    ):
        table.add_column(table.num_columns + 1, "_", Array(pa.array(c_value)))


def test_table_add_column_chunked():
    """Test that a table is correctly added in a chunked table."""
    rbs = [
        pa.record_batch(
            [
                pa.array(
                    [
                        1,
                    ]
                ),
            ],
            names=["c0"],
        ),
        pa.record_batch(
            [
                pa.array(
                    [
                        2,
                    ]
                ),
            ],
            names=["c0"],
        ),
    ]

    table = Table.from_batches(rbs)
    assert table.chunk_lengths == [1, 1]

    c_name, c_value, col_id = "c1", [3, 4], 0
    table = table.add_column(col_id, c_name, Array(pa.array(c_value)))

    assert c_name in table.column_names
    assert table.column(col_id).to_pylist() == c_value

    table = table.rechunk(max_chunksize=10)

    c_name, c_value, col_id = "c2", [5, 6], 1  # <- different id
    table = table.add_column(col_id, c_name, Array(pa.array(c_value)))
    assert c_name in table.column_names
    assert table.column(col_id).to_pylist() == c_value
    assert table.chunk_lengths == [2]


def test_table_set_column():
    """
    Test that we can set a column from other types like Array, ChunkedArray, and ArrayReader
    """
    table = Table.from_arrays([pa.array([1, 2])], names=["c0"])

    c_value, c_name, c_index = [3, 4], "c1", 0
    table = table.set_column(c_index, c_name, Array(pa.array(c_value)))
    assert table.column(c_index).to_pylist() == c_value
    assert c_name in table.column_names
    assert (
        table.column(c_index).field.name == c_name
    )  # checks that the rename was effective.

    c_value, c_name, c_index = [4, 5], "c2", 0
    table = table.set_column(c_index, c_name, ChunkedArray(pa.array(c_value)))
    assert table.column(c_index).to_pylist() == c_value
    assert c_name in table.column_names
    assert table.column(c_index).field.name == c_name

    c_value, c_name, c_index = [6, 7], "c3", 0
    reader = ArrayReader.from_arrays(
        pa.field("_", pa.int64()), arrays=[pa.array(c_value)]
    )
    table = table.set_column(c_index, c_name, reader)
    assert table.column(c_index).to_pylist() == c_value
    assert c_name in table.column_names
    assert table.column(c_index).field.name == c_name


def test_table_set_column_chunked():
    """
    Test that a table's column can be set as an array when it's chunked, and after it's
    rechunked.
    """

    rbs = [
        pa.record_batch(
            [
                pa.array(
                    [
                        1,
                    ]
                ),
            ],
            names=["c0"],
        ),
        pa.record_batch(
            [
                pa.array(
                    [
                        2,
                    ]
                ),
            ],
            names=["c0"],
        ),
    ]
    table = Table.from_batches(rbs)

    c_value, c_name, c_index = [3, 4], "c0", 0
    table = table.set_column(c_index, c_name, Array(pa.array(c_value)))
    assert table.column(c_index).to_pylist() == c_value
    assert c_name in table.column_names
    assert table.chunk_lengths == [1, 1]

    table = table.rechunk(max_chunksize=10)
    assert table.chunk_lengths == [2]

    # Can it be set again after a rechunk?
    c_value, c_name, c_index = [5, 6], "c0", 0  # Different column name on purpose.
    table = table.set_column(c_index, c_name, Array(pa.array(c_value)))
    assert table.column(c_index).to_pylist() == c_value
    assert c_name in table.column_names
    assert table.chunk_lengths == [2]


def test_table_rename():
    table = Table.from_arrays(
        [pa.array(["a", "b"]), pa.array([1, 2]), pa.array(["x", "y"])],
        names=["c0", "c1", "c2"],
    )

    new_names = ["d1", "d2", "d3"]
    table = table.rename_columns(new_names)
    assert table.column_names == new_names

    with pytest.raises(ValueError, match="Expected 3 names, got 1"):
        table.rename_columns(
            [
                "onlyoneargument",
            ]
        )


def test_table_from_batches_empty_columns_with_len():
    df = pd.DataFrame({"a": [1, 2, 3]})
    no_columns = df[[]]
    pa_table = pa.Table.from_pandas(no_columns)
    table = Table.from_batches(pa_table.to_batches())
    assert table.num_columns == 0
    assert table.num_rows == 3


def test_rechunk():
    a = pa.chunked_array([[1, 2, 3, 4]])
    b = pa.chunked_array([["a", "b", "c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    rechunked1 = table.rechunk(max_chunksize=1)
    assert rechunked1.chunk_lengths == [1, 1, 1, 1]

    rechunked2 = rechunked1.rechunk(max_chunksize=2)
    assert rechunked2.chunk_lengths == [2, 2]
    assert rechunked2.rechunk().chunk_lengths == [4]


def test_slice():
    a = pa.chunked_array([[1, 2], [3, 4]])
    b = pa.chunked_array([["a", "b"], ["c", "d"]])
    table = Table.from_pydict({"a": a, "b": b})

    sliced1 = table.slice(0, 1)
    assert sliced1.num_rows == 1
    assert sliced1.chunk_lengths == [1]

    sliced2 = table.slice(1, 2)
    assert sliced2.num_rows == 2
    assert sliced2.chunk_lengths == [1, 1]


def test_nonempty_table_no_columns():
    table = pa.table({"a": [1, 2, 3, 4]}).select([])
    assert len(table) == 4
    assert table.num_columns == 0
    arro3_table = Table.from_arrow(table)
    retour = pa.table(arro3_table)
    assert table == retour


def test_remove_column():
    """Test the removal of columns given an index"""
    table = Table.from_arrays(
        [pa.array([1, 2]), pa.array(["a", "b"]), pa.array([10, 20])],
        names=["c0", "c1", "c2"],
    )

    table = table.remove_column(0)
    assert "c0" not in table.column_names
    assert len(table.columns) == 2

    table = table.remove_column(1)
    assert "c2" not in table.column_names
    assert len(table.columns) == 1

    with pytest.raises(IndexError, match="Invalid column index"):
        table.remove_column(1)

    # Other types than int raise an Error
    with pytest.raises(TypeError):
        table.remove_column("mycolumn")


def test_drop_columns():
    """
    Test that several columns can be dropped at the same time.
    """

    table = pa.table({"a": [1, 2], "b": [1, 2], "c": [1, 2], "d": [1, 2], "e": [1, 2]})

    table = Table.from_arrow(table)
    del_column = "a", "c"
    expected_columns = ["b", "d", "e"]

    table = table.drop_columns(del_column)

    assert table.column_names == expected_columns

    with pytest.raises(KeyError, match=r'Column\(s\): \["c", "abcde"\] not found'):
        # This should now raise an exception
        # since "c" no longer exists in the table.
        table.drop_columns(["c", "abcde"])

    # All columns should be removed or none, in this case "d" exists but not "abcde"
    with pytest.raises(KeyError):
        table.drop_columns(["abcde", "d"])
    assert table.column_names == expected_columns

    # It's case-sensitive.
    with pytest.raises(KeyError):
        table.drop_columns(["D"])

    # Any other unsupported type raises an exception.
    with pytest.raises(TypeError):
        table.drop_columns([Field("D", type=pa.int64())])

    # Empty input should not delete any columns.
    table = table.drop_columns([])
    assert table.column_names == expected_columns

    # Verify other sequences
    table = table.drop_columns([][:])
    table = table.drop_columns(tuple())
    assert table.column_names == expected_columns

    # at this point, "b" exists.
    assert "b" in table.column_names
    # This should not raise an error.
    # https://github.com/kylebarron/arro3/pull/440#discussion_r2495784707
    table = table.drop_columns(["b", "b", "b"])
    assert "b" not in table.column_names

    with pytest.raises(KeyError, match="not found"):
        table.drop_columns(["ccccde", "ccccde"])

    # keyword argument works as intended.
    table = table.drop_columns(columns=["d", "e"])
    assert not table.column_names


class CustomException(Exception):
    pass


class ArrowCStreamFails:
    def __arrow_c_stream__(self, requested_schema=None):
        raise CustomException


def test_table_import_preserve_exception():
    """https://github.com/kylebarron/arro3/issues/325"""

    c_stream_obj = ArrowCStreamFails()
    with pytest.raises(CustomException):
        Table.from_arrow(c_stream_obj)

    with pytest.raises(CustomException):
        Table(c_stream_obj)
