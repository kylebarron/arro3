from datetime import date, datetime, timedelta

import numpy as np
import pyarrow as pa
from arro3.core import Array, DataType


def test_from_numpy():
    arr = np.array([1, 2, 3, 4], dtype=np.uint8)
    assert Array.from_numpy(arr).type == DataType.uint8()

    arr = np.array([1, 2, 3, 4], dtype=np.float64)
    assert Array.from_numpy(arr).type == DataType.float64()

    # arr = np.array([b"1", b"2", b"3"], np.object_)
    # Array.from_numpy(arr)


def test_binary_to_numpy():
    bytes_list = [b"1", b"2", b"3"]
    expected = np.array(bytes_list, dtype=np.object_)

    arr = Array(bytes_list, DataType.binary())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(bytes_list, DataType.large_binary())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(bytes_list, DataType.binary_view())
    assert np.array_equal(arr.to_numpy(), expected)


def test_string_to_numpy():
    string_list = ["1", "2", "3"]
    expected = np.array(string_list, dtype=np.object_)

    arr = Array(string_list, DataType.string())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(string_list, DataType.large_string())
    assert np.array_equal(arr.to_numpy(), expected)

    arr = Array(string_list, DataType.string_view())
    assert np.array_equal(arr.to_numpy(), expected)


def test_date_from_numpy():
    dates = [date(2023, 1, 1), date(2023, 1, 2)]
    data = np.array(dates, dtype="datetime64[D]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.date64()
    assert arr[0].as_py() == dates[0]
    assert arr[1].as_py() == dates[1]


def test_date_from_numpy_non_contiguous():
    dates = [
        date(2023, 1, 1),
        date(2023, 1, 2),
        date(2023, 1, 3),
    ]
    data = np.array(dates, dtype="datetime64[D]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.date64()
    assert arr[0].as_py() == dates[0]
    assert arr[1].as_py() == dates[2]


def test_seconds_from_numpy():
    seconds = [datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 2, 0, 0, 0)]
    data = np.array(seconds, dtype="datetime64[s]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("s")
    assert arr[0].as_py() == seconds[0]
    assert arr[1].as_py() == seconds[1]


def test_seconds_from_numpy_non_contiguous():
    seconds = [
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 2, 0, 0, 0),
        datetime(2023, 1, 3, 0, 0, 0),
    ]
    data = np.array(seconds, dtype="datetime64[s]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("s")
    assert arr[0].as_py() == seconds[0]
    assert arr[1].as_py() == seconds[2]


def test_milliseconds_from_numpy():
    milliseconds = [
        datetime(2023, 1, 1, 0, 0, 0, 123000),
        datetime(2023, 1, 2, 0, 0, 0, 654000),
    ]
    data = np.array(milliseconds, dtype="datetime64[ms]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("ms")
    assert arr[0].as_py() == milliseconds[0]
    assert arr[1].as_py() == milliseconds[1]


def test_milliseconds_from_numpy_non_contiguous():
    milliseconds = [
        datetime(2023, 1, 1, 0, 0, 0, 123000),
        datetime(2023, 1, 2, 0, 0, 0, 654000),
        datetime(2023, 1, 3, 0, 0, 0, 789000),
    ]
    data = np.array(milliseconds, dtype="datetime64[ms]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("ms")
    assert arr[0].as_py() == milliseconds[0]
    assert arr[1].as_py() == milliseconds[2]


def test_microseconds_from_numpy():
    microseconds = [
        datetime(2023, 1, 1, 0, 0, 0, 123456),
        datetime(2023, 1, 2, 0, 0, 0, 654321),
    ]
    data = np.array(microseconds, dtype="datetime64[us]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("us")
    assert arr[0].as_py() == microseconds[0]
    assert arr[1].as_py() == microseconds[1]


def test_microseconds_from_numpy_non_contiguous():
    microseconds = [
        datetime(2023, 1, 1, 0, 0, 0, 123456),
        datetime(2023, 1, 2, 0, 0, 0, 654321),
        datetime(2023, 1, 3, 0, 0, 0, 789012),
    ]
    data = np.array(microseconds, dtype="datetime64[us]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("us")
    assert arr[0].as_py() == microseconds[0]
    assert arr[1].as_py() == microseconds[2]


def test_nanoseconds_from_numpy():
    date_strings = [
        "2023-01-01T10:30:00.123456789",
        "2023-01-01T10:30:00.987654321",
    ]
    data = np.array(date_strings, dtype="datetime64[ns]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("ns")

    # We can't go through Python datetime because it doesn't support nanoseconds
    assert pa.array(arr) == pa.array(data)


def test_nanoseconds_from_numpy_non_contiguous():
    date_strings = [
        "2023-01-01T10:30:00.123456789",
        "2023-01-01T10:30:00.987654321",
        "2023-01-01T10:30:00.111111111",
    ]
    data = np.array(date_strings, dtype="datetime64[ns]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.timestamp("ns")

    # We can't go through Python datetime because it doesn't support nanoseconds
    assert pa.array(arr) == pa.array(data)


def test_second_duration_from_numpy():
    seconds = [timedelta(seconds=1), timedelta(seconds=2)]
    data = np.array(seconds, dtype="timedelta64[s]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("s")
    assert arr[0].as_py() == seconds[0]
    assert arr[1].as_py() == seconds[1]


def test_second_duration_from_numpy_non_contiguous():
    seconds = [timedelta(seconds=1), timedelta(seconds=2), timedelta(seconds=3)]
    data = np.array(seconds, dtype="timedelta64[s]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("s")
    assert arr[0].as_py() == seconds[0]
    assert arr[1].as_py() == seconds[2]


def test_millisecond_duration_from_numpy():
    milliseconds = [
        timedelta(milliseconds=123),
        timedelta(milliseconds=654),
    ]
    data = np.array(milliseconds, dtype="timedelta64[ms]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("ms")
    assert arr[0].as_py() == milliseconds[0]
    assert arr[1].as_py() == milliseconds[1]


def test_millisecond_duration_from_numpy_non_contiguous():
    milliseconds = [
        timedelta(milliseconds=123),
        timedelta(milliseconds=654),
        timedelta(milliseconds=789),
    ]
    data = np.array(milliseconds, dtype="timedelta64[ms]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("ms")
    assert arr[0].as_py() == milliseconds[0]
    assert arr[1].as_py() == milliseconds[2]


def test_microsecond_duration_from_numpy():
    microseconds = [
        timedelta(microseconds=123456),
        timedelta(microseconds=654321),
    ]
    data = np.array(microseconds, dtype="timedelta64[us]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("us")
    assert arr[0].as_py() == microseconds[0]
    assert arr[1].as_py() == microseconds[1]


def test_microsecond_duration_from_numpy_non_contiguous():
    microseconds = [
        timedelta(microseconds=123456),
        timedelta(microseconds=654321),
        timedelta(microseconds=789012),
    ]
    data = np.array(microseconds, dtype="timedelta64[us]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("us")
    assert arr[0].as_py() == microseconds[0]
    assert arr[1].as_py() == microseconds[2]


def test_nanosecond_duration_from_numpy():
    nanoseconds = [
        np.timedelta64(100, "ns"),
        np.timedelta64(200, "ns"),
    ]
    data = np.array(nanoseconds, dtype="timedelta64[ns]")
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("ns")
    # We can't go through Python datetime because it doesn't support nanoseconds


def test_nanosecond_duration_from_numpy_non_contiguous():
    nanoseconds = [
        np.timedelta64(100, "ns"),
        np.timedelta64(200, "ns"),
        np.timedelta64(300, "ns"),
    ]
    data = np.array(nanoseconds, dtype="timedelta64[ns]")[::2]
    arr = Array.from_numpy(data)
    assert arr == Array(pa.array(data)), (
        "Our numpy import should match pyarrow's import."
    )
    assert arr.type == DataType.duration("ns")
    # We can't go through Python datetime because it doesn't support nanoseconds
