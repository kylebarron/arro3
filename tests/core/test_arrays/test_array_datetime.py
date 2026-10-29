import zoneinfo
from datetime import datetime, timezone

import pytest
from arro3.core import Array, DataType


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_array_timestamp_timezone(unit):
    """Test that an array with timestamp type can be created with different units."""
    dt = datetime(1999, 8, 7, 11, 12, 13, 141516)
    arr = Array([dt, None], type=DataType.timestamp(unit))

    result: datetime = arr.to_pylist()[0]

    assert result.replace(microsecond=0) == dt.replace(microsecond=0)

    if unit == "s":
        assert result.microsecond == 0

    if unit == "ms":
        assert result.microsecond == 141000

    if unit == "us" or unit == "ns":
        assert result.microsecond == dt.microsecond


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("tz_name", ["UTC", "America/Chicago", "Europe/Madrid"])
def test_array_timestamp_tz(unit, tz_name):
    """Test that an array with timestamp type can be created with different units and timezone."""
    dt = datetime(1999, 8, 7, 11, 12, 13, 141516)

    tzinfo = zoneinfo.ZoneInfo(tz_name)
    expected: datetime = dt.astimezone(timezone(tzinfo.utcoffset(dt)))

    arr = Array([expected, None], type=DataType.timestamp(unit, tz=tz_name))
    result: datetime = arr.to_pylist()[0]

    # compare without microseconds because its more direct.
    assert result.replace(microsecond=0) == expected.replace(microsecond=0)
    assert result.tzinfo.utcoffset(dt) == expected.tzinfo.utcoffset(dt)

    if unit == "s":
        assert result.microsecond == 0

    if unit == "ms":
        assert result.microsecond == 141000

    if unit == "us" or unit == "ns":
        assert result.microsecond == expected.microsecond
