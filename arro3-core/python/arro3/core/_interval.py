from typing import NamedTuple


class MonthDayNano(NamedTuple):
    """A month, day, and nanosecond interval."""

    months: int
    days: int
    nanoseconds: int


class DayTime(NamedTuple):
    """A day and millisecond interval."""

    days: int
    milliseconds: int
