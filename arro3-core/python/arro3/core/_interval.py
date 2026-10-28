from typing import NamedTuple


class MonthDayNano(NamedTuple):
    """A month, day, and nanosecond interval, matching `pyarrow.MonthDayNano`.

    It compares equal to a plain `(months, days, nanoseconds)` tuple.
    """

    months: int
    days: int
    nanoseconds: int
