from enum import Enum, auto


class StrEnum(str, Enum):
    def __new__(cls, value, *args, **kwargs):
        if not isinstance(value, (str, auto)):
            raise TypeError(
                f"Values of StrEnums must be strings: {value!r} is a {type(value)}"
            )
        return super().__new__(cls, value, *args, **kwargs)

    def __str__(self):
        return str(self.value)

    def _generate_next_value_(name, *_):
        return name.lower()


class DatePart(StrEnum):
    """Valid parts to extract from date/time/timestamp arrays.

    See [`date_part`][arro3.compute.date_part].
    """

    Quarter = auto()
    """Quarter of the year, in range `1..=4`"""

    Year = auto()
    """Calendar year"""

    Month = auto()
    """Month in the year, in range `1..=12`"""

    Week = auto()
    """ISO week of the year, in range `1..=53`"""

    Day = auto()
    """Day of the month, in range `1..=31`"""

    DayOfWeekSunday0 = auto()
    """Day of the week, in range `0..=6`, where Sunday is `0`"""

    DayOfWeekMonday0 = auto()
    """Day of the week, in range `0..=6`, where Monday is `0`"""

    DayOfYear = auto()
    """Day of year, in range `1..=366`"""

    Hour = auto()
    """Hour of the day, in range `0..=23`"""

    Minute = auto()
    """Minute of the hour, in range `0..=59`"""

    Second = auto()
    """Second of the minute, in range `0..=59`"""

    Millisecond = auto()
    """Millisecond of the second"""

    Microsecond = auto()
    """Microsecond of the second"""

    Nanosecond = auto()
    """Nanosecond of the second"""
