from __future__ import annotations

from typing import Literal

DatePartT = Literal[
    "quarter",
    "year",
    "month",
    "week",
    "day",
    "dayofweeksunday0",
    "dayofweekmonday0",
    "dayofyear",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
]
"""
Acceptable strings to be passed into the `part` parameter for
[`date_part`][arro3.compute.date_part].
"""
