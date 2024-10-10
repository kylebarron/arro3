from typing import overload

from arro3.core import Array, ArrayReader
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable

from .enums import DatePart
from .types import DatePartT

# # Examples

# ```
# # use arrow_array::{Int32Array, TimestampMicrosecondArray};
# # use arrow_arith::temporal::{DatePart, date_part};
# let input: TimestampMicrosecondArray =
#     vec![Some(1612025847000000), None, Some(1722015847000000)].into();

# let actual = date_part(&input, DatePart::Week).unwrap();
# let expected: Int32Array = vec![Some(4), None, Some(30)].into();
# assert_eq!(actual.as_ref(), &expected);
# ```

@overload
def date_part(input: ArrowArrayExportable, part: DatePart | DatePartT) -> Array: ...
@overload
def date_part(
    input: ArrowStreamExportable, part: DatePart | DatePartT
) -> ArrayReader: ...
def date_part(
    input: ArrowArrayExportable | ArrowStreamExportable, part: DatePart | DatePartT
) -> Array | ArrayReader:
    """
    Given an array, return a new array with the extracted [`DatePart`] as signed 32-bit
    integer values.

    Currently only supports temporal types:
      - Date32/Date64
      - Time32/Time64
      - Timestamp
      - Interval
      - Duration

    Returns an int32-typed array unless input was a dictionary type, in which case
    returns the dictionary but with this function applied onto its values.

    If array passed in is not of the above listed types (or is a dictionary array where
    the values array isn't of the above listed types), then this function will return an
    error.

    Args:
        array: Argument to compute function.

    Returns:
        The extracted date part.
    """
