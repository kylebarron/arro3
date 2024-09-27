from arro3.core import Scalar
from arro3.core.types import ArrowArrayExportable, ArrowStreamExportable

def max(input: ArrowArrayExportable | ArrowStreamExportable) -> Scalar:
    """
    Returns the max of values in the array.
    """

def min(input: ArrowArrayExportable | ArrowStreamExportable) -> Scalar:
    """
    Returns the min of values in the array.
    """

def sum(input: ArrowArrayExportable | ArrowStreamExportable) -> Scalar:
    """
    Returns the sum of values in the array.
    """
