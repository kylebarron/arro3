from arro3.core import Array
from arro3.core.types import ArrowArrayExportable

def add(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs + rhs`, returning an error on overflow"""

def add_wrapping(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs + rhs`, wrapping on overflow for integer data types."""

def div(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs / rhs`"""

def mul(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs * rhs`, returning an error on overflow"""

def mul_wrapping(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs * rhs`, wrapping on overflow for integer data types."""

def neg(array: ArrowArrayExportable) -> Array:
    """Negates each element of array, returning an error on overflow"""

def neg_wrapping(array: ArrowArrayExportable) -> Array:
    """Negates each element of array, wrapping on overflow for integer data types."""

def rem(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs % rhs`"""

def sub(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs - rhs`, returning an error on overflow"""

def sub_wrapping(lhs: ArrowArrayExportable, rhs: ArrowArrayExportable) -> Array:
    """Perform `lhs - rhs`, wrapping on overflow for integer data types."""
