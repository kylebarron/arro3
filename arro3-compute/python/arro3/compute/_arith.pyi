from arro3.core import Array
from arro3.core.types import ArrayInput

def add(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs + rhs`, returning an error on overflow"""

def add_wrapping(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs + rhs`, wrapping on overflow for integer data types."""

def div(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs / rhs`"""

def mul(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs * rhs`, returning an error on overflow"""

def mul_wrapping(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs * rhs`, wrapping on overflow for integer data types."""

def neg(array: ArrayInput) -> Array:
    """Negates each element of array, returning an error on overflow"""

def neg_wrapping(array: ArrayInput) -> Array:
    """Negates each element of array, wrapping on overflow for integer data types."""

def rem(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs % rhs`"""

def sub(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs - rhs`, returning an error on overflow"""

def sub_wrapping(lhs: ArrayInput, rhs: ArrayInput) -> Array:
    """Perform `lhs - rhs`, wrapping on overflow for integer data types."""
