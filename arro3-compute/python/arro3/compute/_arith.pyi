# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core
import arro3.core.types as types

def add(lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable) -> core.Array:
    """Perform `lhs + rhs`, returning an error on overflow"""

def add_wrapping(
    lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable
) -> core.Array:
    """Perform `lhs + rhs`, wrapping on overflow for DataType::is_integer"""

def div(lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable) -> core.Array:
    """Perform `lhs / rhs`"""

def mul(lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable) -> core.Array:
    """Perform `lhs * rhs`, returning an error on overflow"""

def mul_wrapping(
    lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable
) -> core.Array:
    """Perform `lhs * rhs`, wrapping on overflow for DataType::is_integer"""

def neg(array: types.ArrowArrayExportable) -> core.Array:
    """Negates each element of array, returning an error on overflow"""

def neg_wrapping(array: types.ArrowArrayExportable) -> core.Array:
    """Negates each element of array, wrapping on overflow for DataType::is_integer"""

def rem(lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable) -> core.Array:
    """Perform `lhs % rhs`"""

def sub(lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable) -> core.Array:
    """Perform `lhs - rhs`, returning an error on overflow"""

def sub_wrapping(
    lhs: types.ArrowArrayExportable, rhs: types.ArrowArrayExportable
) -> core.Array:
    """Perform `lhs - rhs`, wrapping on overflow for DataType::is_integer"""
