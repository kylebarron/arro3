from arro3.core import Array
from arro3.core.types import ArrowStreamExportable

def concat(input: ArrowStreamExportable) -> Array:
    """Concatenate the chunks of a chunked array into a single Array.

    Args:
        input: The Arrow data to concatenate, such as a ChunkedArray or ArrayReader.

    Returns:
        A single contiguous Array.
    """
