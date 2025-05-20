# Note: importing with
# `from arro3.core import Array`
# will cause Array to be included in the generated docs in this module.
import arro3.core as core

class RecordBatchStream:
    def __aiter__(self) -> RecordBatchStream:
        """Return `Self` as an async iterator."""
    async def __anext__(self) -> core.RecordBatch:
        """Return the next record batch in the stream."""
    async def collect_async(self) -> core.Table:
        """Collect the stream into a single table."""
