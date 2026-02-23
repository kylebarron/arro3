import sys

if sys.version_info >= (3, 12):
    from collections.abc import Buffer as _Buffer
else:
    from typing_extensions import Buffer as _Buffer

class Buffer(_Buffer):
    """An Arrow Buffer"""
    def __init__(self, buffer) -> None: ...
    def __buffer__(self, flags: int) -> memoryview: ...
    def __len__(self) -> int: ...
    def to_bytes(self) -> bytes:
        """Copy this buffer into a Python `bytes` object."""
