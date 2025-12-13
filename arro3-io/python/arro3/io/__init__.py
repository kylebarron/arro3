from ._io import *
from ._io import ___version, store

try:
    from ._io import SchemaStore

    __all_avro__ = [
        "SchemaStore",
    ]
except ImportError:
    __all_avro__ = []
__version__: str = ___version()
