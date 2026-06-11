from ._io import *
from ._io import ___version

try:
    # The store module only exists when arro3-io is compiled with the "async"
    # feature; emscripten/pyodide wheels are built without it.
    from ._io import store
except ImportError:
    pass

__version__: str = ___version()
