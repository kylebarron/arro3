import subprocess
import sys


def test_numpy_backed_array_to_pyarrow():
    # Passing a numpy-backed `arro3.core.Array` to `pyarrow.Array`
    # caused a segfault at interpreter shutdown.
    # Affected versions: 0.4.0, 0.4.1
    # See: [#230](https://github.com/kylebarron/arro3/issues/230)
    code = (
        "import numpy as np\n"
        "import pyarrow as pa\n"
        "from arro3.core import Array\n"
        "\n"
        "numpy_arr = np.array([0, 1, 2, 3], dtype=np.float64)\n"
        "arro3_arr = Array(numpy_arr)\n"
        "pyarrow_arr = pa.array(arro3_arr)\n"
    )
    subprocess.check_call([sys.executable, "-c", code])
