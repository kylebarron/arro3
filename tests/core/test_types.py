import contextlib
import os
import pathlib
import subprocess
import sys
from typing import Generator


@contextlib.contextmanager
def change_cwd(path: pathlib.Path) -> Generator[None, None, None]:
    """Helper function to change the current working directory."""

    current_dir = pathlib.Path.cwd()
    os.chdir(path.absolute())
    try:
        yield
    finally:
        os.chdir(current_dir)


def test_import_arro3_core_types_no_dependencies():
    """Ensure that the `arro3.core.types` module can be imported without any runtime dependencies."""

    with change_cwd(
        pathlib.Path(__file__).parent.parent.parent / "arro3-core" / "python"
    ):
        proc = subprocess.run(
            [sys.executable, "-S", "-c", "import arro3.core.types"],
            stderr=subprocess.PIPE,
        )
        assert (
            proc.returncode == 0
        ), f"Process failed with stderr:\n{proc.stderr.decode()}"
