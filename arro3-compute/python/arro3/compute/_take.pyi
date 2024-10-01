from arro3.core import Array
from arro3.core.types import ArrayInput

def take(values: ArrayInput, indices: ArrayInput) -> Array:
    """Take elements by index from Array, creating a new Array from those indexes.

    ```
    ┌─────────────────┐      ┌─────────┐                              ┌─────────────────┐
    │        A        │      │    0    │                              │        A        │
    ├─────────────────┤      ├─────────┤                              ├─────────────────┤
    │        D        │      │    2    │                              │        B        │
    ├─────────────────┤      ├─────────┤   take(values, indices)      ├─────────────────┤
    │        B        │      │    3    │ ─────────────────────────▶   │        C        │
    ├─────────────────┤      ├─────────┤                              ├─────────────────┤
    │        C        │      │    1    │                              │        D        │
    ├─────────────────┤      └─────────┘                              └─────────────────┘
    │        E        │
    └─────────────────┘
    values array             indices array                            result
    ```

    Args:
        values: The input Arrow data to select from.
        indices: The indices within `values` to take. This must be a numeric array.

    Returns:
        The selected arrow data.
    """
