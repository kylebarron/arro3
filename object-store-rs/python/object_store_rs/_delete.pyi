from ._pyo3_object_store import ObjectStore

def delete(store: ObjectStore, location: str) -> None:
    """Delete the object at the specified location.

    Args:
        store: The ObjectStore instance to use.
        location: The path within ObjectStore to delete.
    """

async def delete_async(store: ObjectStore, location: str) -> None:
    """Call `delete` asynchronously.

    Refer to the documentation for [delete][object_store_rs.delete].
    """
