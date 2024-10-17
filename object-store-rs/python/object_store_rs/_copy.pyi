from ._pyo3_object_store import ObjectStore

def copy(store: ObjectStore, from_: str, to: str) -> None:
    """Copy an object from one path to another in the same object store.

    If there exists an object at the destination, it will be overwritten.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path
    """

async def copy_async(store: ObjectStore, from_: str, to: str) -> None:
    """Call `copy` asynchronously.

    Refer to the documentation for [copy][object_store_rs.copy].
    """

def copy_if_not_exists(store: ObjectStore, from_: str, to: str) -> None:
    """
    Copy an object from one path to another, only if destination is empty.

    Will return an error if the destination already has an object.

    Performs an atomic operation if the underlying object storage supports it.
    If atomic operations are not supported by the underlying object storage (like S3)
    it will return an error.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path
    """

async def copy_if_not_exists_async(store: ObjectStore, from_: str, to: str) -> None:
    """Call `copy_if_not_exists` asynchronously.

    Refer to the documentation for
    [copy_if_not_exists][object_store_rs.copy_if_not_exists].
    """
