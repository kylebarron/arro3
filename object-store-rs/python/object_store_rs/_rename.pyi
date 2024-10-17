from ._pyo3_object_store import ObjectStore

def rename(store: ObjectStore, from_: str, to: str) -> None:
    """
    Move an object from one path to another in the same object store.

    By default, this is implemented as a copy and then delete source. It may not check
    when deleting source that it was the same object that was originally copied.

    If there exists an object at the destination, it will be overwritten.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path
    """

async def rename_async(store: ObjectStore, from_: str, to: str) -> None:
    """Call `rename` asynchronously.

    Refer to the documentation for [rename][object_store_rs.rename].
    """

def rename_if_not_exists(store: ObjectStore, from_: str, to: str) -> None:
    """
    Move an object from one path to another in the same object store.

    Will return an error if the destination already has an object.

    Args:
        store: The ObjectStore instance to use.
        from_: Source path
        to: Destination path
    """

async def rename_if_not_exists_async(store: ObjectStore, from_: str, to: str) -> None:
    """Call `rename_if_not_exists` asynchronously.

    Refer to the documentation for
    [rename_if_not_exists][object_store_rs.rename_if_not_exists].
    """
