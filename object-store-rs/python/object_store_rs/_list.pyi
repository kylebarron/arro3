from datetime import datetime
from typing import List, TypedDict

from ._sign import HTTP_METHOD as HTTP_METHOD
from ._sign import SignCapableStore as SignCapableStore
from ._sign import sign_url as sign_url
from ._sign import sign_url_async as sign_url_async
from .store import ObjectStore

class ObjectMeta(TypedDict):
    """The metadata that describes an object."""

    location: str
    """The full path to the object"""

    last_modified: datetime
    """The last modified time"""

    size: int
    """The size in bytes of the object"""

    e_tag: str | None
    """The unique identifier for the object

    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for this object"""

class ListResult(TypedDict):
    """
    Result of a list call that includes objects, prefixes (directories) and a token for
    the next set of results. Individual result sets may be limited to 1,000 objects
    based on the underlying object storage's limitations.
    """

    common_prefixes: List[str]
    """Prefixes that are common (like directories)"""

    objects: List[ObjectMeta]
    """Object metadata for the listing"""

def list(store: ObjectStore, prefix: str | None = None) -> List[ObjectMeta]:
    """
    List all the objects with the given prefix.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. List is recursive, i.e. `foo/bar/more/x`
    will be included.

    Note: the order of returned [`ObjectMeta`][object_store_rs.ObjectMeta] is not
    guaranteed

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Returns:
        A list of `ObjectMeta`.
    """

async def list_async(store: ObjectStore, prefix: str | None = None) -> List[ObjectMeta]:
    """Call `list` asynchronously.

    Refer to the documentation for [list][object_store_rs.list].
    """

def list_with_delimiter(store: ObjectStore, prefix: str | None = None) -> ListResult:
    """
    List objects with the given prefix and an implementation specific
    delimiter. Returns common prefixes (directories) in addition to object
    metadata.

    Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of
    `foo/bar/x` but not of `foo/bar_baz/x`. List is not recursive, i.e. `foo/bar/more/x`
    will not be included.

    Args:
        store: The ObjectStore instance to use.
        prefix: The prefix within ObjectStore to use for listing. Defaults to None.

    Returns:
        ListResult
    """

async def list_with_delimiter_async(
    store: ObjectStore, prefix: str | None = None
) -> ListResult:
    """Call `list_with_delimiter` asynchronously.

    Refer to the documentation for
    [list_with_delimiter][object_store_rs.list_with_delimiter].
    """
