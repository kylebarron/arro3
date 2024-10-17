from datetime import datetime
from typing import List, TypedDict

from ._pyo3_object_store import ObjectStore
from ._sign import HTTP_METHOD as HTTP_METHOD
from ._sign import SignCapableStore as SignCapableStore
from ._sign import sign_url as sign_url
from ._sign import sign_url_async as sign_url_async

class ObjectMeta(TypedDict):
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
    common_prefixes: List[str]
    """Prefixes that are common (like directories)"""

    objects: List[ObjectMeta]
    """Object metadata for the listing"""

def list(store: ObjectStore, prefix: str | None = None) -> List[ObjectMeta]: ...
async def list_async(
    store: ObjectStore, prefix: str | None = None
) -> List[ObjectMeta]: ...
def list_with_delimiter(
    store: ObjectStore, prefix: str | None = None
) -> ListResult: ...
async def list_with_delimiter_async(
    store: ObjectStore, prefix: str | None = None
) -> ListResult: ...
