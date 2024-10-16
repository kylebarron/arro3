from datetime import datetime, timedelta
from typing import List, Literal, TypedDict

from ._pyo3_object_store import AzureStore, GCSStore, ObjectStore, S3Store

HTTP_METHOD = Literal[
    "GET", "PUT", "POST", "HEAD", "PATCH", "TRACE", "DELETE", "OPTIONS", "CONNECT"
]
SignCapableStore = AzureStore | GCSStore | S3Store

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

def delete(store: ObjectStore, location: str) -> None: ...
async def delete_async(store: ObjectStore, location: str) -> None: ...
def list(store: ObjectStore, prefix: str | None = None) -> List[ObjectMeta]: ...
async def list_async(
    store: ObjectStore, prefix: str | None = None
) -> List[ObjectMeta]: ...
def sign_url(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str: ...
async def sign_url_async(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str: ...
