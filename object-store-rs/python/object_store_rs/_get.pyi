from datetime import datetime
from typing import Sequence, TypedDict

from ._list import ObjectMeta
from ._pyo3_object_store import ObjectStore
from ._sign import HTTP_METHOD as HTTP_METHOD
from ._sign import SignCapableStore as SignCapableStore
from ._sign import sign_url as sign_url
from ._sign import sign_url_async as sign_url_async

class GetOptions(TypedDict):
    """Options for a get request, such as range"""

    if_match: str | None
    """
    Request will succeed if the `ObjectMeta::e_tag` matches
    otherwise returning [`Error::Precondition`]

    See <https://datatracker.ietf.org/doc/html/rfc9110#name-if-match>

    Examples:

    ```text
    If-Match: "xyzzy"
    If-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    If-Match: *
    ```
    """

    if_none_match: str | None
    """
    Request will succeed if the `ObjectMeta::e_tag` does not match
    otherwise returning [`Error::NotModified`]

    See <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.2>

    Examples:

    ```text
    If-None-Match: "xyzzy"
    If-None-Match: "xyzzy", "r2d2xxxx", "c3piozzzz"
    If-None-Match: *
    ```
    """

    if_unmodified_since: datetime | None
    """
    Request will succeed if the object has been modified since

    <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.3>
    """

    if_modified_since: datetime | None
    """
    Request will succeed if the object has not been modified since
    otherwise returning [`Error::Precondition`]

    Some stores, such as S3, will only return `NotModified` for exact
    timestamp matches, instead of for any timestamp greater than or equal.

    <https://datatracker.ietf.org/doc/html/rfc9110#section-13.1.4>
    """

    # range:
    """
    Request transfer of only the specified range of bytes
    otherwise returning [`Error::NotModified`]

    <https://datatracker.ietf.org/doc/html/rfc9110#name-range>
    """

    version: str | None
    """
    Request a particular object version
    """

    head: bool
    """
    Request transfer of no content

    <https://datatracker.ietf.org/doc/html/rfc9110#name-head>
    """

class GetResult:
    def bytes(self) -> bytes:
        """
        Collects the data into bytes
        """

    async def bytes_async(self) -> bytes:
        """
        Collects the data into bytes
        """

    @property
    def meta(self) -> ObjectMeta:
        """The ObjectMeta for this object"""

def get(
    store: ObjectStore, location: str, *, options: GetOptions | None = None
) -> GetResult: ...
async def get_async(
    store: ObjectStore, location: str, *, options: GetOptions | None = None
) -> GetResult: ...
def get_range(store: ObjectStore, location: str, offset: int, length: int) -> bytes: ...
async def get_range_async(
    store: ObjectStore, location: str, offset: int, length: int
) -> bytes: ...
def get_ranges(
    store: ObjectStore, location: str, offset: Sequence[int], length: Sequence[int]
) -> bytes: ...
async def get_ranges_async(
    store: ObjectStore, location: str, offset: Sequence[int], length: Sequence[int]
) -> bytes: ...
