from datetime import datetime
from typing import List, Sequence, TypedDict

from ._list import ObjectMeta
from ._sign import HTTP_METHOD as HTTP_METHOD
from ._sign import SignCapableStore as SignCapableStore
from ._sign import sign_url as sign_url
from ._sign import sign_url_async as sign_url_async
from .store import ObjectStore

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
    """Result for a get request"""

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
) -> GetResult:
    """Return the bytes that are stored at the specified location.

    Args:
        store: The ObjectStore instance to use.
        location: The path within ObjectStore to retrieve.
        options: options for accessing the file. Defaults to None.

    Returns:
        GetResult
    """

async def get_async(
    store: ObjectStore, location: str, *, options: GetOptions | None = None
) -> GetResult:
    """Call `get` asynchronously.

    Refer to the documentation for [get][object_store_rs.get].
    """

def get_range(store: ObjectStore, location: str, offset: int, length: int) -> bytes:
    """
    Return the bytes that are stored at the specified location in the given byte range.

    If the given range is zero-length or starts after the end of the object, an error
    will be returned. Additionally, if the range ends after the end of the object, the
    entire remainder of the object will be returned. Otherwise, the exact requested
    range will be returned.

    Args:
        store: The ObjectStore instance to use.
        location: The path within ObjectStore to retrieve.
        offset: The start of the byte range.
        length: The number of bytes.

    Returns:
        bytes
    """

async def get_range_async(
    store: ObjectStore, location: str, offset: int, length: int
) -> bytes:
    """Call `get_range` asynchronously.

    Refer to the documentation for [get_range][object_store_rs.get_range].
    """

def get_ranges(
    store: ObjectStore, location: str, offsets: Sequence[int], lengths: Sequence[int]
) -> List[bytes]:
    """
    Return the bytes that are stored at the specified locationin the given byte ranges

    To improve performance this will:

    - Combine ranges less than 10MB apart into a single call to `fetch`
    - Make multiple `fetch` requests in parallel (up to maximum of 10)

    Args:
        store: The ObjectStore instance to use.
        location: The path within ObjectStore to retrieve.
        offsets: A sequence of `int` where each offset starts.
        lengths: A sequence of `int` representing the number of bytes within each range.

    Returns:
        A sequence of `bytes`, one for each range.
    """

async def get_ranges_async(
    store: ObjectStore, location: str, offsets: Sequence[int], lengths: Sequence[int]
) -> List[bytes]:
    """Call `get_ranges` asynchronously.

    Refer to the documentation for [get_ranges][object_store_rs.get_ranges].
    """
