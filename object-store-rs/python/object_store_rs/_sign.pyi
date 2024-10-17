from datetime import timedelta
from typing import Literal

from .store import AzureStore, GCSStore, S3Store

HTTP_METHOD = Literal[
    "GET", "PUT", "POST", "HEAD", "PATCH", "TRACE", "DELETE", "OPTIONS", "CONNECT"
]
"""Allowed HTTP Methods for signing."""

SignCapableStore = AzureStore | GCSStore | S3Store
"""ObjectStore instances that are capable of signing."""

def sign_url(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str:
    """Create a signed URL.

    Given the intended [`Method`] and [`Path`] to use and the desired length of time for
    which the URL should be valid, return a signed [`Url`] created with the object store
    implementation's credentials such that the URL can be handed to something that
    doesn't have access to the object store's credentials, to allow limited access to
    the object store.

    Args:
        store: The ObjectStore instance to use.
        method: The HTTP method to use.
        path: The path within ObjectStore to retrieve.
        expires_in: How long the signed URL should be valid.

    Returns:
        _description_
    """

async def sign_url_async(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str:
    """Call `sign_url` asynchronously.

    Refer to the documentation for [sign_url][object_store_rs.sign_url].
    """
