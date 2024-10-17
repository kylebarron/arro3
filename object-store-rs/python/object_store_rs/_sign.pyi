from datetime import timedelta
from typing import Literal

from ._pyo3_object_store import AzureStore, GCSStore, S3Store

HTTP_METHOD = Literal[
    "GET", "PUT", "POST", "HEAD", "PATCH", "TRACE", "DELETE", "OPTIONS", "CONNECT"
]
SignCapableStore = AzureStore | GCSStore | S3Store

def sign_url(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str: ...
async def sign_url_async(
    store: SignCapableStore, method: HTTP_METHOD, path: str, expires_in: timedelta
) -> str: ...
