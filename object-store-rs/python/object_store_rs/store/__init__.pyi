# TODO: move to reusable types package
from ._aws import S3ConfigKey as S3ConfigKey
from ._aws import S3Store as S3Store
from ._azure import AzureStore as AzureStore
from ._client import ClientConfigKey as ClientConfigKey
from ._gcs import GCSStore as GCSStore
from ._http import HTTPStore as HTTPStore
from ._retry import BackoffConfig as BackoffConfig
from ._retry import RetryConfig as RetryConfig

class LocalStore:
    """
    Local filesystem storage providing an ObjectStore interface to files on local disk.
    Can optionally be created with a directory prefix.

    """
    def __init__(self, prefix: str | None = None) -> None: ...

class MemoryStore:
    """A fully in-memory implementation of ObjectStore."""
    def __init__(self) -> None: ...

ObjectStore = AzureStore | GCSStore | HTTPStore | S3Store | LocalStore | MemoryStore
