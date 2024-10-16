# TODO: move this to a standalone package/docs website that can be shared across
# multiple python packages.

from __future__ import annotations

from datetime import timedelta
from typing import Dict, TypedDict

import boto3
import botocore
import botocore.session

class BackoffConfig(TypedDict):
    init_backoff: timedelta
    max_backoff: timedelta
    base: int | float

class RetryConfig(TypedDict):
    backoff: BackoffConfig
    max_retries: int
    retry_timeout: timedelta

class AzureStore:
    @classmethod
    def from_env(
        cls,
        container: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...

class GCSStore:
    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...

class HTTPStore:
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...

class S3Store:
    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...
    @classmethod
    def from_session(
        cls,
        session: boto3.Session | botocore.session.Session,
        bucket: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> S3Store: ...

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
