from typing import Dict

from ._retry import RetryConfig

class AzureStore:
    @classmethod
    def from_env(
        cls,
        container: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> AzureStore: ...
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> AzureStore: ...
