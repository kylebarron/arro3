from typing import Dict

from ._retry import RetryConfig

class HTTPStore:
    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> HTTPStore: ...
