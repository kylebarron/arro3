from typing import Dict

from ._client import ClientConfigKey
from ._retry import RetryConfig

class HTTPStore:
    """Configure a connection to a generic HTTP server"""

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        client_options: Dict[ClientConfigKey, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> HTTPStore:
        """Construct a new HTTPStore from a URL

        Args:
            url: The base URL to use for the store.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            HTTPStore
        """
