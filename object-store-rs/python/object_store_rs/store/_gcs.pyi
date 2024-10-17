from typing import Dict

from ._retry import RetryConfig

class GCSStore:
    """Configure a connection to Google Cloud Storage.

    If no credentials are explicitly provided, they will be sourced from the environment
    as documented
    [here](https://cloud.google.com/docs/authentication/application-default-credentials).
    """

    @classmethod
    def from_env(
        cls,
        bucket: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> GCSStore:
        """Construct a new GCSStore with values pre-populated from environment variables.

        Variables extracted from environment:

        - `GOOGLE_SERVICE_ACCOUNT`: location of service account file
        - `GOOGLE_SERVICE_ACCOUNT_PATH`: (alias) location of service account file
        - `SERVICE_ACCOUNT`: (alias) location of service account file
        - `GOOGLE_SERVICE_ACCOUNT_KEY`: JSON serialized service account key
        - `GOOGLE_BUCKET`: bucket name
        - `GOOGLE_BUCKET_NAME`: (alias) bucket name

        Args:
            bucket: The GCS bucket to use.
            config: GCS Configuration. Values in this config will override values inferred from the environment. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            GCSStore
        """

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        config: Dict[str, str] | None = None,
        client_options: Dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
    ) -> GCSStore:
        """Construct a new GCSStore with values populated from a well-known storage URL.

        The supported url schemes are:

        - `gs://<bucket>/<path>`

        Args:
            url: well-known storage URL.
            config: GCS Configuration. Values in this config will override values inferred from the url. Defaults to None.
            client_options: HTTP Client options. Defaults to None.
            retry_config: Retry configuration. Defaults to None.

        Returns:
            GCSStore
        """
