from typing import Literal

class SchemaStore:
    """Manages Avro writer schemas keyed by fingerprint.

    Each SchemaStore instance is configured with a specific fingerprint algorithm
    that determines how schemas are identified and stored.
    """

    @staticmethod
    def rabin() -> SchemaStore:
        """Create a SchemaStore using Rabin fingerprints (for SOE streams).

        Returns:
            A new SchemaStore configured for Rabin fingerprints.
        """

    @staticmethod
    def confluent() -> SchemaStore:
        """Create a SchemaStore using ID fingerprints (for Confluent Schema Registry).

        Returns:
            A new SchemaStore configured for Confluent-style 32-bit IDs.
        """

    @staticmethod
    def apicurio() -> SchemaStore:
        """Create a SchemaStore using 64-bit ID fingerprints (for Apicurio Registry).

        Returns:
            A new SchemaStore configured for Apicurio-style 64-bit IDs.
        """

    def set(self, key: int | str, schema_json: str) -> None:
        """Set a schema with an explicit fingerprint or ID.

        Args:
            key: Fingerprint or ID (int for Rabin/Confluent/Apicurio, or hex string)
            schema_json: Avro schema as a JSON string
        """

    def register(self, schema_json: str) -> int:
        """Register a schema and compute its fingerprint.

        Note: This will error for Id and Id64 algorithms (Confluent/Apicurio)
        because they require explicit IDs. Use set() instead.

        Args:
            schema_json: Avro schema as a JSON string

        Returns:
            The computed fingerprint as an integer
        """

    def lookup(self, key: int | str) -> str | None:
        """Look up a schema by fingerprint or ID.

        Args:
            key: Fingerprint or ID to look up

        Returns:
            Schema JSON string if found, None otherwise
        """

    def fingerprints(self) -> list[int]:
        """Get all fingerprints currently stored.

        Returns:
            List of fingerprints as integers
        """

    @property
    def key_type(self) -> Literal["rabin", "id", "id64"]:
        """Get the fingerprint algorithm type for this store.

        Returns:
            One of "rabin", "id", or "id64"
        """

