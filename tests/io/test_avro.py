import pytest
from arro3.io import SchemaStore


def test_schema_store_rabin():
    """Test SchemaStore with Rabin fingerprint algorithm."""
    store = SchemaStore.rabin()
    assert store.key_type == "rabin"

    schema_json = (
        '{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}'
    )
    fp = store.register(schema_json)
    assert isinstance(fp, int)
    assert fp > 0

    # Lookup should work
    retrieved = store.lookup(fp)
    assert retrieved == schema_json

    # List fingerprints
    fps = store.fingerprints()
    assert fp in fps


def test_schema_store_confluent():
    """Test SchemaStore with Confluent (Id) algorithm."""
    store = SchemaStore.confluent()
    assert store.key_type == "id"

    schema_json = (
        '{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}'
    )
    schema_id = 42
    store.set(schema_id, schema_json)

    # Lookup should work
    retrieved = store.lookup(schema_id)
    assert retrieved == schema_json

    # List fingerprints
    fps = store.fingerprints()
    assert schema_id in fps


def test_schema_store_apicurio():
    """Test SchemaStore with Apicurio (Id64) algorithm."""
    store = SchemaStore.apicurio()
    assert store.key_type == "id64"

    schema_json = (
        '{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}'
    )
    schema_id = 123456789
    store.set(schema_id, schema_json)

    # Lookup should work
    retrieved = store.lookup(schema_id)
    assert retrieved == schema_json

    # List fingerprints
    fps = store.fingerprints()
    assert schema_id in fps


def test_schema_store_lookup_missing():
    """Test lookup of non-existent schema."""
    store = SchemaStore.rabin()
    result = store.lookup(999999)
    assert result is None


def test_schema_store_confluent_register_fails():
    """Test that register() fails for Confluent (requires explicit IDs)."""
    store = SchemaStore.confluent()
    schema_json = '{"type":"record","name":"Test","fields":[]}'
    with pytest.raises(Exception):  # Should fail because Id requires explicit set()
        store.register(schema_json)
