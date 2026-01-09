"""Tests for Avro SchemaStore with different fingerprint algorithms."""

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


def test_schema_store_apicurio_register_fails():
    """Test that register() fails for Apicurio (requires explicit IDs)."""
    store = SchemaStore.apicurio()
    schema_json = '{"type":"record","name":"Test","fields":[]}'
    with pytest.raises(Exception):  # Should fail because Id64 requires explicit set()
        store.register(schema_json)


def test_schema_store_rabin_multiple_schemas():
    """Test storing and retrieving multiple schemas with Rabin."""
    store = SchemaStore.rabin()

    schema1 = '{"type":"record","name":"Schema1","fields":[{"name":"a","type":"int"}]}'
    schema2 = (
        '{"type":"record","name":"Schema2","fields":[{"name":"b","type":"string"}]}'
    )

    fp1 = store.register(schema1)
    fp2 = store.register(schema2)

    # Fingerprints should be different
    assert fp1 != fp2

    # Both should be retrievable
    assert store.lookup(fp1) == schema1
    assert store.lookup(fp2) == schema2

    # Both should be in fingerprints list
    fps = store.fingerprints()
    assert fp1 in fps
    assert fp2 in fps
    assert len(fps) == 2


def test_schema_store_confluent_boundary_values():
    """Test Confluent SchemaStore with boundary ID values."""
    store = SchemaStore.confluent()

    schema_json = '{"type":"record","name":"Test","fields":[]}'

    # Test with 0
    store.set(0, schema_json)
    assert store.lookup(0) == schema_json

    # Test with max u32 value
    max_u32 = 4294967295
    store.set(max_u32, schema_json)
    assert store.lookup(max_u32) == schema_json


def test_schema_store_apicurio_boundary_values():
    """Test Apicurio SchemaStore with boundary ID values."""
    store = SchemaStore.apicurio()

    schema_json = '{"type":"record","name":"Test","fields":[]}'

    # Test with 0
    store.set(0, schema_json)
    assert store.lookup(0) == schema_json

    # Test with large u64 value
    large_id = 9223372036854775807
    store.set(large_id, schema_json)
    assert store.lookup(large_id) == schema_json


def test_schema_store_hex_string_keys():
    """Test that SchemaStore accepts hex string keys."""
    store = SchemaStore.rabin()

    schema_json = '{"type":"record","name":"Test","fields":[]}'

    # First register to get a fingerprint
    fp = store.register(schema_json)

    # Look it up using hex string representation
    hex_key = f"0x{fp:x}"
    retrieved = store.lookup(hex_key)
    assert retrieved == schema_json
