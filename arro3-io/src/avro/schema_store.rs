use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use arrow_avro::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore as AvroSchemaStore,
};

#[derive(Clone, Copy)]
pub(crate) enum AlgorithmType {
    Rabin,
    Id,
    Id64,
}

impl AlgorithmType {
    /// Convert Python object (bytes | int | str) to Fingerprint based on this algorithm.
    pub(crate) fn coerce_to_fingerprint(&self, obj: &Bound<PyAny>) -> PyResult<Fingerprint> {
        match self {
            AlgorithmType::Rabin => {
                self.coerce_value(obj, |bytes| Fingerprint::Rabin(u64::from_le_bytes(bytes)))
            }
            AlgorithmType::Id => {
                self.coerce_value(obj, |bytes| Fingerprint::Id(u32::from_be_bytes(bytes)))
            }
            AlgorithmType::Id64 => {
                self.coerce_value(obj, |bytes| Fingerprint::Id64(u64::from_be_bytes(bytes)))
            }
        }
    }

    fn coerce_value<const N: usize>(
        &self,
        obj: &Bound<PyAny>,
        fingerprint_from: impl FnOnce([u8; N]) -> Fingerprint,
    ) -> PyResult<Fingerprint> {
        if let Ok(bytes) = obj.extract::<[u8; N]>() {
            return Ok(fingerprint_from(bytes));
        }

        let value: u64 = if let Ok(int_val) = obj.extract::<u64>() {
            int_val
        } else if let Ok(s) = obj.extract::<std::borrow::Cow<str>>() {
            s.strip_prefix("0x")
                .map_or_else(|| s.parse::<u64>(), |hex| u64::from_str_radix(hex, 16))
                .map_err(|_| PyValueError::new_err(format!("Cannot parse '{s}' as integer")))?
        } else {
            return Err(PyTypeError::new_err(
                "Expected int, string, or bytes for fingerprint",
            ));
        };

        if N < 8 && value >= (1u64 << (N * 8)) {
            return Err(PyValueError::new_err(format!(
                "Value {value} is too large to fit in a {N}-byte fingerprint"
            )));
        }

        let bytes: [u8; N] = match self {
            AlgorithmType::Rabin => value.to_le_bytes()[..N].try_into()?,
            AlgorithmType::Id | AlgorithmType::Id64 => value.to_be_bytes()[8 - N..].try_into()?,
        };

        Ok(fingerprint_from(bytes))
    }
}

impl From<AlgorithmType> for FingerprintAlgorithm {
    fn from(val: AlgorithmType) -> Self {
        match val {
            AlgorithmType::Rabin => FingerprintAlgorithm::Rabin,
            AlgorithmType::Id => FingerprintAlgorithm::Id,
            AlgorithmType::Id64 => FingerprintAlgorithm::Id64,
        }
    }
}

/// Python wrapper for arrow-avro's SchemaStore
///
/// SchemaStore manages Avro writer schemas keyed by fingerprint.
/// Each instance is configured with a FingerprintAlgorithm that determines
/// how fingerprints are computed.
#[pyclass(module = "arro3.io._io", name = "SchemaStore")]
pub struct PySchemaStore {
    inner: AvroSchemaStore,
    algorithm: AlgorithmType,
}

#[pymethods]
impl PySchemaStore {
    /// Create a SchemaStore using Confluent fingerprints
    #[staticmethod]
    pub fn confluent() -> Self {
        Self {
            inner: AvroSchemaStore::new_with_type(FingerprintAlgorithm::Id),
            algorithm: AlgorithmType::Id,
        }
    }

    /// Create a SchemaStore using Apicurio fingerprints
    #[staticmethod]
    pub fn apicurio() -> Self {
        Self {
            inner: AvroSchemaStore::new_with_type(FingerprintAlgorithm::Id64),
            algorithm: AlgorithmType::Id64,
        }
    }

    /// Create a SchemaStore using Rabin fingerprints
    #[staticmethod]
    pub fn rabin() -> Self {
        Self {
            inner: AvroSchemaStore::new(),
            algorithm: AlgorithmType::Rabin,
        }
    }

    /// Set a schema with an explicit fingerprint/ID
    ///
    /// Args:
    ///     key: Fingerprint or ID (int for Rabin/Confluent/Apicurio)
    ///     schema_json: Avro schema as JSON string
    pub fn set(&mut self, key: &Bound<PyAny>, schema_json: &str) -> PyResult<()> {
        let fingerprint = self.algorithm.coerce_to_fingerprint(key)?;
        self.inner
            .set(fingerprint, AvroSchema::new(schema_json.to_string()))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Register a schema and compute its fingerprint
    /// use .set() for Confluent & Apicurio as they require
    /// explicitly set IDs.
    ///
    /// Args:
    ///     schema_json: Avro schema as JSON string
    ///
    /// Returns:
    ///     The computed fingerprint (int depending on algorithm)
    pub fn register(&mut self, schema_json: &str) -> PyResult<u64> {
        let schema = AvroSchema::new(schema_json.to_string());
        let fingerprint = self
            .inner
            .register(schema)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        match fingerprint {
            Fingerprint::Rabin(val) => Ok(val),
            Fingerprint::Id(val) => Ok(val as u64),
            Fingerprint::Id64(val) => Ok(val),
        }
    }

    /// Look up a schema by fingerprint
    ///
    /// Args:
    ///     key: Fingerprint or ID to look up
    ///
    /// Returns:
    ///     Schema JSON string if found
    pub fn lookup(&self, key: &Bound<PyAny>) -> PyResult<Option<&str>> {
        let fingerprint = self.algorithm.coerce_to_fingerprint(key)?;
        Ok(self
            .inner
            .lookup(&fingerprint)
            .map(|s| s.json_string.as_str()))
    }

    /// Get all fingerprints currently in the store
    ///
    /// Returns:
    ///     List of fingerprints (int depending on algorithm)
    pub fn fingerprints(&self) -> PyResult<Vec<u64>> {
        Ok(self
            .inner
            .fingerprints()
            .into_iter()
            .map(|fp| match fp {
                Fingerprint::Rabin(val) => val,
                Fingerprint::Id(val) => val as u64,
                Fingerprint::Id64(val) => val,
            })
            .collect())
    }

    /// Get the fingerprint algorithm type for this store
    #[getter]
    pub fn key_type(&self) -> &'static str {
        match self.algorithm {
            AlgorithmType::Rabin => "rabin",
            AlgorithmType::Id => "id",
            AlgorithmType::Id64 => "id64",
        }
    }
}
