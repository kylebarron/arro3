use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

#[cfg(feature = "avro")]
use arrow_avro::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore as AvroSchemaStore,
};

#[cfg(feature = "avro")]
#[derive(Clone, Copy)]
pub(crate) enum AlgorithmType {
    Rabin,
    Id,
    Id64,
}

#[cfg(feature = "avro")]
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
#[cfg(feature = "avro")]
#[pyclass(module = "arro3.io._io", name = "SchemaStore")]
pub struct PySchemaStore {
    inner: AvroSchemaStore,
    algorithm: AlgorithmType,
}

#[cfg(feature = "avro")]
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
        let fingerprint = coerce_to_fingerprint(self.algorithm, key)?;
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
    pub fn lookup(&mut self, key: &Bound<PyAny>) -> PyResult<Option<String>> {
        let fingerprint = coerce_to_fingerprint(self.algorithm, key)?;
        Ok(self
            .inner
            .lookup(&fingerprint)
            .map(|s| s.json_string.clone()))
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

/// Convert Python object (int | str) to Fingerprint based on the algorithm
#[cfg(feature = "avro")]
pub(crate) fn coerce_to_fingerprint(
    alg: AlgorithmType,
    obj: &Bound<PyAny>,
) -> PyResult<Fingerprint> {
    // Extract the numeric value from Python regardless of algorithm type.
    // All fingerprint types (Rabin, Id, Id64) use the same parsing logic:
    // - Try direct extraction as u64 (handles Python int)
    // - If that fails, try extracting as string and parse it
    //   - hex strings with "0x" prefix (e.g., "0x1234abcd")
    //   - decimal strings (e.g., "12345")
    let val = obj
        .extract::<u64>()
        .or_else(|_| {
            obj.extract::<String>().and_then(|s| {
                (if let Some(hex) = s.strip_prefix("0x") {
                    u64::from_str_radix(hex, 16)
                } else {
                    s.parse::<u64>()
                })
                .map_err(|_| PyValueError::new_err(format!("Cannot parse '{}' as integer", s)))
            })
        })
        .map_err(|_| PyTypeError::new_err("Expected int or string for fingerprint"))?;
    match alg {
        AlgorithmType::Rabin => Ok(Fingerprint::Rabin(val)),
        AlgorithmType::Id => {
            let id = u32::try_from(val).map_err(|_| PyValueError::new_err("ID must fit in u32"))?;
            Ok(Fingerprint::Id(id))
        }
        AlgorithmType::Id64 => Ok(Fingerprint::Id64(val)),
    }
}
