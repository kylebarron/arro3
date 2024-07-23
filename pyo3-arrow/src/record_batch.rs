use std::fmt::Display;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, SchemaBuilder};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array;
use crate::ffi::to_python::to_array_pycapsules;
use crate::schema::display_schema;

/// A Python-facing Arrow record batch.
///
/// This is a wrapper around a [RecordBatch].
#[pyclass(module = "arro3.core._rust", name = "RecordBatch", subclass)]
#[derive(Debug)]
pub struct PyRecordBatch(RecordBatch);

impl PyRecordBatch {
    pub fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }

    pub fn into_inner(self) -> RecordBatch {
        self.0
    }

    /// Export this to a Python `arro3.core.RecordBatch`.
    pub fn to_arro3(&self, py: Python) -> PyArrowResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod
            .getattr(intern!(py, "RecordBatch"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                self.__arrow_c_array__(py, None)?,
            )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.Array`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array(py, &self.__arrow_c_array__(py, None)?)
    }

    /// Export to a pyarrow.RecordBatch
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyArrowResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "record_batch"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self(value)
    }
}

impl From<PyRecordBatch> for RecordBatch {
    fn from(value: PyRecordBatch) -> Self {
        value.0
    }
}

impl AsRef<RecordBatch> for PyRecordBatch {
    fn as_ref(&self) -> &RecordBatch {
        &self.0
    }
}

impl Display for PyRecordBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.RecordBatch")?;
        writeln!(f, "-----------------")?;
        display_schema(&self.0.schema(), f)
    }
}

#[pymethods]
impl PyRecordBatch {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyTuple>> {
        let field = Field::new_struct("", self.0.schema_ref().fields().clone(), false);
        let array: ArrayRef = Arc::new(StructArray::from(self.0.clone()));
        to_array_pycapsules(py, field.into(), &array, requested_schema)

        // let schema = self.0.schema();
        // let array = StructArray::from(self.0.clone());

        // let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
        // let ffi_array = FFI_ArrowArray::new(&array.to_data());

        // let schema_capsule_name = CString::new("arrow_schema").unwrap();
        // let array_capsule_name = CString::new("arrow_array").unwrap();
        // let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
        // let array_capsule = PyCapsule::new_bound(py, ffi_array, Some(array_capsule_name))?;
        // Ok(PyTuple::new_bound(py, vec![schema_capsule, array_capsule]))
    }

    pub fn __eq__(&self, other: &PyRecordBatch) -> bool {
        self.0 == other.0
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this from an existing Arrow RecordBatch.
    ///
    /// It can be called on anything that exports the Arrow data interface
    /// (`__arrow_c_array__`) and returns a StructArray..
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let (array, field) = import_array_pycapsules(schema_capsule, array_capsule)?;
        match field.data_type() {
            DataType::Struct(fields) => {
                let struct_array = array.as_struct();
                let schema = SchemaBuilder::from(fields)
                    .finish()
                    .with_metadata(field.metadata().clone());
                assert_eq!(
                    struct_array.null_count(),
                    0,
                    "Cannot convert nullable StructArray to RecordBatch"
                );

                let columns = struct_array.columns().to_vec();
                let batch = RecordBatch::try_new(Arc::new(schema), columns)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                Ok(Self::new(batch))
            }
            dt => Err(PyValueError::new_err(format!(
                "Unexpected data type {}",
                dt
            ))),
        }
    }
}
