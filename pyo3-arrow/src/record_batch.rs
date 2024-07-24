use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array;
use crate::ffi::to_python::to_array_pycapsules;
use crate::schema::display_schema;
use crate::{PyArray, PyField, PySchema};

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
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
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
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
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

    /// Construct a RecordBatch from multiple Arrays
    #[classmethod]
    #[pyo3(signature = (arrays, *, schema))]
    pub fn from_arrays(
        _cls: &Bound<PyType>,
        arrays: Vec<PyArray>,
        schema: PySchema,
    ) -> PyArrowResult<Self> {
        let rb = RecordBatch::try_new(
            schema.into(),
            arrays
                .into_iter()
                .map(|arr| {
                    let (arr, _field) = arr.into_inner();
                    arr
                })
                .collect(),
        )?;
        Ok(Self::new(rb))
    }

    #[classmethod]
    pub fn from_pydict(
        _cls: &Bound<PyType>,
        mapping: HashMap<String, PyArray>,
    ) -> PyArrowResult<Self> {
        let mut fields = vec![];
        let mut arrays = vec![];
        mapping.into_iter().for_each(|(name, py_array)| {
            let (arr, field) = py_array.into_inner();
            fields.push(field.as_ref().clone().with_name(name));
            arrays.push(arr);
        });
        let schema = Schema::new(fields);
        let rb = RecordBatch::try_new(schema.into(), arrays)?;
        Ok(Self::new(rb))
    }

    /// Construct a RecordBatch from a StructArray.
    ///
    /// Each field in the StructArray will become a column in the resulting RecordBatch.
    #[classmethod]
    pub fn from_struct_array(_cls: &Bound<PyType>, struct_array: PyArray) -> PyArrowResult<Self> {
        let (array, field) = struct_array.into_inner();
        match field.data_type() {
            DataType::Struct(fields) => {
                let schema = Schema::new_with_metadata(fields.clone(), field.metadata().clone());
                let struct_arr = array.as_struct();
                let columns = struct_arr.columns().to_vec();
                let rb = RecordBatch::try_new(schema.into(), columns)?;
                Ok(Self::new(rb))
            }
            _ => Err(PyTypeError::new_err("Expected struct array").into()),
        }
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

    /// Select single column from RecordBatch
    fn column(&self, py: Python, i: usize) -> PyArrowResult<PyObject> {
        let field = self.0.schema().field(i).clone();
        let array = self.0.column(i).clone();
        Ok(PyArray::new(array, field.into()).to_arro3(py)?)
    }

    /// Names of the Table or RecordBatch columns.
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.0
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// List of all columns in numerical order.
    #[getter]
    fn columns(&self, py: Python) -> PyResult<Vec<PyObject>> {
        self.0
            .schema()
            .fields()
            .iter()
            .zip(self.0.columns())
            .map(|(field, array)| PyArray::new(array.clone(), field.clone()).to_arro3(py))
            .collect()
    }

    /// Select a schema field by its numeric index.
    fn field(&self, py: Python, i: usize) -> PyResult<PyObject> {
        PyField::new(self.0.schema().field(i).clone().into()).to_arro3(py)
    }

    /// Number of columns in this RecordBatch.
    #[getter]
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }

    /// Number of rows in this RecordBatch.
    #[getter]
    fn num_rows(&self) -> usize {
        self.0.num_rows()
    }

    /// Access the schema of this RecordBatch
    #[getter]
    fn schema(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(self.0.schema()).to_arro3(py)
    }

    /// Dimensions of the table or record batch: (#rows, #columns).
    #[getter]
    fn shape(&self) -> (usize, usize) {
        (self.num_rows(), self.num_columns())
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, py: Python, offset: usize, length: Option<usize>) -> PyResult<PyObject> {
        let length = length.unwrap_or_else(|| self.num_rows() - offset);
        PyRecordBatch::new(self.0.slice(offset, length)).to_arro3(py)
    }

    fn to_struct_array(&self, py: Python) -> PyArrowResult<PyObject> {
        let struct_array: StructArray = self.0.clone().into();
        let field = Field::new_struct("", self.0.schema_ref().fields().clone(), false);
        Ok(PyArray::new(Arc::new(struct_array), field.into()).to_arro3(py)?)
    }
}
