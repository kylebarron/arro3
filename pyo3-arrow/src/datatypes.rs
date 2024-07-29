use std::fmt::Display;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_schema::{Field, IntervalUnit, TimeUnit};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_schema_pycapsule;
use crate::PyField;

struct PyTimeUnit(arrow_schema::TimeUnit);

impl<'a> FromPyObject<'a> for PyTimeUnit {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "s" => Ok(Self(TimeUnit::Second)),
            "ms" => Ok(Self(TimeUnit::Millisecond)),
            "us" => Ok(Self(TimeUnit::Microsecond)),
            "ns" => Ok(Self(TimeUnit::Nanosecond)),
            _ => Err(PyValueError::new_err("Unexpected time unit")),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
#[pyclass(module = "arro3.core._core", name = "DataType", subclass)]
pub struct PyDataType(DataType);

impl PyDataType {
    pub fn new(data_type: DataType) -> Self {
        Self(data_type)
    }

    pub fn into_inner(self) -> DataType {
        self.0
    }

    /// Export this to a Python `arro3.core.Field`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "DataType"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_schema__(py)?]),
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.Field
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_field = pyarrow_mod
            .getattr(intern!(py, "field"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_field.getattr(intern!(py, "type"))?.to_object(py))
    }
}

impl From<PyDataType> for DataType {
    fn from(value: PyDataType) -> Self {
        value.0
    }
}

impl From<DataType> for PyDataType {
    fn from(value: DataType) -> Self {
        Self(value)
    }
}

impl From<&DataType> for PyDataType {
    fn from(value: &DataType) -> Self {
        Self(value.clone())
    }
}

impl AsRef<DataType> for PyDataType {
    fn as_ref(&self) -> &DataType {
        &self.0
    }
}

impl Display for PyDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.DataType<")?;
        self.0.fmt(f)?;
        writeln!(f, ">")?;
        Ok(())
    }
}

#[pymethods]
impl PyDataType {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.field()`][pyarrow.field] to convert this array
    /// into a pyarrow field, without copying memory.
    pub fn __arrow_c_schema__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, &self.0)
    }

    pub fn __eq__(&self, other: &PyDataType) -> bool {
        self.0 == other.0
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow schema interface
    /// (`__arrow_c_schema__`).
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let data_type =
            DataType::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(data_type))
    }

    pub fn bit_width(&self) -> Option<usize> {
        self.0.primitive_width()
    }

    #[classmethod]
    fn null(_: &Bound<PyType>) -> Self {
        Self(DataType::Null)
    }

    #[classmethod]
    fn bool(_: &Bound<PyType>) -> Self {
        Self(DataType::Boolean)
    }

    #[classmethod]
    fn int8(_: &Bound<PyType>) -> Self {
        Self(DataType::Int8)
    }

    #[classmethod]
    fn int16(_: &Bound<PyType>) -> Self {
        Self(DataType::Int16)
    }

    #[classmethod]
    fn int32(_: &Bound<PyType>) -> Self {
        Self(DataType::Int32)
    }

    #[classmethod]
    fn int64(_: &Bound<PyType>) -> Self {
        Self(DataType::Int64)
    }

    #[classmethod]
    fn uint8(_: &Bound<PyType>) -> Self {
        Self(DataType::UInt8)
    }

    #[classmethod]
    fn uint16(_: &Bound<PyType>) -> Self {
        Self(DataType::UInt16)
    }

    #[classmethod]
    fn uint32(_: &Bound<PyType>) -> Self {
        Self(DataType::UInt32)
    }

    #[classmethod]
    fn uint64(_: &Bound<PyType>) -> Self {
        Self(DataType::UInt64)
    }

    #[classmethod]
    fn float16(_: &Bound<PyType>) -> Self {
        Self(DataType::Float16)
    }

    #[classmethod]
    fn float32(_: &Bound<PyType>) -> Self {
        Self(DataType::Float32)
    }

    #[classmethod]
    fn float64(_: &Bound<PyType>) -> Self {
        Self(DataType::Float64)
    }

    #[classmethod]
    fn time32(_: &Bound<PyType>, unit: PyTimeUnit) -> PyArrowResult<Self> {
        if unit.0 == TimeUnit::Microsecond || unit.0 == TimeUnit::Nanosecond {
            return Err(PyValueError::new_err("Unexpected timeunit for time32").into());
        }

        Ok(Self(DataType::Time32(unit.0)))
    }

    #[classmethod]
    fn time64(_: &Bound<PyType>, unit: PyTimeUnit) -> PyArrowResult<Self> {
        if unit.0 == TimeUnit::Second || unit.0 == TimeUnit::Millisecond {
            return Err(PyValueError::new_err("Unexpected timeunit for time64").into());
        }

        Ok(Self(DataType::Time64(unit.0)))
    }

    #[classmethod]
    #[pyo3(signature = (unit, *, tz=None))]
    fn timestamp(_: &Bound<PyType>, unit: PyTimeUnit, tz: Option<String>) -> Self {
        Self(DataType::Timestamp(unit.0, tz.map(|s| s.into())))
    }

    #[classmethod]
    fn date32(_: &Bound<PyType>) -> Self {
        Self(DataType::Date32)
    }

    #[classmethod]
    fn date64(_: &Bound<PyType>) -> Self {
        Self(DataType::Date64)
    }

    #[classmethod]
    fn duration(_: &Bound<PyType>, unit: PyTimeUnit) -> Self {
        Self(DataType::Duration(unit.0))
    }

    #[classmethod]
    fn month_day_nano_interval(_: &Bound<PyType>) -> Self {
        Self(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    #[classmethod]
    fn binary(_: &Bound<PyType>, length: Option<i32>) -> Self {
        if let Some(length) = length {
            Self(DataType::FixedSizeBinary(length))
        } else {
            Self(DataType::Binary)
        }
    }

    #[classmethod]
    fn string(_: &Bound<PyType>) -> Self {
        Self(DataType::Utf8)
    }

    #[classmethod]
    fn utf8(_: &Bound<PyType>) -> Self {
        Self(DataType::Utf8)
    }

    #[classmethod]
    fn large_binary(_: &Bound<PyType>) -> Self {
        Self(DataType::LargeBinary)
    }

    #[classmethod]
    fn large_string(_: &Bound<PyType>) -> Self {
        Self(DataType::LargeUtf8)
    }

    #[classmethod]
    fn large_utf8(_: &Bound<PyType>) -> Self {
        Self(DataType::LargeUtf8)
    }

    #[classmethod]
    fn binary_view(_: &Bound<PyType>) -> Self {
        Self(DataType::BinaryView)
    }

    #[classmethod]
    fn string_view(_: &Bound<PyType>) -> Self {
        Self(DataType::Utf8View)
    }

    #[classmethod]
    fn decimal128(_: &Bound<PyType>, precision: u8, scale: i8) -> Self {
        Self(DataType::Decimal128(precision, scale))
    }

    #[classmethod]
    fn decimal256(_: &Bound<PyType>, precision: u8, scale: i8) -> Self {
        Self(DataType::Decimal256(precision, scale))
    }

    #[classmethod]
    fn list(_: &Bound<PyType>, value_type: PyField, list_size: Option<i32>) -> Self {
        if let Some(list_size) = list_size {
            Self(DataType::FixedSizeList(value_type.into(), list_size))
        } else {
            Self(DataType::List(value_type.into()))
        }
    }

    #[classmethod]
    fn large_list(_: &Bound<PyType>, value_type: PyField) -> Self {
        Self(DataType::LargeList(value_type.into()))
    }

    #[classmethod]
    fn list_view(_: &Bound<PyType>, value_type: PyField) -> Self {
        Self(DataType::ListView(value_type.into()))
    }

    #[classmethod]
    fn large_list_view(_: &Bound<PyType>, value_type: PyField) -> Self {
        Self(DataType::LargeListView(value_type.into()))
    }

    #[classmethod]
    fn map(_: &Bound<PyType>, key_type: PyField, item_type: PyField, keys_sorted: bool) -> Self {
        // Note: copied from source of `Field::new_map`
        // https://github.com/apache/arrow-rs/blob/bf9ce475df82d362631099d491d3454d64d50217/arrow-schema/src/field.rs#L251-L258
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(vec![key_type.into_inner(), item_type.into_inner()].into()),
                false, // The inner map field is always non-nullable (arrow-rs #1697),
            )),
            keys_sorted,
        );
        Self(data_type)
    }

    #[classmethod]
    fn r#struct(_: &Bound<PyType>, fields: Vec<PyField>) -> Self {
        Self(DataType::Struct(
            fields.into_iter().map(|field| field.into_inner()).collect(),
        ))
    }

    #[classmethod]
    fn dictionary(_: &Bound<PyType>, index_type: PyField, value_type: PyField) -> Self {
        Self(DataType::Dictionary(
            Box::new(index_type.into_inner().data_type().clone()),
            Box::new(value_type.into_inner().data_type().clone()),
        ))
    }

    #[classmethod]
    fn run_end_encoded(_: &Bound<PyType>, run_end_type: PyField, value_type: PyField) -> Self {
        Self(DataType::RunEndEncoded(
            run_end_type.into_inner(),
            value_type.into_inner(),
        ))
    }
}
