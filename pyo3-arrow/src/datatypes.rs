use std::fmt::Display;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::export::{Arro3DataType, Arro3Field};
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_schema;
use crate::ffi::to_schema_pycapsule;
use crate::PyField;

struct PyTimeUnit(arrow_schema::TimeUnit);

impl<'a> FromPyObject<'_, 'a> for PyTimeUnit {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'a, PyAny>) -> Result<Self, Self::Error> {
        let s: String = obj.extract()?;
        match s.to_lowercase().as_str() {
            "s" => Ok(Self(TimeUnit::Second)),
            "ms" => Ok(Self(TimeUnit::Millisecond)),
            "us" => Ok(Self(TimeUnit::Microsecond)),
            "ns" => Ok(Self(TimeUnit::Nanosecond)),
            _ => Err(PyValueError::new_err("Unexpected time unit")),
        }
    }
}

/// A Python-facing wrapper around [DataType].
#[derive(PartialEq, Eq, Debug)]
#[pyclass(module = "arro3.core._core", name = "DataType", subclass, frozen, eq)]
pub struct PyDataType(DataType);

impl PyDataType {
    /// Construct a new PyDataType around a [DataType].
    pub fn new(data_type: DataType) -> Self {
        Self(data_type)
    }

    /// Create from a raw Arrow C Schema capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let data_type =
            DataType::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(data_type))
    }

    /// Consume this and return its inner part.
    pub fn into_inner(self) -> DataType {
        self.0
    }

    /// Export this to a Python `arro3.core.DataType`.
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "DataType"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![self.__arrow_c_schema__(py)?])?,
        )
    }

    /// Export this to a Python `arro3.core.DataType`.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let capsule = to_schema_pycapsule(py, &self.0)?;
        arro3_mod.getattr(intern!(py, "DataType"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![capsule])?,
        )
    }

    /// Export this to a Python `nanoarrow.Schema`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_schema(py, &self.__arrow_c_schema__(py)?)
    }

    /// Export to a pyarrow.DataType
    ///
    /// Requires pyarrow >=14
    pub fn into_pyarrow(self, py: Python) -> PyResult<Bound<PyAny>> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let pyarrow_field = pyarrow_mod
            .getattr(intern!(py, "field"))?
            .call1(PyTuple::new(py, vec![self.into_pyobject(py)?])?)?;
        pyarrow_field.getattr(intern!(py, "type"))
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

#[allow(non_snake_case)]
#[pymethods]
impl PyDataType {
    pub(crate) fn __arrow_c_schema__<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, &self.0)
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: Self) -> Self {
        input
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(_cls: &Bound<PyType>, capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[getter]
    fn bit_width(&self) -> Option<usize> {
        self.0.primitive_width().map(|width| width * 8)
    }

    #[pyo3(signature=(other, *, check_metadata=false))]
    fn equals(&self, other: PyDataType, check_metadata: bool) -> bool {
        let other = other.into_inner();
        if check_metadata {
            self.0 == other
        } else {
            self.0.equals_datatype(&other)
        }
    }

    #[getter]
    fn list_size(&self) -> Option<i32> {
        match &self.0 {
            DataType::FixedSizeList(_, list_size) => Some(*list_size),
            _ => None,
        }
    }

    #[getter]
    fn num_fields(&self) -> usize {
        match &self.0 {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => 0,
            DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_) => 1,
            DataType::Struct(fields) => fields.len(),
            DataType::Union(fields, _) => fields.len(),
            // Is this accurate?
            DataType::Dictionary(_, _) | DataType::Map(_, _) | DataType::RunEndEncoded(_, _) => 2,
        }
    }

    #[getter]
    fn time_unit(&self) -> Option<&str> {
        match &self.0 {
            DataType::Time32(unit)
            | DataType::Time64(unit)
            | DataType::Timestamp(unit, _)
            | DataType::Duration(unit) => match unit {
                TimeUnit::Second => Some("s"),
                TimeUnit::Millisecond => Some("ms"),
                TimeUnit::Microsecond => Some("us"),
                TimeUnit::Nanosecond => Some("ns"),
            },
            _ => None,
        }
    }

    #[getter]
    fn tz(&self) -> Option<&str> {
        match &self.0 {
            DataType::Timestamp(_, tz) => tz.as_deref(),
            _ => None,
        }
    }

    #[getter]
    fn value_type(&self) -> Option<Arro3DataType> {
        match &self.0 {
            DataType::FixedSizeList(value_field, _)
            | DataType::List(value_field)
            | DataType::LargeList(value_field)
            | DataType::ListView(value_field)
            | DataType::LargeListView(value_field)
            | DataType::RunEndEncoded(_, value_field) => {
                Some(PyDataType::new(value_field.data_type().clone()).into())
            }
            DataType::Dictionary(_key_type, value_type) => {
                Some(PyDataType::new(*value_type.clone()).into())
            }
            _ => None,
        }
    }

    #[getter]
    fn value_field(&self) -> Option<Arro3Field> {
        match &self.0 {
            DataType::FixedSizeList(value_field, _)
            | DataType::List(value_field)
            | DataType::LargeList(value_field)
            | DataType::ListView(value_field)
            | DataType::LargeListView(value_field) => {
                Some(PyField::new(value_field.clone()).into())
            }
            _ => None,
        }
    }

    #[getter]
    fn fields(&self) -> Option<Vec<Arro3Field>> {
        match &self.0 {
            DataType::Struct(fields) => Some(
                fields
                    .into_iter()
                    .map(|f| PyField::new(f.clone()).into())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        }
    }

    ///////////////////// Constructors

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
    #[pyo3(signature = (length=None))]
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
    #[pyo3(signature = (value_type, list_size=None))]
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
    fn dictionary(_: &Bound<PyType>, index_type: PyDataType, value_type: PyDataType) -> Self {
        Self(DataType::Dictionary(
            Box::new(index_type.into_inner()),
            Box::new(value_type.into_inner()),
        ))
    }

    #[classmethod]
    fn run_end_encoded(_: &Bound<PyType>, run_end_type: PyField, value_type: PyField) -> Self {
        Self(DataType::RunEndEncoded(
            run_end_type.into_inner(),
            value_type.into_inner(),
        ))
    }

    ///////////////////// Type checking

    #[staticmethod]
    fn is_boolean(t: PyDataType) -> bool {
        t.0 == DataType::Boolean
    }

    #[staticmethod]
    fn is_integer(t: PyDataType) -> bool {
        t.0.is_integer()
    }

    #[staticmethod]
    fn is_signed_integer(t: PyDataType) -> bool {
        t.0.is_signed_integer()
    }

    #[staticmethod]
    fn is_unsigned_integer(t: PyDataType) -> bool {
        t.0.is_unsigned_integer()
    }

    #[staticmethod]
    fn is_int8(t: PyDataType) -> bool {
        t.0 == DataType::Int8
    }
    #[staticmethod]
    fn is_int16(t: PyDataType) -> bool {
        t.0 == DataType::Int16
    }
    #[staticmethod]
    fn is_int32(t: PyDataType) -> bool {
        t.0 == DataType::Int32
    }
    #[staticmethod]
    fn is_int64(t: PyDataType) -> bool {
        t.0 == DataType::Int64
    }
    #[staticmethod]
    fn is_uint8(t: PyDataType) -> bool {
        t.0 == DataType::UInt8
    }
    #[staticmethod]
    fn is_uint16(t: PyDataType) -> bool {
        t.0 == DataType::UInt16
    }
    #[staticmethod]
    fn is_uint32(t: PyDataType) -> bool {
        t.0 == DataType::UInt32
    }
    #[staticmethod]
    fn is_uint64(t: PyDataType) -> bool {
        t.0 == DataType::UInt64
    }
    #[staticmethod]
    fn is_floating(t: PyDataType) -> bool {
        t.0.is_floating()
    }
    #[staticmethod]
    fn is_float16(t: PyDataType) -> bool {
        t.0 == DataType::Float16
    }
    #[staticmethod]
    fn is_float32(t: PyDataType) -> bool {
        t.0 == DataType::Float32
    }
    #[staticmethod]
    fn is_float64(t: PyDataType) -> bool {
        t.0 == DataType::Float64
    }
    #[staticmethod]
    fn is_decimal(t: PyDataType) -> bool {
        matches!(t.0, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
    }
    #[staticmethod]
    fn is_decimal128(t: PyDataType) -> bool {
        matches!(t.0, DataType::Decimal128(_, _))
    }
    #[staticmethod]
    fn is_decimal256(t: PyDataType) -> bool {
        matches!(t.0, DataType::Decimal256(_, _))
    }

    #[staticmethod]
    fn is_list(t: PyDataType) -> bool {
        matches!(t.0, DataType::List(_))
    }
    #[staticmethod]
    fn is_large_list(t: PyDataType) -> bool {
        matches!(t.0, DataType::LargeList(_))
    }
    #[staticmethod]
    fn is_fixed_size_list(t: PyDataType) -> bool {
        matches!(t.0, DataType::FixedSizeList(_, _))
    }
    #[staticmethod]
    fn is_list_view(t: PyDataType) -> bool {
        matches!(t.0, DataType::ListView(_))
    }
    #[staticmethod]
    fn is_large_list_view(t: PyDataType) -> bool {
        matches!(t.0, DataType::LargeListView(_))
    }
    #[staticmethod]
    fn is_struct(t: PyDataType) -> bool {
        matches!(t.0, DataType::Struct(_))
    }
    #[staticmethod]
    fn is_union(t: PyDataType) -> bool {
        matches!(t.0, DataType::Union(_, _))
    }
    #[staticmethod]
    fn is_nested(t: PyDataType) -> bool {
        t.0.is_nested()
    }
    #[staticmethod]
    fn is_run_end_encoded(t: PyDataType) -> bool {
        t.0.is_run_ends_type()
    }
    #[staticmethod]
    fn is_temporal(t: PyDataType) -> bool {
        t.0.is_temporal()
    }
    #[staticmethod]
    fn is_timestamp(t: PyDataType) -> bool {
        matches!(t.0, DataType::Timestamp(_, _))
    }
    #[staticmethod]
    fn is_date(t: PyDataType) -> bool {
        matches!(t.0, DataType::Date32 | DataType::Date64)
    }
    #[staticmethod]
    fn is_date32(t: PyDataType) -> bool {
        t.0 == DataType::Date32
    }
    #[staticmethod]
    fn is_date64(t: PyDataType) -> bool {
        t.0 == DataType::Date64
    }
    #[staticmethod]
    fn is_time(t: PyDataType) -> bool {
        matches!(t.0, DataType::Time32(_) | DataType::Time64(_))
    }
    #[staticmethod]
    fn is_time32(t: PyDataType) -> bool {
        matches!(t.0, DataType::Time32(_))
    }
    #[staticmethod]
    fn is_time64(t: PyDataType) -> bool {
        matches!(t.0, DataType::Time64(_))
    }
    #[staticmethod]
    fn is_duration(t: PyDataType) -> bool {
        matches!(t.0, DataType::Duration(_))
    }
    #[staticmethod]
    fn is_interval(t: PyDataType) -> bool {
        matches!(t.0, DataType::Interval(_))
    }
    #[staticmethod]
    fn is_null(t: PyDataType) -> bool {
        t.0 == DataType::Null
    }
    #[staticmethod]
    fn is_binary(t: PyDataType) -> bool {
        t.0 == DataType::Binary
    }
    #[staticmethod]
    fn is_unicode(t: PyDataType) -> bool {
        t.0 == DataType::Utf8
    }
    #[staticmethod]
    fn is_string(t: PyDataType) -> bool {
        t.0 == DataType::Utf8
    }
    #[staticmethod]
    fn is_large_binary(t: PyDataType) -> bool {
        t.0 == DataType::LargeBinary
    }
    #[staticmethod]
    fn is_large_unicode(t: PyDataType) -> bool {
        t.0 == DataType::LargeUtf8
    }
    #[staticmethod]
    fn is_large_string(t: PyDataType) -> bool {
        t.0 == DataType::LargeUtf8
    }
    #[staticmethod]
    fn is_binary_view(t: PyDataType) -> bool {
        t.0 == DataType::BinaryView
    }
    #[staticmethod]
    fn is_string_view(t: PyDataType) -> bool {
        t.0 == DataType::Utf8View
    }
    #[staticmethod]
    fn is_fixed_size_binary(t: PyDataType) -> bool {
        matches!(t.0, DataType::FixedSizeBinary(_))
    }
    #[staticmethod]
    fn is_map(t: PyDataType) -> bool {
        matches!(t.0, DataType::Map(_, _))
    }
    #[staticmethod]
    fn is_dictionary(t: PyDataType) -> bool {
        matches!(t.0, DataType::Dictionary(_, _))
    }
    #[staticmethod]
    fn is_primitive(t: PyDataType) -> bool {
        t.0.is_primitive()
    }
    #[staticmethod]
    fn is_numeric(t: PyDataType) -> bool {
        t.0.is_numeric()
    }
    #[staticmethod]
    fn is_dictionary_key_type(t: PyDataType) -> bool {
        t.0.is_dictionary_key_type()
    }
}
