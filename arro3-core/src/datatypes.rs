/// NOTE: The content of this file is not yet exported to Python.
///
/// It's not clear whether including data type information is in scope, because data types are not
/// themselves an FFI construct.
///
use arrow::datatypes::DataType;
use arrow_schema::{IntervalUnit, TimeUnit};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::error::PyArrowResult;
use crate::field::PyField;

pub struct PyTimeUnit(arrow_schema::TimeUnit);

impl<'a> FromPyObject<'a> for PyTimeUnit {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
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

#[derive(Clone, PartialEq, Eq, Debug)]
#[pyclass(module = "arro3.core._rust", name = "DataType", subclass)]
pub struct PyDataType(pub DataType);

#[pymethods]
impl PyDataType {
    #[classmethod]
    fn null(_: &PyType) -> Self {
        Self(DataType::Null)
    }

    #[classmethod]
    fn bool(_: &PyType) -> Self {
        Self(DataType::Boolean)
    }

    #[classmethod]
    fn int8(_: &PyType) -> Self {
        Self(DataType::Int8)
    }

    #[classmethod]
    fn int16(_: &PyType) -> Self {
        Self(DataType::Int16)
    }

    #[classmethod]
    fn int32(_: &PyType) -> Self {
        Self(DataType::Int32)
    }

    #[classmethod]
    fn int64(_: &PyType) -> Self {
        Self(DataType::Int64)
    }

    #[classmethod]
    fn uint8(_: &PyType) -> Self {
        Self(DataType::UInt8)
    }

    #[classmethod]
    fn uint16(_: &PyType) -> Self {
        Self(DataType::UInt16)
    }

    #[classmethod]
    fn uint32(_: &PyType) -> Self {
        Self(DataType::UInt32)
    }

    #[classmethod]
    fn uint64(_: &PyType) -> Self {
        Self(DataType::UInt64)
    }

    #[classmethod]
    fn float16(_: &PyType) -> Self {
        Self(DataType::Float16)
    }

    #[classmethod]
    fn float32(_: &PyType) -> Self {
        Self(DataType::Float32)
    }

    #[classmethod]
    fn float64(_: &PyType) -> Self {
        Self(DataType::Float64)
    }

    #[classmethod]
    fn time32(_: &PyType, unit: PyTimeUnit) -> PyArrowResult<Self> {
        if unit.0 == TimeUnit::Microsecond || unit.0 == TimeUnit::Nanosecond {
            return Err(PyValueError::new_err("Unexpected timeunit for time32").into());
        }

        Ok(Self(DataType::Time32(unit.0)))
    }

    #[classmethod]
    fn time64(_: &PyType, unit: PyTimeUnit) -> PyArrowResult<Self> {
        if unit.0 == TimeUnit::Second || unit.0 == TimeUnit::Millisecond {
            return Err(PyValueError::new_err("Unexpected timeunit for time64").into());
        }

        Ok(Self(DataType::Time64(unit.0)))
    }

    #[classmethod]
    fn timestamp(_: &PyType, unit: PyTimeUnit, tz: Option<String>) -> Self {
        Self(DataType::Timestamp(unit.0, tz.map(|s| s.into())))
    }

    #[classmethod]
    fn date32(_: &PyType) -> Self {
        Self(DataType::Date32)
    }

    #[classmethod]
    fn date64(_: &PyType) -> Self {
        Self(DataType::Date64)
    }

    #[classmethod]
    fn duration(_: &PyType, unit: PyTimeUnit) -> Self {
        Self(DataType::Duration(unit.0))
    }

    #[classmethod]
    fn month_day_nano_interval(_: &PyType) -> Self {
        Self(DataType::Interval(IntervalUnit::MonthDayNano))
    }

    #[classmethod]
    fn binary(_: &PyType) -> Self {
        Self(DataType::Binary)
    }

    #[classmethod]
    fn string(_: &PyType) -> Self {
        Self(DataType::Utf8)
    }

    #[classmethod]
    fn utf8(_: &PyType) -> Self {
        Self(DataType::Utf8)
    }

    #[classmethod]
    fn large_binary(_: &PyType) -> Self {
        Self(DataType::LargeBinary)
    }

    #[classmethod]
    fn large_string(_: &PyType) -> Self {
        Self(DataType::LargeUtf8)
    }

    #[classmethod]
    fn large_utf8(_: &PyType) -> Self {
        Self(DataType::LargeUtf8)
    }

    #[classmethod]
    fn binary_view(_: &PyType) -> Self {
        Self(DataType::BinaryView)
    }

    #[classmethod]
    fn string_view(_: &PyType) -> Self {
        Self(DataType::Utf8View)
    }

    #[classmethod]
    fn decimal128(_: &PyType, precision: u8, scale: i8) -> Self {
        Self(DataType::Decimal128(precision, scale))
    }

    #[classmethod]
    fn decimal256(_: &PyType, precision: u8, scale: i8) -> Self {
        Self(DataType::Decimal256(precision, scale))
    }

    #[classmethod]
    fn list(_: &PyType, value_type: PyField, list_size: Option<i32>) -> Self {
        if let Some(list_size) = list_size {
            Self(DataType::FixedSizeList(value_type.into(), list_size))
        } else {
            Self(DataType::List(value_type.into()))
        }
    }

    #[classmethod]
    fn large_list(_: &PyType, value_type: PyField) -> Self {
        Self(DataType::LargeList(value_type.into()))
    }

    #[classmethod]
    fn list_view(_: &PyType, value_type: PyField) -> Self {
        Self(DataType::ListView(value_type.into()))
    }

    #[classmethod]
    fn large_list_view(_: &PyType, value_type: PyField) -> Self {
        Self(DataType::LargeListView(value_type.into()))
    }

    // TODO: fix this.
    // #[classmethod]
    // fn map(_: &PyType, key_type: PyField, item_type: PyField, keys_sorted: bool) -> Self {
    //     let field = Field::new(
    //         "entries",
    //         DataType::Struct(vec![Arc::new(key_type.0), Arc::new(item_type.0)].into()),
    //         true,
    //     );
    //     //  ::new_struct("entries", , true);
    //     Self(DataType::Map(field.into(), keys_sorted))
    // }

    fn __repr__(&self) -> String {
        format!("{}", &self.0)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}
