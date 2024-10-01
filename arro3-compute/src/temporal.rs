use arrow_schema::{DataType, Field};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

pub enum DatePart {
    /// Quarter of the year, in range `1..=4`
    Quarter,
    /// Calendar year
    Year,
    /// Month in the year, in range `1..=12`
    Month,
    /// ISO week of the year, in range `1..=53`
    Week,
    /// Day of the month, in range `1..=31`
    Day,
    /// Day of the week, in range `0..=6`, where Sunday is `0`
    DayOfWeekSunday0,
    /// Day of the week, in range `0..=6`, where Monday is `0`
    DayOfWeekMonday0,
    /// Day of year, in range `1..=366`
    DayOfYear,
    /// Hour of the day, in range `0..=23`
    Hour,
    /// Minute of the hour, in range `0..=59`
    Minute,
    /// Second of the minute, in range `0..=59`
    Second,
    /// Millisecond of the second
    Millisecond,
    /// Microsecond of the second
    Microsecond,
    /// Nanosecond of the second
    Nanosecond,
}

impl<'a> FromPyObject<'a> for DatePart {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "quarter" => Ok(Self::Quarter),
            "year" => Ok(Self::Year),
            "month" => Ok(Self::Month),
            "week" => Ok(Self::Week),
            "day" => Ok(Self::Day),
            "dayofweeksunday0" => Ok(Self::DayOfWeekSunday0),
            "dayofweekmonday0" => Ok(Self::DayOfWeekMonday0),
            "dayofyear" => Ok(Self::DayOfYear),
            "hour" => Ok(Self::Hour),
            "minute" => Ok(Self::Minute),
            "second" => Ok(Self::Second),
            "millisecond" => Ok(Self::Millisecond),
            "microsecond" => Ok(Self::Microsecond),
            "nanosecond" => Ok(Self::Nanosecond),
            _ => Err(PyValueError::new_err("Unexpected date part")),
        }
    }
}

impl From<DatePart> for arrow::compute::DatePart {
    fn from(value: DatePart) -> Self {
        match value {
            DatePart::Quarter => arrow::compute::DatePart::Quarter,
            DatePart::Year => arrow::compute::DatePart::Year,
            DatePart::Month => arrow::compute::DatePart::Month,
            DatePart::Week => arrow::compute::DatePart::Week,
            DatePart::Day => arrow::compute::DatePart::Day,
            DatePart::DayOfWeekSunday0 => arrow::compute::DatePart::DayOfWeekSunday0,
            DatePart::DayOfWeekMonday0 => arrow::compute::DatePart::DayOfWeekMonday0,
            DatePart::DayOfYear => arrow::compute::DatePart::DayOfYear,
            DatePart::Hour => arrow::compute::DatePart::Hour,
            DatePart::Minute => arrow::compute::DatePart::Minute,
            DatePart::Second => arrow::compute::DatePart::Second,
            DatePart::Millisecond => arrow::compute::DatePart::Millisecond,
            DatePart::Microsecond => arrow::compute::DatePart::Microsecond,
            DatePart::Nanosecond => arrow::compute::DatePart::Nanosecond,
        }
    }
}

#[pyfunction]
pub fn date_part(py: Python, input: AnyArray, part: DatePart) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(input) => {
            let out = arrow::compute::date_part(input.as_ref(), part.into())?;
            Ok(PyArray::from_array_ref(out).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let output_field = Field::new("", DataType::Int32, true);
            let part = part.into();

            let iter = reader
                .into_iter()
                .map(move |array| arrow::compute::date_part(array?.as_ref(), part));
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, output_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}
