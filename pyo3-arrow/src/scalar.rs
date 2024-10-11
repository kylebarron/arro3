use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::*;
use arrow_array::timezone::Tz;
use arrow_array::{Array, ArrayRef, Datum, UnionArray};
use arrow_schema::{ArrowError, DataType, FieldRef};
use indexmap::IndexMap;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::to_array_pycapsules;
use crate::{PyArray, PyDataType, PyField};

/// A Python-facing Arrow scalar
#[pyclass(module = "arro3.core._core", name = "Scalar", subclass)]
pub struct PyScalar {
    array: ArrayRef,
    field: FieldRef,
}

impl PyScalar {
    /// Create a new PyScalar without any checks
    ///
    /// # Safety
    ///
    /// - The array's DataType must match the field's DataType
    /// - The array must have length 1
    pub unsafe fn new_unchecked(array: ArrayRef, field: FieldRef) -> Self {
        Self { array, field }
    }

    /// Create a new PyArray from an [ArrayRef], inferring its data type automatically.
    pub fn try_from_array_ref(array: ArrayRef) -> PyArrowResult<Self> {
        let field = Field::new("", array.data_type().clone(), true);
        Self::try_new(array, Arc::new(field))
    }

    /// Create a new PyScalar
    ///
    /// This will error if the arrays' data type does not match the field's data type or if the
    /// length of the array is not 1.
    pub fn try_new(array: ArrayRef, field: FieldRef) -> PyArrowResult<Self> {
        // Do usual array validation
        let (array, field) = PyArray::try_new(array, field)?.into_inner();
        if array.len() != 1 {
            return Err(ArrowError::SchemaError(
                "Expected array of length 1 for scalar".to_string(),
            )
            .into());
        }

        Ok(Self { array, field })
    }

    /// Import from raw Arrow capsules
    pub fn try_from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyArrowResult<Self> {
        let (array, field) =
            PyArray::from_arrow_pycapsule(schema_capsule, array_capsule)?.into_inner();
        Self::try_new(array, field)
    }

    /// Access the underlying [ArrayRef].
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    /// Access the underlying [FieldRef].
    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    /// Consume self to access the underlying [ArrayRef] and [FieldRef].
    pub fn into_inner(self) -> (ArrayRef, FieldRef) {
        (self.array, self.field)
    }

    /// Export to an arro3.core.Scalar.
    ///
    /// This requires that you depend on arro3-core from your Python package.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Scalar"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            self.__arrow_c_array__(py, None)?,
        )?;
        Ok(core_obj.to_object(py))
    }
}

impl Display for PyScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.Scalar<")?;
        self.array.data_type().fmt(f)?;
        writeln!(f, ">")?;
        Ok(())
    }
}

impl Datum for PyScalar {
    fn get(&self) -> (&dyn Array, bool) {
        (self.array.as_ref(), true)
    }
}

#[pymethods]
impl PyScalar {
    #[new]
    #[pyo3(signature = (obj, /, r#type = None, *))]
    fn init(py: Python, obj: &Bound<PyAny>, r#type: Option<PyField>) -> PyArrowResult<Self> {
        if let Ok(data) = obj.extract::<PyScalar>() {
            return Ok(data);
        }

        let obj = PyList::new_bound(py, vec![obj]);
        let array = PyArray::init(&obj, r#type)?;
        let (array, field) = array.into_inner();
        Self::try_new(array, field)
    }

    #[allow(unused_variables)]
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<PyTuple>> {
        to_array_pycapsules(py, self.field.clone(), &self.array, requested_schema)
    }

    fn __eq__(&self, py: Python, other: Bound<'_, PyAny>) -> PyResult<PyObject> {
        if let Ok(other) = other.extract::<PyScalar>() {
            let eq = self.array == other.array && self.field == other.field;
            Ok(eq.into_py(py))
        } else {
            // If other is not an Arrow scalar, cast self to a Python object, and then call its
            // `__eq__` method.
            let self_py = self.as_py(py)?;
            self_py.call_method1(
                py,
                intern!(py, "__eq__"),
                PyTuple::new_bound(py, vec![other]),
            )
        }
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: PyScalar) -> Self {
        input
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyArrowResult<Self> {
        Self::try_from_arrow_pycapsule(schema_capsule, array_capsule)
    }

    pub(crate) fn as_py(&self, py: Python) -> PyArrowResult<PyObject> {
        if self.array.is_null(0) {
            return Ok(py.None());
        }

        let arr = self.array.as_ref();
        let result = match self.array.data_type() {
            DataType::Null => py.None(),
            DataType::Boolean => arr.as_boolean().value(0).into_py(py),
            DataType::Int8 => arr.as_primitive::<Int8Type>().value(0).into_py(py),
            DataType::Int16 => arr.as_primitive::<Int16Type>().value(0).into_py(py),
            DataType::Int32 => arr.as_primitive::<Int32Type>().value(0).into_py(py),
            DataType::Int64 => arr.as_primitive::<Int64Type>().value(0).into_py(py),
            DataType::UInt8 => arr.as_primitive::<UInt8Type>().value(0).into_py(py),
            DataType::UInt16 => arr.as_primitive::<UInt16Type>().value(0).into_py(py),
            DataType::UInt32 => arr.as_primitive::<UInt32Type>().value(0).into_py(py),
            DataType::UInt64 => arr.as_primitive::<UInt64Type>().value(0).into_py(py),
            DataType::Float16 => f32::from(arr.as_primitive::<Float16Type>().value(0)).into_py(py),
            DataType::Float32 => arr.as_primitive::<Float32Type>().value(0).into_py(py),
            DataType::Float64 => arr.as_primitive::<Float64Type>().value(0).into_py(py),
            DataType::Timestamp(time_unit, tz) => {
                if let Some(tz) = tz {
                    let tz = Tz::from_str(tz)?;
                    match time_unit {
                        TimeUnit::Second => arr
                            .as_primitive::<TimestampSecondType>()
                            .value_as_datetime_with_tz(0, tz)
                            .into_py(py),
                        TimeUnit::Millisecond => arr
                            .as_primitive::<TimestampMillisecondType>()
                            .value_as_datetime_with_tz(0, tz)
                            .into_py(py),
                        TimeUnit::Microsecond => arr
                            .as_primitive::<TimestampMicrosecondType>()
                            .value_as_datetime_with_tz(0, tz)
                            .into_py(py),
                        TimeUnit::Nanosecond => arr
                            .as_primitive::<TimestampNanosecondType>()
                            .value_as_datetime_with_tz(0, tz)
                            .into_py(py),
                    }
                } else {
                    match time_unit {
                        TimeUnit::Second => arr
                            .as_primitive::<TimestampSecondType>()
                            .value_as_datetime(0)
                            .into_py(py),
                        TimeUnit::Millisecond => arr
                            .as_primitive::<TimestampMillisecondType>()
                            .value_as_datetime(0)
                            .into_py(py),
                        TimeUnit::Microsecond => arr
                            .as_primitive::<TimestampMicrosecondType>()
                            .value_as_datetime(0)
                            .into_py(py),
                        TimeUnit::Nanosecond => arr
                            .as_primitive::<TimestampNanosecondType>()
                            .value_as_datetime(0)
                            .into_py(py),
                    }
                }
            }
            DataType::Date32 => arr
                .as_primitive::<Date32Type>()
                .value_as_date(0)
                .into_py(py),
            DataType::Date64 => arr
                .as_primitive::<Date64Type>()
                .value_as_date(0)
                .into_py(py),
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => arr
                    .as_primitive::<Time32SecondType>()
                    .value_as_time(0)
                    .into_py(py),
                TimeUnit::Millisecond => arr
                    .as_primitive::<Time32MillisecondType>()
                    .value_as_time(0)
                    .into_py(py),
                _ => unreachable!(),
            },
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => arr
                    .as_primitive::<Time64MicrosecondType>()
                    .value_as_time(0)
                    .into_py(py),
                TimeUnit::Nanosecond => arr
                    .as_primitive::<Time64NanosecondType>()
                    .value_as_time(0)
                    .into_py(py),

                _ => unreachable!(),
            },
            DataType::Duration(time_unit) => match time_unit {
                TimeUnit::Second => arr
                    .as_primitive::<DurationSecondType>()
                    .value_as_duration(0)
                    .into_py(py),
                TimeUnit::Millisecond => arr
                    .as_primitive::<DurationMillisecondType>()
                    .value_as_duration(0)
                    .into_py(py),
                TimeUnit::Microsecond => arr
                    .as_primitive::<DurationMicrosecondType>()
                    .value_as_duration(0)
                    .into_py(py),
                TimeUnit::Nanosecond => arr
                    .as_primitive::<DurationNanosecondType>()
                    .value_as_duration(0)
                    .into_py(py),
            },
            DataType::Interval(_) => {
                // https://github.com/apache/arrow-rs/blob/6c59b7637592e4b67b18762b8313f91086c0d5d8/arrow-array/src/temporal_conversions.rs#L219
                todo!("interval is not yet fully documented [ARROW-3097]")
            }
            DataType::Binary => arr.as_binary::<i32>().value(0).into_py(py),
            DataType::FixedSizeBinary(_) => arr.as_fixed_size_binary().value(0).into_py(py),
            DataType::LargeBinary => arr.as_binary::<i64>().value(0).into_py(py),
            DataType::BinaryView => arr.as_binary_view().value(0).into_py(py),
            DataType::Utf8 => arr.as_string::<i32>().value(0).into_py(py),
            DataType::LargeUtf8 => arr.as_string::<i64>().value(0).into_py(py),
            DataType::Utf8View => arr.as_string_view().value(0).into_py(py),
            DataType::List(inner_field) => {
                let inner_array = arr.as_list::<i32>().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::LargeList(inner_field) => {
                let inner_array = arr.as_list::<i64>().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::FixedSizeList(inner_field, _list_size) => {
                let inner_array = arr.as_fixed_size_list().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::ListView(_inner_field) => {
                todo!("as_list_view does not yet exist");
                // let inner_array = arr.as_list_view::<i32>().value(0);
                // list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::LargeListView(_inner_field) => {
                todo!("as_list_view does not yet exist");
                // let inner_array = arr.as_list_view::<i64>().value(0);
                // list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::Struct(inner_fields) => {
                let struct_array = arr.as_struct();
                let mut dict_py_objects: IndexMap<&str, PyObject> =
                    IndexMap::with_capacity(inner_fields.len());
                for (inner_field, column) in inner_fields.iter().zip(struct_array.columns()) {
                    let scalar =
                        unsafe { PyScalar::new_unchecked(column.clone(), inner_field.clone()) };
                    dict_py_objects.insert(inner_field.name(), scalar.as_py(py)?);
                }
                dict_py_objects.into_py(py)
            }
            DataType::Union(_, _) => {
                let array = arr.as_any().downcast_ref::<UnionArray>().unwrap();
                let scalar = PyScalar::try_from_array_ref(array.value(0))?;
                scalar.as_py(py)?
            }
            DataType::Dictionary(_, _) => {
                let array = arr.as_any_dictionary();
                let keys = array.keys();
                let key = match keys.data_type() {
                    DataType::Int8 => keys.as_primitive::<Int8Type>().value(0) as usize,
                    DataType::Int16 => keys.as_primitive::<Int16Type>().value(0) as usize,
                    DataType::Int32 => keys.as_primitive::<Int32Type>().value(0) as usize,
                    DataType::Int64 => keys.as_primitive::<Int64Type>().value(0) as usize,
                    DataType::UInt8 => keys.as_primitive::<UInt8Type>().value(0) as usize,
                    DataType::UInt16 => keys.as_primitive::<UInt16Type>().value(0) as usize,
                    DataType::UInt32 => keys.as_primitive::<UInt32Type>().value(0) as usize,
                    DataType::UInt64 => keys.as_primitive::<UInt64Type>().value(0) as usize,
                    // Above are the valid dictionary key types
                    // https://docs.rs/arrow/latest/arrow/datatypes/trait.ArrowDictionaryKeyType.html
                    _ => unreachable!(),
                };
                let value = array.values().slice(key, 1);
                PyScalar::try_from_array_ref(value)?.as_py(py)?
            }

            // TODO: decimal support.
            //
            // We should implement this by constructing a tuple object to pass into the
            // decimal.Decimal constructor.
            //
            // From the docs: https://docs.python.org/3/library/decimal.html#decimal.Decimal
            //
            // If value is a tuple, it should have three components, a sign (0 for positive or 1
            // for negative), a tuple of digits, and an integer exponent. For example, Decimal((0,
            // (1, 4, 1, 4), -3)) returns Decimal('1.414').
            DataType::Decimal128(_, _) => {
                // let array = arr.as_primitive::<Decimal128Type>();
                todo!()
            }
            DataType::Decimal256(_, _) => {
                // let array = arr.as_primitive::<Decimal256Type>();
                todo!()
            }
            DataType::Map(_, _) => {
                let array = arr.as_map();
                let struct_arr = array.value(0);
                let key_arr = struct_arr.column_by_name("key").unwrap();
                let value_arr = struct_arr.column_by_name("value").unwrap();

                let mut entries = Vec::with_capacity(struct_arr.len());
                for i in 0..struct_arr.len() {
                    let py_key = PyScalar::try_from_array_ref(key_arr.slice(i, 1))?.as_py(py)?;
                    let py_value =
                        PyScalar::try_from_array_ref(value_arr.slice(i, 1))?.as_py(py)?;
                    entries.push(PyTuple::new_bound(py, vec![py_key, py_value]));
                }

                entries.into_py(py)
            }
            DataType::RunEndEncoded(_, _) => {
                todo!()
            }
        };
        Ok(result)
    }

    fn cast(&self, py: Python, target_type: PyField) -> PyArrowResult<PyObject> {
        let new_field = target_type.into_inner();
        let new_array = arrow::compute::cast(&self.array, new_field.data_type())?;
        Ok(PyScalar::try_new(new_array, new_field)
            .unwrap()
            .to_arro3(py)?)
    }

    #[getter]
    #[pyo3(name = "field")]
    fn py_field(&self, py: Python) -> PyResult<PyObject> {
        PyField::new(self.field.clone()).to_arro3(py)
    }

    #[getter]
    fn is_valid(&self) -> bool {
        self.array.is_valid(0)
    }

    #[getter]
    fn r#type(&self, py: Python) -> PyResult<PyObject> {
        PyDataType::new(self.field.data_type().clone()).to_arro3(py)
    }
}

fn list_values_to_py(
    py: Python,
    inner_array: ArrayRef,
    inner_field: &Arc<Field>,
) -> PyArrowResult<Vec<PyObject>> {
    let mut list_py_objects = Vec::with_capacity(inner_array.len());
    for i in 0..inner_array.len() {
        let scalar =
            unsafe { PyScalar::new_unchecked(inner_array.slice(i, 1), inner_field.clone()) };
        list_py_objects.push(scalar.as_py(py)?);
    }
    Ok(list_py_objects)
}
