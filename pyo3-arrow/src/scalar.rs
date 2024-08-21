use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::*;
use arrow_array::{ArrayRef, UnionArray};
use arrow_schema::{ArrowError, DataType, FieldRef};
use pyo3::prelude::*;

use crate::error::PyArrowResult;
use crate::{PyArray, PyDataType};

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
}

impl Display for PyScalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.Scalar<")?;
        self.array.data_type().fmt(f)?;
        writeln!(f, ">")?;
        Ok(())
    }
}

#[pymethods]
impl PyScalar {
    fn __repr__(&self) -> String {
        self.to_string()
    }

    fn as_py(&self, py: Python) -> PyArrowResult<PyObject> {
        if self.array.is_null(0) {
            return Ok(py.None());
        }

        let dt = self.array.data_type();
        let array_ref = self.array.as_ref();

        let result = match dt {
            DataType::Null => py.None(),
            DataType::Boolean => array_ref.as_boolean().value(0).into_py(py),
            DataType::Int8 => array_ref.as_primitive::<Int8Type>().value(0).into_py(py),
            DataType::Int16 => array_ref.as_primitive::<Int16Type>().value(0).into_py(py),
            DataType::Int32 => array_ref.as_primitive::<Int32Type>().value(0).into_py(py),
            DataType::Int64 => array_ref.as_primitive::<Int64Type>().value(0).into_py(py),
            DataType::UInt8 => array_ref.as_primitive::<UInt8Type>().value(0).into_py(py),
            DataType::UInt16 => array_ref.as_primitive::<UInt16Type>().value(0).into_py(py),
            DataType::UInt32 => array_ref.as_primitive::<UInt32Type>().value(0).into_py(py),
            DataType::UInt64 => array_ref.as_primitive::<UInt64Type>().value(0).into_py(py),
            DataType::Float16 => {
                f32::from(array_ref.as_primitive::<Float16Type>().value(0)).into_py(py)
            }
            DataType::Float32 => array_ref.as_primitive::<Float32Type>().value(0).into_py(py),
            DataType::Float64 => array_ref.as_primitive::<Float64Type>().value(0).into_py(py),
            // TODO: timestamp, date, time, duration, interval
            DataType::Binary => array_ref.as_binary::<i32>().value(0).into_py(py),
            DataType::FixedSizeBinary(_) => array_ref.as_fixed_size_binary().value(0).into_py(py),
            DataType::LargeBinary => array_ref.as_binary::<i64>().value(0).into_py(py),
            DataType::BinaryView => array_ref.as_binary_view().value(0).into_py(py),
            DataType::Utf8 => array_ref.as_string::<i32>().value(0).into_py(py),
            DataType::LargeUtf8 => array_ref.as_string::<i64>().value(0).into_py(py),
            DataType::Utf8View => array_ref.as_string_view().value(0).into_py(py),
            DataType::List(inner_field) => {
                let inner_array = array_ref.as_list::<i32>().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::LargeList(inner_field) => {
                let inner_array = array_ref.as_list::<i64>().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::FixedSizeList(inner_field, _list_size) => {
                let inner_array = array_ref.as_fixed_size_list().value(0);
                list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::ListView(_inner_field) => {
                todo!("as_list_view does not yet exist");
                // let inner_array = array_ref.as_list_view::<i32>().value(0);
                // list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::LargeListView(_inner_field) => {
                todo!("as_list_view does not yet exist");
                // let inner_array = array_ref.as_list_view::<i64>().value(0);
                // list_values_to_py(py, inner_array, inner_field)?.into_py(py)
            }
            DataType::Struct(inner_fields) => {
                let struct_array = array_ref.as_struct();
                let mut dict_py_objects: HashMap<&str, PyObject> =
                    HashMap::with_capacity(inner_fields.len());
                for (inner_field, column) in inner_fields.iter().zip(struct_array.columns()) {
                    let scalar =
                        unsafe { PyScalar::new_unchecked(column.clone(), inner_field.clone()) };
                    dict_py_objects.insert(inner_field.name(), scalar.as_py(py)?);
                }
                dict_py_objects.into_py(py)
            }
            DataType::Union(_, _) => {
                let array = array_ref.as_any().downcast_ref::<UnionArray>().unwrap();
                let scalar = PyScalar::try_from_array_ref(array.value(0))?;
                scalar.as_py(py)?
            }
            DataType::Dictionary(_, _) => {
                todo!()
                // let array = array_ref.as_any_dictionary();
                // array.
                // todo!()
            }
            DataType::Decimal128(_, _) => {
                todo!()
            }
            // Decimal 256
            // Map
            // RunEndEncoded
            _ => todo!(),
        };
        Ok(result)
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
