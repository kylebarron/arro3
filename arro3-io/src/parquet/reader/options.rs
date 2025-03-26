use arrow::array::AsArray;
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, DataType};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowReaderBuilder, ArrowReaderMetadata, RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3RecordBatch;
use pyo3_arrow::PyArray;

/// Note that these are a list of columns and are not yet resolved until [resolve] is called.
#[derive(Debug, FromPyObject)]
struct PyProjectionMask(Vec<String>);

impl PyProjectionMask {
    fn resolve(&self, schema: &SchemaDescriptor) -> ProjectionMask {
        ProjectionMask::columns(schema, self.0.iter().map(|s| s.as_str()))
    }
}

#[derive(Debug)]
struct PyInputPredicate {
    evaluate: PyObject,
    projection: PyProjectionMask,
}

impl PyInputPredicate {
    fn into_arrow_predicate(self, schema: &SchemaDescriptor) -> Box<dyn ArrowPredicate> {
        Box::new(SchemaResolvedPredicate {
            evaluate: self.evaluate,
            projection: self.projection.resolve(schema),
        })
    }
}

impl<'py> FromPyObject<'py> for PyInputPredicate {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        let projection = ob.getattr(intern!(py, "projection"))?.extract()?;

        let evaluate_callback = ob.getattr(intern!(py, "evaluate"))?;

        // Check that the callback object is actually callable
        let builtins_mod = py.import(intern!(py, "builtins"))?;
        if !builtins_mod
            .call_method1(intern!(py, "callable"), (&evaluate_callback,))?
            .extract::<bool>()?
        {
            return Err(PyTypeError::new_err("evaluate must be callable"));
        }

        Ok(Self {
            projection,
            evaluate: evaluate_callback.unbind(),
        })
    }
}

struct SchemaResolvedPredicate {
    evaluate: PyObject,
    projection: ProjectionMask,
}

impl SchemaResolvedPredicate {
    fn evaluate_inner(&mut self, py: Python, batch: RecordBatch) -> PyResult<PyArray> {
        let result = self
            .evaluate
            .bind(py)
            .call1((Arro3RecordBatch::from(batch),))?;
        result.extract::<PyArray>()
    }
}

impl ArrowPredicate for SchemaResolvedPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> std::result::Result<BooleanArray, ArrowError> {
        let py_array = Python::with_gil(|py| self.evaluate_inner(py, batch))
            .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
        let (array, _field) = py_array.into_inner();
        if !matches!(array.data_type(), DataType::Boolean) {
            Err(ArrowError::SchemaError(format!(
                "Expected a boolean array, got {:?}",
                array.data_type()
            )))
        } else {
            Ok(array.as_boolean().clone())
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PyParquetOptions {
    batch_size: Option<usize>,
    row_groups: Option<Vec<usize>>,
    columns: Option<PyProjectionMask>,
    filter: Option<Vec<PyInputPredicate>>,
    // selection: Option<RowSelection>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'py> FromPyObject<'py> for PyParquetOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        let mut batch_size = None;
        let mut row_groups = None;
        let mut columns = None;
        let mut filter = None;
        let mut limit = None;
        let mut offset = None;
        if let Ok(val) = ob.get_item(intern!(py, "batch_size")) {
            batch_size = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "row_groups")) {
            row_groups = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "columns")) {
            columns = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "filter")) {
            // Can either be a sequence of filters or a single filter
            let mut filters = vec![];
            match val.try_iter() {
                Ok(iter) => {
                    for item in iter {
                        filters.push(item?.extract()?);
                    }
                }
                Err(_) => {
                    filters.push(val.extract()?);
                }
            }
            filter = Some(filters);
        }
        if let Ok(val) = ob.get_item(intern!(py, "limit")) {
            limit = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "offset")) {
            offset = Some(val.extract()?);
        }

        Ok(Self {
            batch_size,
            row_groups,
            columns,
            filter,
            limit,
            offset,
        })
    }
}

impl PyParquetOptions {
    pub(crate) fn apply_to_reader_builder<T>(
        self,
        mut builder: ArrowReaderBuilder<T>,
        metadata: &ArrowReaderMetadata,
    ) -> ArrowReaderBuilder<T> {
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(row_groups) = self.row_groups {
            builder = builder.with_row_groups(row_groups);
        }
        if let Some(columns) = self.columns {
            builder = builder.with_projection(columns.resolve(metadata.parquet_schema()));
        }
        if let Some(filters) = self.filter {
            let predicates = filters
                .into_iter()
                .map(|predicate| predicate.into_arrow_predicate(metadata.parquet_schema()))
                .collect();
            builder = builder.with_row_filter(RowFilter::new(predicates));
        }
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }
        builder
    }
}
