use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ArrowReaderMetadata};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use pyo3::intern;
use pyo3::prelude::*;

#[derive(Debug, FromPyObject, Clone)]
struct PyProjectionMask(Vec<String>);

impl PyProjectionMask {
    fn resolve(&self, schema: &SchemaDescriptor) -> ProjectionMask {
        ProjectionMask::columns(schema, self.0.iter().map(|s| s.as_str()))
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PyParquetOptions {
    batch_size: Option<usize>,
    row_groups: Option<Vec<usize>>,
    columns: Option<PyProjectionMask>,
    // filter: Option<RowFilter>,
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
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }
        builder
    }
}
