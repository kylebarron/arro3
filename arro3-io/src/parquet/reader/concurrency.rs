use arrow_array::RecordBatch;
use futures::future::join_all;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::error::Arro3IoResult;
use crate::parquet::reader::options::PyParquetOptions;

pub(crate) async fn read_concurrent<T: AsyncFileReader + Unpin + Send + 'static + Clone>(
    source: T,
    meta: &ArrowReaderMetadata,
    options: PyParquetOptions,
) -> Arro3IoResult<Vec<RecordBatch>> {
    let split_options = split_options(options);
    let mut readers = split_options
        .into_iter()
        .map(|options| {
            let async_reader_builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(source.clone(), meta.clone());
            options
                .apply_to_reader_builder(async_reader_builder, &meta)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let futures = readers
        .iter_mut()
        .map(|stream| stream.try_collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let batches = join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(batches)
}

fn split_options(options: PyParquetOptions) -> Vec<PyParquetOptions> {
    if can_split_readers(&options) {
        let mut split_options = vec![];
        if let Some(row_groups) = options.row_groups {
            let row_groups_per_reader = row_groups / 2;
            for i in 0..2 {
                let start = i * row_groups_per_reader;
                let end = (i + 1) * row_groups_per_reader;
                let mut new_options = options.clone();
                new_options.row_groups = Some(row_groups[start..end].to_vec());
                split_options.push(new_options);
            }
            return split_options;
        }
    }
    todo!()
}

fn can_split_readers(options: &PyParquetOptions) -> bool {
    // No row groups to
    if options
        .row_groups
        .is_some_and(|row_groups| row_groups.len() <= 1)
    {
        return false;
    }
    if let Some(row_groups) = options.row_groups {
        if row_groups <= 1 {
            return false;
        }

        if options.limit.is_some() {
            return false;
        }
        if let Some(limit) = options.limit {}
    }
}
