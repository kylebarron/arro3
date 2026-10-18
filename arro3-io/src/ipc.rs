use std::io::{BufReader, BufWriter};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::{
    read_footer_length, FileDecoder, FileReaderBuilder, StreamDecoder, StreamReader,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::{root_as_footer, Block};
use arrow_schema::{ArrowError, SchemaRef};
use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3RecordBatchReader;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::PyRecordBatchReader;

use crate::utils::{FileReader, FileWriter};

/// Zero-copy reader over an in-memory IPC stream
struct BufferStreamReader {
    buffer: Buffer,
    decoder: StreamDecoder,
    schema: SchemaRef,
    /// A batch decoded with the schema, if the schema length was misread
    pending: Option<RecordBatch>,
    finished: bool,
}

impl BufferStreamReader {
    /// `None` if the schema can't be decoded, to leave the stream to StreamReader
    fn try_new(bytes: Bytes) -> Result<Option<Self>, ArrowError> {
        let mut buffer = Buffer::from(bytes);
        let mut decoder = StreamDecoder::new();
        let mut finished = false;
        // Decode only the schema, like StreamReader. The decoder needs one byte past a message
        // to complete it.
        let head_len = first_message_len(&buffer)
            .saturating_add(1)
            .min(buffer.len());
        let mut head = buffer.slice_with_length(0, head_len);
        let decoded = decoder.decode(&mut head);
        buffer.advance(head_len - head.len());
        let pending = match decoded {
            Ok(pending) => pending,
            Err(e) if is_bytes_after_eos(&e) => {
                finished = true;
                None
            }
            Err(e) => return Err(e),
        };
        // Also the case when the schema ends the buffer, which StreamDecoder can't finish
        let Some(schema) = decoder.schema() else {
            return Ok(None);
        };
        Ok(Some(Self {
            buffer,
            decoder,
            schema,
            pending,
            finished,
        }))
    }
}

/// Length of the first message, read from its prefix
fn first_message_len(buffer: &[u8]) -> usize {
    let word = |i: usize| {
        buffer
            .get(i..i + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
    };
    match word(0) {
        Some(0xFFFF_FFFF) => word(4).map_or(buffer.len(), |len| len.saturating_add(8)),
        Some(len) => len.saturating_add(4),
        None => buffer.len(),
    }
}

/// StreamDecoder errors on bytes after the end-of-stream marker, which StreamReader ignores
fn is_bytes_after_eos(e: &ArrowError) -> bool {
    matches!(e, ArrowError::IpcError(msg) if msg == "Unexpected EOS")
}

impl Iterator for BufferStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.pending.take() {
            return Some(Ok(batch));
        }
        if self.finished {
            return None;
        }
        match self.decoder.decode(&mut self.buffer) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.finished = true;
                // Error on a stream truncated mid-message
                self.decoder.finish().err().map(Err)
            }
            Err(e) => {
                self.finished = true;
                (!is_bytes_after_eos(&e)).then_some(Err(e))
            }
        }
    }
}

impl RecordBatchReader for BufferStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Zero-copy reader over an in-memory IPC file, as in arrow-rs's zero_copy_ipc example
struct BufferFileReader {
    buffer: Buffer,
    decoder: FileDecoder,
    schema: SchemaRef,
    batches: Vec<Block>,
    next: usize,
}

impl BufferFileReader {
    fn try_new(bytes: Bytes) -> Result<Self, ArrowError> {
        let buffer = Buffer::from(bytes);
        if buffer.len() < 10 {
            return Err(ArrowError::IpcError(
                "IPC file too short for a footer".to_string(),
            ));
        }
        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())?;
        if footer_len > trailer_start {
            return Err(ArrowError::IpcError(
                "IPC footer length exceeds file".to_string(),
            ));
        }
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
            .map_err(|e| ArrowError::ParseError(format!("Unable to get root as footer: {e:?}")))?;
        let ipc_schema = footer
            .schema()
            .ok_or_else(|| ArrowError::IpcError("IPC footer has no schema".to_string()))?;
        // Same check as FileReaderBuilder
        if !ipc_schema.endianness().equals_to_target_endianness() {
            return Err(ArrowError::IpcError(
                "the endianness of the source system does not match the endianness of the target system.".to_string(),
            ));
        }
        let schema = Arc::new(fb_to_schema(ipc_schema));
        let mut decoder = FileDecoder::new(schema.clone(), footer.version());
        for block in footer.dictionaries().iter().flatten() {
            let data = block_slice(&buffer, block)?;
            decoder.read_dictionary(block, &data)?;
        }
        let batches = footer
            .recordBatches()
            .ok_or_else(|| ArrowError::IpcError("IPC footer has no record batches".to_string()))?
            .iter()
            .copied()
            .collect();
        Ok(Self {
            buffer,
            decoder,
            schema,
            batches,
            next: 0,
        })
    }
}

/// Slice out a footer block, erroring instead of panicking on bad offsets
fn block_slice(buffer: &Buffer, block: &Block) -> Result<Buffer, ArrowError> {
    let out_of_range = || ArrowError::IpcError("IPC block out of range".to_string());
    let offset = usize::try_from(block.offset()).map_err(|_| out_of_range())?;
    let metadata_len = usize::try_from(block.metaDataLength()).map_err(|_| out_of_range())?;
    let body_len = usize::try_from(block.bodyLength()).map_err(|_| out_of_range())?;
    // FileDecoder reads the 8-byte message prefix unchecked
    if metadata_len < 8 {
        return Err(out_of_range());
    }
    let len = metadata_len
        .checked_add(body_len)
        .ok_or_else(out_of_range)?;
    if offset
        .checked_add(len)
        .filter(|end| *end <= buffer.len())
        .is_none()
    {
        return Err(ArrowError::IpcError(
            "IPC block extends past the end of the file".to_string(),
        ));
    }
    Ok(buffer.slice_with_length(offset, len))
}

impl Iterator for BufferFileReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.batches.len() {
            let block = self.batches[self.next];
            self.next += 1;
            let data = match block_slice(&self.buffer, &block) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };
            match self.decoder.read_record_batch(&block, &data) {
                Ok(Some(batch)) => return Some(Ok(batch)),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl RecordBatchReader for BufferFileReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Read an Arrow IPC file to an Arrow RecordBatchReader
#[pyfunction]
pub fn read_ipc(file: FileReader) -> PyArrowResult<Arro3RecordBatchReader> {
    if let FileReader::Buffer {
        cursor,
        writable: false,
    } = file
    {
        let reader = BufferFileReader::try_new(cursor.into_inner())?;
        return Ok(PyRecordBatchReader::new(Box::new(reader)).into());
    }
    let builder = FileReaderBuilder::new();
    let buf_file = BufReader::new(file);
    let reader = builder.build(buf_file)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).into())
}

/// Read an Arrow IPC Stream file to an Arrow RecordBatchReader
#[pyfunction]
pub fn read_ipc_stream(file: FileReader) -> PyArrowResult<Arro3RecordBatchReader> {
    if let FileReader::Buffer {
        cursor,
        writable: false,
    } = &file
    {
        if let Some(reader) = BufferStreamReader::try_new(cursor.get_ref().clone())? {
            return Ok(PyRecordBatchReader::new(Box::new(reader)).into());
        }
    }
    let reader = StreamReader::try_new(file, None)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).into())
}

#[allow(clippy::upper_case_acronyms)]
pub enum IpcCompression {
    LZ4,
    ZSTD,
}

impl<'py> FromPyObject<'_, 'py> for IpcCompression {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s: String = obj.extract()?;
        match s.to_lowercase().as_str() {
            "lz4" | "lz4_frame" | "lz4frame" => Ok(Self::LZ4),
            "zstd" => Ok(Self::ZSTD),
            _ => Err(PyValueError::new_err(
                "Unexpected compression. Should be one of 'LZ4', 'ZSTD'.",
            )),
        }
    }
}

impl From<IpcCompression> for arrow_ipc::CompressionType {
    fn from(value: IpcCompression) -> Self {
        match value {
            IpcCompression::LZ4 => Self::LZ4_FRAME,
            IpcCompression::ZSTD => Self::ZSTD,
        }
    }
}

/// Write an Arrow Table or stream to an IPC File
#[pyfunction]
#[pyo3(
    signature = (data, file, *, compression = IpcCompression::LZ4),
    text_signature = "(data, file, *, compression = 'LZ4')")
]
pub fn write_ipc(
    data: AnyRecordBatch,
    file: FileWriter,
    compression: Option<IpcCompression>,
) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let options = IpcWriteOptions::default().try_with_compression(compression.map(|x| x.into()))?;
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new_with_options(buf_writer, &reader.schema(), options)?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}

/// Write an Arrow Table or stream to an IPC Stream
#[pyfunction]
#[pyo3(
    signature = (data, file, *, compression = IpcCompression::LZ4),
    text_signature = "(data, file, *, compression = 'LZ4')")
]
pub fn write_ipc_stream(
    data: AnyRecordBatch,
    file: FileWriter,
    compression: Option<IpcCompression>,
) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let options = IpcWriteOptions::default().try_with_compression(compression.map(|x| x.into()))?;
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
        buf_writer,
        &reader.schema(),
        options,
    )?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}
