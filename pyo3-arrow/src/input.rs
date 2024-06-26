use crate::{PyArray, PyRecordBatchReader};

/// An enum over [PyArray] and [PyRecordBatchReader], used when a function accepts either Arrow
/// object as input.
pub enum AnyArray {
    Array(PyArray),
    Stream(PyRecordBatchReader),
}
