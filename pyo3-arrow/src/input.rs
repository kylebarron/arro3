//! Special input types to allow broader user input.
//!
//! These types tend to be used for unknown user input, and thus do not exist for return types,
//! where the exact type is known.

use crate::{PyRecordBatch, PyRecordBatchReader};

/// An enum over [PyRecordBatch] and [PyRecordBatchReader], used when a function accepts either
/// Arrow object as input.
pub enum AnyRecordBatch {
    RecordBatch(PyRecordBatch),
    Stream(PyRecordBatchReader),
}
