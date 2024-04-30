use arrow::datatypes::FieldRef;
use arrow::error::ArrowError;
use arrow_array::ArrayRef;

/// Trait for types that can read `ArrayRef`'s.
///
/// To create from an iterator, see [ArrayIterator].
pub trait ArrayReader: Iterator<Item = Result<ArrayRef, ArrowError>> {
    /// Returns the field of this `ArrayReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn field(&self) -> FieldRef;
}

impl<R: ArrayReader + ?Sized> ArrayReader for Box<R> {
    fn field(&self) -> FieldRef {
        self.as_ref().field()
    }
}

pub struct ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    inner: I::IntoIter,
    inner_field: FieldRef,
}

impl<I> ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    /// Create a new [ArrayIterator].
    ///
    /// If `iter` is an infallible iterator, use `.map(Ok)`.
    pub fn new(iter: I, field: FieldRef) -> Self {
        Self {
            inner: iter.into_iter(),
            inner_field: field,
        }
    }
}

impl<I> Iterator for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I> ArrayReader for ArrayIterator<I>
where
    I: IntoIterator<Item = Result<ArrayRef, ArrowError>>,
{
    fn field(&self) -> FieldRef {
        self.inner_field.clone()
    }
}
