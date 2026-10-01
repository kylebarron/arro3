use arrow_cast::display::FormatOptions;
use arrow_schema::Schema;

/// Check whether two schemas are equal
///
/// Field names, types, and nullability must match, as in pyarrow's `Schema.equals` with
/// `check_metadata=False`. This allows schemas to have different top-level and field metadata,
/// as well as different nested field names and keys.
pub(crate) fn schema_equals(left: &Schema, right: &Schema) -> bool {
    left.fields.len() == right.fields.len()
        && left
            .fields
            .iter()
            .zip(right.fields.iter())
            .all(|(left_field, right_field)| {
                left_field.name() == right_field.name()
                    && left_field.is_nullable() == right_field.is_nullable()
                    && left_field
                        .data_type()
                        .equals_datatype(right_field.data_type())
            })
}

pub(crate) fn default_repr_options<'a>() -> FormatOptions<'a> {
    FormatOptions::new()
        .with_display_error(true)
        .with_null("null")
        .with_types_info(true)
}
