use arrow_schema::Schema;

/// Check whether two schemas are equal
///
/// This allows schemas to have different top-level metadata, as well as different nested field
/// names and keys.
pub(crate) fn schema_equals(left: &Schema, right: &Schema) -> bool {
    left.fields
        .iter()
        .zip(right.fields.iter())
        .all(|(left_field, right_field)| {
            left_field.name() == right_field.name()
                && left_field
                    .data_type()
                    .equals_datatype(right_field.data_type())
        })
}
