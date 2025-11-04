use std::str::FromStr;

use arrow_array::temporal_conversions::as_datetime;
use arrow_array::ArrowPrimitiveType;
use arrow_schema::ArrowError;
use chrono::offset::TimeZone;
use chrono::{DateTime, FixedOffset, Utc};
use chrono::{LocalResult, NaiveDate, NaiveDateTime, Offset};
use pyo3::prelude::*;

/// An [`Offset`] for [`PyArrowTz`]
#[derive(Debug, Copy, Clone)]
pub(crate) struct PyArrowTzOffset {
    tz: PyArrowTz,
    offset: FixedOffset,
}

impl std::fmt::Display for PyArrowTzOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.offset.fmt(f)
    }
}

impl Offset for PyArrowTzOffset {
    fn fix(&self) -> FixedOffset {
        self.offset
    }
}
/// An Arrow [`TimeZone`]
///
/// This is vendored from upstream so we can implement `IntoPyObject`, while also needing to
/// implement chrono::TimeZone
///
/// <https://github.com/apache/arrow-rs/blob/77df2ee42d8ca1d1557a64681b240b8409deef01/arrow-array/src/timezone.rs#L78-L80>
#[derive(Debug, Clone, Copy, IntoPyObject)]
pub(crate) struct PyArrowTz(TzInner);

#[derive(Debug, Copy, Clone, IntoPyObject)]
pub(crate) enum TzInner {
    Timezone(chrono_tz::Tz),
    Offset(FixedOffset),
}

macro_rules! tz {
    ($s:ident, $tz:ident, $b:block) => {
        match $s.0 {
            TzInner::Timezone($tz) => $b,
            TzInner::Offset($tz) => $b,
        }
    };
}

impl TimeZone for PyArrowTz {
    type Offset = PyArrowTzOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        offset.tz
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
        tz!(self, tz, {
            tz.offset_from_local_date(local).map(|x| PyArrowTzOffset {
                tz: *self,
                offset: x.fix(),
            })
        })
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
        tz!(self, tz, {
            tz.offset_from_local_datetime(local)
                .map(|x| PyArrowTzOffset {
                    tz: *self,
                    offset: x.fix(),
                })
        })
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        tz!(self, tz, {
            PyArrowTzOffset {
                tz: *self,
                offset: tz.offset_from_utc_date(utc).fix(),
            }
        })
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        tz!(self, tz, {
            PyArrowTzOffset {
                tz: *self,
                offset: tz.offset_from_utc_datetime(utc).fix(),
            }
        })
    }
}

impl FromStr for PyArrowTz {
    type Err = ArrowError;

    fn from_str(tz: &str) -> Result<Self, Self::Err> {
        match parse_fixed_offset(tz) {
            Some(offset) => Ok(Self(TzInner::Offset(offset))),
            None => Ok(Self(TzInner::Timezone(tz.parse().map_err(|e| {
                ArrowError::ParseError(format!("Invalid timezone \"{tz}\": {e}"))
            })?))),
        }
    }
}

/// Parses a fixed offset of the form "+09:00", "-09" or "+0930"
///
/// Vendored from upstream
/// <https://github.com/apache/arrow-rs/blob/77df2ee42d8ca1d1557a64681b240b8409deef01/arrow-array/src/timezone.rs#L24-L49>
///
/// Upstream doesn't want to expose the TzInner enum, only expose as string
/// <https://github.com/apache/arrow-rs/issues/7173#issuecomment-2675276458>
///
/// While we need to discern between fixed offset and tz string so that we can convert to correct
/// Python tz class
fn parse_fixed_offset(tz: &str) -> Option<FixedOffset> {
    let bytes = tz.as_bytes();

    let mut values = match bytes.len() {
        // [+-]XX:XX
        6 if bytes[3] == b':' => [bytes[1], bytes[2], bytes[4], bytes[5]],
        // [+-]XXXX
        5 => [bytes[1], bytes[2], bytes[3], bytes[4]],
        // [+-]XX
        3 => [bytes[1], bytes[2], b'0', b'0'],
        _ => return None,
    };
    values.iter_mut().for_each(|x| *x = x.wrapping_sub(b'0'));
    if values.iter().any(|x| *x > 9) {
        return None;
    }
    let secs =
        (values[0] * 10 + values[1]) as i32 * 60 * 60 + (values[2] * 10 + values[3]) as i32 * 60;

    match bytes[0] {
        b'+' => FixedOffset::east_opt(secs),
        b'-' => FixedOffset::west_opt(secs),
        _ => None,
    }
}

/// Converts an [`ArrowPrimitiveType`] to [`DateTime<Tz>`]
///
/// Vendored from
/// <https://github.com/apache/arrow-rs/blob/77df2ee42d8ca1d1557a64681b240b8409deef01/arrow-array/src/temporal_conversions.rs#L274-L278>
/// So we can use our own PyArrowTz type
pub(crate) fn as_datetime_with_timezone<T: ArrowPrimitiveType>(
    v: i64,
    tz: PyArrowTz,
) -> Option<DateTime<PyArrowTz>> {
    let naive = as_datetime::<T>(v)?;
    Some(Utc.from_utc_datetime(&naive).with_timezone(&tz))
}
