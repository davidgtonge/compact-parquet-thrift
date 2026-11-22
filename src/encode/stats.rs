//! Column chunk statistics (`ColumnMetaData` field 12) for readers that prune row groups.
//!
//! Only types whose Parquet sort order is the plain signed order of their value get a
//! range: BOOLEAN, INT32 (including DATE) and INT64. Floats (NaN ordering) and byte
//! arrays (owned, truncated bounds) are left without one.

use super::ColumnValues;
use crate::{SliceSink, Writer};

/// What a chunk's footer says about its values. `Default` claims nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkStats {
    /// Null rows in the chunk.
    pub null_count: Option<i64>,
    /// Min/max of the present values, for types with a plain sort order.
    pub range: Option<ValueRange>,
}

/// Inclusive min/max of a chunk's present values, in the physical type's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)] // `min` and `max` mean the same thing in every variant.
pub enum ValueRange {
    /// `BOOLEAN` (false < true).
    Bool { min: bool, max: bool },
    /// `INT32`, including DATE.
    I32 { min: i32, max: i32 },
    /// `INT64`.
    I64 { min: i64, max: i64 },
}

pub(crate) fn chunk_stats(rows: usize, values: &ColumnValues<'_>, present: usize) -> ChunkStats {
    ChunkStats {
        null_count: i64::try_from(rows - present).ok(),
        range: value_range(values),
    }
}

fn value_range(values: &ColumnValues<'_>) -> Option<ValueRange> {
    match values {
        ColumnValues::Bool(v) => {
            bounds(v.iter().copied()).map(|(min, max)| ValueRange::Bool { min, max })
        }
        ColumnValues::I32(v) | ColumnValues::Date(v) | ColumnValues::DeltaI32(v) => {
            bounds(v.iter().copied()).map(|(min, max)| ValueRange::I32 { min, max })
        }
        ColumnValues::DictI32 { dictionary, ids } => {
            bounds(lookup(dictionary, ids)?).map(|(min, max)| ValueRange::I32 { min, max })
        }
        ColumnValues::I64(v) | ColumnValues::DeltaI64(v) => {
            bounds(v.iter().copied()).map(|(min, max)| ValueRange::I64 { min, max })
        }
        ColumnValues::DictI64 { dictionary, ids } => {
            bounds(lookup(dictionary, ids)?).map(|(min, max)| ValueRange::I64 { min, max })
        }
        _ => None,
    }
}

/// Present values through their dictionary ids; `None` if any id is out of range
/// (page encoding rejects that column anyway).
fn lookup<'a, T: Copy>(
    dictionary: &'a [T],
    ids: &'a [u32],
) -> Option<impl Iterator<Item = T> + 'a> {
    let len = dictionary.len();
    ids.iter()
        .all(|&id| (id as usize) < len)
        .then(|| ids.iter().map(move |&id| dictionary[id as usize]))
}

fn bounds<T: Ord + Copy>(mut values: impl Iterator<Item = T>) -> Option<(T, T)> {
    let first = values.next()?;
    Some(values.fold((first, first), |(min, max), v| (min.min(v), max.max(v))))
}

/// Write `ColumnMetaData` field 12 when there is anything to say.
pub(crate) fn write_statistics(
    w: &mut Writer<SliceSink<'_>>,
    stats: &ChunkStats,
) -> crate::Result<()> {
    if stats.null_count.is_none() && stats.range.is_none() {
        return Ok(());
    }
    w.field_struct(12, |w| {
        if let Some(nulls) = stats.null_count {
            w.field_i64(3, nulls)?;
        }
        match stats.range {
            Some(ValueRange::Bool { min, max }) => {
                write_bounds(w, &[u8::from(min)], &[u8::from(max)])
            }
            Some(ValueRange::I32 { min, max }) => {
                write_bounds(w, &min.to_le_bytes(), &max.to_le_bytes())
            }
            Some(ValueRange::I64 { min, max }) => {
                write_bounds(w, &min.to_le_bytes(), &max.to_le_bytes())
            }
            None => Ok(()),
        }
    })
}

/// `min`/`max` are the plain little-endian value bytes Parquet's `min_value`/`max_value` expect.
fn write_bounds(w: &mut Writer<SliceSink<'_>>, min: &[u8], max: &[u8]) -> crate::Result<()> {
    w.field_binary(5, max)?;
    w.field_binary(6, min)
}
