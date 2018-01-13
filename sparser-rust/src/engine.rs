//! Execution engine for Sparser.

extern crate libc;
use self::libc::{c_uchar, c_void, c_int, c_ulong};

use super::prefilters::PreFilterKind;

/// A callback into a full parser. This function takes a C string as a pointer and an arbitrary
/// context object, and returns 0 if the callback successfully passed the record and 1 if the
/// callback failed (i.e., received a NULL record or did not pass the record).
///
/// The context generally encapsulates the query to execute on the parsed record (e.g., a set of
/// selections or projections).
pub type ParserCallbackFn = fn(*const c_uchar, *mut c_void) -> c_int;

/// A callback which returns the length of the passed record, in bytes.
pub type RecordIteratorFn = fn(*const c_uchar) -> c_ulong;

pub enum Operator {
    /// Apply a pre-filter. Depending on whether the pre-filter passes or fails,
    /// calls `on_true` or `on_false`.
    PreFilter {
        kind: PreFilterKind,
        on_true: Box<Operator>,
        on_false: Box<Operator>,
    },
    /// Skip to the next record in the input using the given iterator function, and then
    /// run `next`.
    NextRecord {
        func: RecordIteratorFn,
        next: Box<Operator>,
    },
    /// Parse the record fully using `func`, and the run `next.
    ParseRecord {
        func: ParserCallbackFn,
        next: Box<Operator>,
    },
    /// Finish processing the data.
    Finish,
}
