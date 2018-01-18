//! The Sparser library.

// Suppress warnings for unused functions.
#![cfg_attr(not(test), allow(dead_code))]

pub mod filters;
pub mod prefilters;
pub mod optimizer;
pub mod engine;

use filters::FilterKind;
use engine::{ParserCallbackFn, RecordIteratorFn};

/// Builds the Sparser query for `data` with the given `filter`. This function is re-entrant,
/// in that it can be called multiple times with no side effects.
pub fn compile(filter: &FilterKind,
               _data: &[u8],
               _parser: ParserCallbackFn,
               _rec_iter: RecordIteratorFn) {
    // First, convert the user filter into a canonical form (CNF and then filter sets).
    let mut filter = filter.clone();
    filter.to_cnf();
    let _sets = filter.into_filter_sets();
}
