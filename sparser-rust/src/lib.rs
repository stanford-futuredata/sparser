//! The Sparser library.

pub mod filters;
pub mod prefilters;
pub mod optimizer;

use filters::FilterKind;
use optimizer::{ParserCallbackFn, RecordIteratorFn};

/// Builds the Sparser query for `data` with the given `filter`. This function is re-entrant,
/// in that it can be called multiple times with no side effects.
pub fn compile(filter: &FilterKind, data: &[u8], parser: ParserCallbackFn, rec_iter: RecordIteratorFn) {
    // First, convert the user filter into a canonical form (CNF and then filter sets).
    let mut filter = filter.clone();
    filter.to_cnf();
    let sets = filter.into_filter_sets();

    // Generate pre-filter candidates for each filter set.
    let prefilters: Vec<_> = sets.into_iter().map(|ref v| prefilters::prefilter_candidates(v)).collect();

    // Run the optimizer to get the filter cascade. TODO use this.
    let _cascade = optimizer::generate_cascade(data, &prefilters, parser, rec_iter);

    // Generate a runtime which either JITs code or creates an interpreter for the filter cascade.
    // let runtime = engine::generate_runtime(cascade, false);
    // return runtime;
}
