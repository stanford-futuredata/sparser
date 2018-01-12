//! Pre-filters are the main workhorse of Sparser. This file defines the pre-filter types and a
//! trait which converts a user-defined filter type into a Pre-filter type.

/// The maximum word length (generally the size in bytes of a `long` in C).
pub const MAX_WORD_LENGTH: usize = 8;

/// The maximum number of candidates considered.
pub const MAX_CANDIDATES: usize= 64;

/// Designates the kind of pre-filter.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum PreFilterKind {
    /// Searches for an exact match of `word` in a record. A word is generally a 1, 2, or 4, or 8
    /// byte long string.
    WordSearch(String),
    /// Searches for `key`. If it is found, searches for `value` until any one of the `delimiters`.
    KeyValueSearch {
        key: String,
        value: String,
        delimiters: Vec<String>,
    },
}

/// Converts `Self` into a set of pre-filters.
pub trait PreFilterSet {
    /// Converts `Self` into a set of pre-filters.
    fn prefilter_set(&self) -> Vec<PreFilterKind>;
}

/// Converts a set of user-defined filters into a set of candidate prefilters. A user-defined
/// filter set represents a conjunction of filters; the optimizer may choose _any_ zero or more
/// of the generated pre-filters from the returned set to represent the conjunction with only
/// false positives.
pub fn prefilter_candidates<T: PreFilterSet>(filters: &Vec<T>) -> Vec<PreFilterKind> {
    filters.iter().flat_map(|f| f.prefilter_set().into_iter()).take(MAX_CANDIDATES).collect()
}
