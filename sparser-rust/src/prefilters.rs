//! Defines Sparser's pre-filters, which represent approximate search queries over the input.

extern crate memmem;

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

impl PreFilterKind {
    /// Evaluates a pre-filter on a slice, returning `true` if the filter passes (potentially
    /// indicating a false positive) or `false` otherwise. This effectively implements a Rust-based
    /// interpreter for the prefilters.
    pub fn evaluate(&self, buffer: &[u8]) -> bool {
        use self::PreFilterKind::*;
        use self::memmem::Searcher;
        match *self {
            WordSearch(ref word) => {
                let searcher = memmem::TwoWaySearcher::new(word.as_bytes());
                searcher.search_in(buffer).is_some()
            }
            _ => unimplemented!(),
        }
    }

    /// Returns a numeric cost for the prefilter. The cost should roughly correspond to the number
    /// of nanoseconds required to execute a single filter. For example, if the filter takes one
    /// scalar instruction per byte to execute on a 1GHz CPU, the returned cost will be
    /// `record_length` * 1.
    pub fn cost(&self, record_length: usize) -> f64 {
        use self::PreFilterKind::*;
        match *self {
            WordSearch(ref word) => {
                return ((word.len() / MAX_WORD_LENGTH) as f64) * (record_length as f64);
            }
            _ => unimplemented!(),
        }
    }
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
