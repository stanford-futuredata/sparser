extern crate memchr;
extern crate memmem;

extern crate libc;
extern crate time;

extern crate bit_vec;

use libc::{c_uchar, c_void, c_int};

use memmem::Searcher;

mod expressions;

/// The maximum word length (generally the size in bytes of a `long` in C).
const MAX_WORD_LENGTH: usize = 8;

/// The maximum number of candidates to consider.
const MAX_CANDIDATES: usize= 64;

/// The maximum number of samples to evaluate during calibration.
const MAX_SAMPLES: usize = 64;

/// A callback into a full parser. This function takes a C string as a pointer and an arbitrary
/// context object, and returns 0 if the callback successfully passed the record and 1 if the
/// callback failed (i.e., received a NULL record or did not pass the record).
/// 
/// The context generally encapsulates the query to execute on the parsed record (e.g., a set of
/// selections or projections).
type ParserCallbackFn = fn(*const c_uchar, *mut c_void) -> c_int;

/// Holds data returned by the sampler.
#[derive(Debug, Clone)]
pub struct SampleData {
    /// Tracks false positives across samples.
    /// 
    /// If bit false_positives[i][j] is set, then the candidate pre-filter `i` returned a false
    /// positive for sample `j`. A false positive is defined as a pre-filter which passes a record,
    /// but the callback fails the record.
    false_positives: Vec<bit_vec::BitVec>,

    /// Measures the average duration of invoking the callback.
    callback_cost: time::Duration,
}

impl SampleData {
    /// Generates a new `SampleData` given the number of candidates and samples.
    fn with_size(num_samples: usize, num_candidates: usize) -> SampleData {
        SampleData {
            false_positives: vec![bit_vec::BitVec::from_elem(num_samples, false); num_candidates],
            callback_cost: time::Duration::nanoseconds(0),
        }
    }
}

/// Designates the kind of pre-filter.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum PreFilterKind {
    /// Searches for an exact match of `word` in a record.
    WordSearch(String),
    /// Searches for `key`. If it is found, searches for `value` until any one of the `delimiters`.
    KeyValueSearch {
        key: String,
        value: String,
        delimiters: Vec<String>,
    },
}

/// Returns all substrings of `s` of up to length `max` (and of at least length 2).
/// The returned substrings all fit exactly within a word, i.e., are 2, 4, or 8 bytes long.
fn all_substrings(s: &str, max: usize) -> Vec<&str> {
    let mut substrings = vec![];
    let mut max = std::cmp::min(s.len(), max);

    // Don't search for things that are too small.
    if max < 2 {
        return substrings;
    }

    // Clamp the max to an even number that fits into a single word (2, 4, or 8 bytes).
    if max >= 8 {
        max = 8;
    } else if max >= 4 {
        max = 4; 
    } else {
        max = 2;
    }
    
    for len in 2..max {
        for start in 0..(s.len() - max) {
            substrings.push(&s[start..len]);
        }
    }
    substrings
}

/// Returns a list of candidate strings to search for when performing calibration.
/// TODO Currently, this just returns `WordMatch` candidates.
/// TODO We can make this smarter; it just returns an exhaustive set of strings for now.
fn generate_candidates(predicates: &Vec<String>) -> Vec<PreFilterKind> {
    let mut candidates = vec![];

    // Generate the list of predicates.
    for predicate in predicates.iter() {
        let substrings = all_substrings(&predicate, MAX_WORD_LENGTH);
        let num_to_add = std::cmp::min(substrings.len(), MAX_CANDIDATES - candidates.len());
        candidates.extend(substrings.into_iter().take(num_to_add).map(|e| PreFilterKind::WordSearch(e.into())));

        // Candidate list is full!
        if candidates.len() == MAX_CANDIDATES {
            break;
        }
    }
    candidates
}

pub fn generate_false_positives(sample: &mut [u8], predicates: Vec<String>, parser_callback: ParserCallbackFn) -> SampleData {
    use PreFilterKind::*;
    let candidates = generate_candidates(&predicates);
    let mut records_processed = 0;
    let mut result = SampleData::with_size(MAX_SAMPLES, candidates.len());

    // The index of the record currently being processed as an offset from `sample`.
    let mut base = 0;

    while records_processed < MAX_SAMPLES {
        if let Some(endpos) = memchr::memchr(b'\n', sample) { 
            sample[base + endpos] = b'\0';

            // Tracks whether a candidates is found in the current record.
            let mut found = bit_vec::BitVec::from_elem(candidates.len(), false);

            for (i, candidate) in candidates.iter().enumerate() {
                // TODO! This part will change based on the *kind* of candidate (i.e., how the
                // candidate is applied).  This is the biggest change we want to make for VLDB;
                // instead of just grepping for some terms, we need some kind of interpretation
                // engine that can handle different kinds of pre-filters.
                match *candidate {
                    WordSearch(ref word) => {
                        let searcher = memmem::TwoWaySearcher::new(word.as_bytes());
                        if let Some(_) = searcher.search_in(sample.get(base..base + endpos).unwrap()) {
                            result.false_positives[i].set(records_processed, true);
                            found.set(i, true);
                        }
                    }
                    _ => unimplemented!(),
                };
            }

            // If all the candidates were found, we need to run the full parser to check
            // if they're all false positives.
            if found.all() {
                let c_sample = unsafe { sample.as_mut_ptr().offset(base as isize) };

                let start = time::PreciseTime::now();
                let passed = parser_callback(c_sample, std::ptr::null_mut()) as i32;
                let end = time::PreciseTime::now();
                result.callback_cost = result.callback_cost + start.to(end);

                if passed != 0 { 
                    // the callback passed too, so these aren't false positives!
                    for i in 0..candidates.len() {
                        result.false_positives[i].set(records_processed, false);
                    }
                }
            }

            records_processed += 1;

            sample[base + endpos] = b'\n';
            // Check if we are finished processing the input dataset.
            // If not, proceed to the next record.
            if base + endpos + 1 > sample.len() {
                break;
            } else {
                base += endpos + 1;
            }
        }
    }
    result
}

/// Parses filter expressions into an expression tree.
pub fn parse_expressions() {}

/// Returns a heuristically generated list of candidate pre-filters which satisfy the expression
/// tree provided by the user. Each candidate pre-filter only generates false positives. Pre-filter
/// candidates are returned as _pre-filter classes_. The optimizer must select at least one
/// pre-filter from each class.
pub fn gen_candidates() {}


/// Generates the final schedule to execute, which is interpreted by the execution engine.
pub fn gen_schedule(_: &SampleData) {}

// parse_expressions                    gen_candidates                             sample         gen_schedule
// Filter Expression (input by user) -> Expression Set to Candidate Pre-Filters -> Measurement -> Optimizer
