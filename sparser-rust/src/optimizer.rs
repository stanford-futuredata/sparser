extern crate memchr;
extern crate memmem;
extern crate bit_vec;

extern crate libc;
extern crate time;

use std;
use self::libc::{c_uchar, c_void, c_int};
use self::memmem::Searcher;
use self::bit_vec::BitVec;

use super::prefilters::PreFilterKind;

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
    false_positives: Vec<BitVec>,

    /// Measures the average duration of invoking the callback.
    callback_cost: time::Duration,
}

impl SampleData {
    /// Generates a new `SampleData` given the number of candidates and samples.
    fn with_size(num_samples: usize, num_candidates: usize) -> SampleData {
        SampleData {
            false_positives: vec![BitVec::from_elem(num_samples, false); num_candidates],
            callback_cost: time::Duration::nanoseconds(0),
        }
    }
}

pub fn generate_false_positives(sample: &mut [u8],
                                candidates: Vec<PreFilterKind>,
                                parser_callback: ParserCallbackFn) -> SampleData {
    use super::prefilters::PreFilterKind::*;
    let mut records_processed = 0;
    let mut result = SampleData::with_size(MAX_SAMPLES, candidates.len());

    // The index of the record currently being processed as an offset from `sample`.
    let mut base = 0;

    while records_processed < MAX_SAMPLES {
        if let Some(endpos) = memchr::memchr(b'\n', sample) {
            sample[base + endpos] = b'\0';

            // Tracks whether a candidates is found in the current record.
            let mut found = BitVec::from_elem(candidates.len(), false);

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
