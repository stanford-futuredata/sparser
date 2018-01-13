extern crate memchr;
extern crate bit_vec;

extern crate libc;
extern crate time;

use std;
use self::libc::{c_uchar, c_void, c_int, c_ulong};
use self::bit_vec::BitVec;

use super::prefilters::PreFilterKind;

/// The maximum number of samples to evaluate during calibration.
pub const MAX_SAMPLES: usize = 64;

/// A callback into a full parser. This function takes a C string as a pointer and an arbitrary
/// context object, and returns 0 if the callback successfully passed the record and 1 if the
/// callback failed (i.e., received a NULL record or did not pass the record).
///
/// The context generally encapsulates the query to execute on the parsed record (e.g., a set of
/// selections or projections).
pub type ParserCallbackFn = fn(*const c_uchar, *mut c_void) -> c_int;

/// A callback which returns the length of the passed record, in bytes.
pub type RecordIteratorFn = fn(*const c_uchar) -> c_ulong;

/// Holds data returned by the sampler.
#[derive(Debug, Clone)]
struct SampleData {
    /// Tracks false positives across samples.
    ///
    /// If bit false_positives[i][j] is set, then the candidate pre-filter `i` returned a false
    /// positive for sample `j`. A false positive is defined as a pre-filter which passes a record,
    /// but the callback fails the record.
    false_positives: Vec<BitVec>,

    /// Measures the average duration of invoking the callback.
    callback_cost: time::Duration,

    /// The number of samples tested.
    records_processed: usize,
}

impl SampleData {
    /// Generates a new `SampleData` given the number of candidates and samples.
    fn with_size(num_samples: usize, num_candidates: usize) -> SampleData {
        SampleData {
            false_positives: vec![BitVec::from_elem(num_samples, false); num_candidates],
            callback_cost: time::Duration::nanoseconds(0),
            records_processed: 0,
        }
    }
}

/// Retruns a cascade of ordered prefilters to execute using Sparser's optimization algorithm.
pub fn generate_cascade(data: &[u8],
                        prefilters: &Vec<Vec<PreFilterKind>>,
                        parser: ParserCallbackFn,
                        rec_iter: RecordIteratorFn) {

    // TODO Consider all the prefilter sets.
    let _sample_data = generate_false_positives(data, prefilters.get(0).unwrap(), parser, rec_iter);
    // TODO run optimzation algorithm by combining bitmaps.
}

fn generate_false_positives(data: &[u8],
                                prefilters: &Vec<PreFilterKind>,
                                parser: ParserCallbackFn,
                                rec_iter: RecordIteratorFn) -> SampleData {

    let mut result = SampleData::with_size(MAX_SAMPLES, prefilters.len());

    // The index of the record currently being processed as an offset from `data`.
    let mut base = 0;

    while result.records_processed < MAX_SAMPLES {
        // The record currently being processed.
        let record = unsafe { data.as_ptr().offset(base as isize) };
        // The length of the record.
        let endpos = rec_iter(record) as usize;

        // Tracks whether a prefilters is found in the current record.
        let mut found = BitVec::from_elem(prefilters.len(), false);

        // Evaluate each of the candidate pre-filters on the record, recording whether
        // the pre-filter passed the record.
        for (i, candidate) in prefilters.iter().enumerate() {
            if candidate.evaluate(data.get(base..base + endpos).unwrap()) {
                result.false_positives[i].set(result.records_processed, true);
                found.set(i, true);
            }
        }

        // If all the prefilters were found, we need to run the full parser to check
        // if they are all false positives. If any of them failed, since each one can only
        // return a false positive, all the other ones which passed are false positives.
        if found.all() {
            let start = time::PreciseTime::now();
            let passed = parser(record, std::ptr::null_mut()) as i32;
            let end = time::PreciseTime::now();
            result.callback_cost = result.callback_cost + start.to(end);

            if passed != 0 {
                // the callback passed too, so these aren't false positives; the record actually
                // passed!
                for i in 0..prefilters.len() {
                    result.false_positives[i].set(result.records_processed, false);
                }
            }
        }

        result.records_processed += 1;

        // Check if we are finished processing the input dataset.
        // If not, proceed to the next record.
        if base + endpos + 1 > data.len() {
            break;
        } else {
            base += endpos + 1;
        }
    }
    result
}
