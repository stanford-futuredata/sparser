extern crate memchr;
extern crate bit_vec;

extern crate libc;

use libc::{c_char, c_void, c_int};

/// The maximum word length (generally the size in bytes of a `long` in C).
const MAX_WORD_LENGTH: usize = 8;

/// The maximum number of candidates to consider.
const MAX_CANDIDATES: usize= 64;

/// The maximum number of samples to evaluate during calibration.
const MAX_SAMPLES: usize = 64;

/// A callback into a full parser.
type ParserCallbackFn = fn(*mut c_char, *mut c_void) -> c_int;

/// A sparser query, which encapsulates search tokens. Currently, this only
/// supports conjunctions of tokens, though eventually we want to support
/// disjunctions as well.
/// 
/// TODO maybe this should be called a `Schedule` instead?
struct SparserQuery {
    queries: Vec<String>,
}

impl SparserQuery {
    fn new() -> SparserQuery {
        SparserQuery {
            queries: vec![]
        }
    }

    fn add_query<S>(&mut self, v: S) where S: Into<String> {
        self.queries.push(v.into());
    }
}

/// Returns all substrings of `s` of up to length `max` (and of at least length 2).
/// The returned substrings all fit exactly within a word, i.e., are 2, 4, or 8 bytes long.
fn all_substrings<'a>(s: &'a str, max: usize) -> Vec<&'a str> {
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
/// TODO We can make this smarter; it just returns an exhaustive set of strings for now.
fn generate_candidates(predicates: &Vec<String>) -> Vec<&str> {
    let mut candidates = vec![];

    // Generate the list of predicates.
    for predicate in predicates.iter() {
        let substrings = all_substrings(&predicate, MAX_WORD_LENGTH);
        let num_to_add = std::cmp::min(substrings.len(), MAX_CANDIDATES - candidates.len());
        candidates.extend(substrings.into_iter().take(num_to_add));

        // Candidate list is full!
        if candidates.len() == MAX_CANDIDATES {
            break;
        }
    }
    candidates
}

/// Given an input dataset `sample` and a set of `predicates`, 
fn calibrate(sample: &mut [u8], predicates: Vec<String>) {
    let candidates = generate_candidates(&predicates);

    // The estimated callback cost.
    let mut callback_cost = 0;
    let mut records_processed = 0;

    // Tracks false positives across samples.
    let mut false_positives = vec![bit_vec::BitVec::from_elem(MAX_SAMPLES, false); candidates.len()];

    // The index of the record currently being processed as an offset from `sample`.
    let mut base = 0;

    while records_processed < MAX_SAMPLES {
        if let Some(endpos) = memchr::memchr(b'\n', sample) { 
            sample[base + endpos] = b'\0';

            // Tracks whether a candidates is found in the current record.
            let mut found = bit_vec::BitVec::from_elem(candidates.len(), false);

            for (i, candidate) in candidates.iter().enumerate() {
                // TODO Search for the predicates in this record using memmem or something.
                if false {
                    false_positives[i].set(records_processed, true);
                    found.set(i, true);
                }
            }

            // If all the candidates were found, we need to run the full parser to check
            // if they're all false positives.
            if found.all() {
                // TODO call the callback here and time it!
                if true { 
                    // the callback passed too, so these aren't false positives!
                    for i in 0..candidates.len() {
                        false_positives[i].set(records_processed, false);
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
}
