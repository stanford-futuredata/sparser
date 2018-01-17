//! Chooses pre-filters to execute using Sparser's optimization algorithm.

extern crate memchr;
extern crate bit_vec;

extern crate time;

use std;

use std::collections::HashMap;

use self::bit_vec::BitVec;

use super::prefilters::PreFilterKind;
use super::engine::ParserCallbackFn;
use super::engine::RecordIteratorFn;

/// The maximum number of samples to evaluate during calibration.
pub const MAX_SAMPLES: usize = 64;

/// ID of a prefilter set.
type SetId = i32;

#[derive(Debug, Clone)]
struct PreFilterEntry {
    /// The false positive mask for the prefilter.
    false_positives: BitVec,
    /// The prefilter sets this prefilter belongs to. Prefilters in the same set are OR'd together.
    /// If the prefilter belongs in more than one set, then it appears more than once in the full
    /// expression.
    set: Vec<SetId>,
}

impl PreFilterEntry {
    fn new(bits: usize, sets: Vec<SetId>) -> PreFilterEntry {
        PreFilterEntry {
            false_positives: BitVec::from_elem(bits, false),
            set: sets
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Operator {
    PreFilter {
        kind: PreFilterKind,
        on_true: Box<Operator>,
        on_false: Box<Operator>,
    },
    Parse,
    Finish,
}

/// A plan is just a tree of operators.
pub type Plan = Operator;

impl Plan {
    /// Computes the cost of a sparser plan, which is a tree of operators such as prefilters, parsing,
    /// or aborting the computation on the current record.
    fn cost(&self, prefilters: &HashMap<PreFilterKind, PreFilterEntry>, parse_cost: f64) -> f64 {
        // Stack to track probabilities, used to compute joint probabilities and nodes in the tree.
        let ref mut stack = vec![];
        // Stores the probabilities of executing a prefilter.
        let ref mut p_pfs = HashMap::new();
        // Probability of parsing the full result.
        let ref mut p_parse = 0.0;

        self.cost_internal(prefilters, stack, p_pfs, p_parse);

        let mut total_cost = 0.0;
        for (key, _) in prefilters.iter() {
            total_cost += p_pfs.get(key).unwrap_or(&0.0) * key.cost(1000);
        }
        total_cost += *p_parse * parse_cost;
        total_cost
    }

    /// Internal recursive helper for `cost`.
    fn cost_internal(&self,
                     prefilters: &HashMap<PreFilterKind, PreFilterEntry>,
                     stack: &mut Vec<BitVec>,
                     p_pfs: &mut HashMap<PreFilterKind, f64>,
                     p_parse: &mut f64) {
        use self::Operator::*;
        match *self {
            PreFilter { ref kind, ref on_true, ref on_false } => {
                // Add the probability into the map.
                // TODO memoize this.
                let probability = joint_false_positive_rate(stack);
                { // scope borrow of p_pfs
                    let prob = p_pfs.entry(kind.clone()).or_insert(0.0);
                    *prob += probability;
                }

                // Recurse.
                let bitvec = prefilters.get(kind).unwrap().false_positives.clone();
                stack.push(bitvec);
                on_true.cost_internal(prefilters, stack, p_pfs, p_parse);
                let mut bitvec = stack.pop().unwrap();

                bitvec.negate();
                stack.push(bitvec);
                on_false.cost_internal(prefilters, stack, p_pfs, p_parse);
                stack.pop();
            }
            Parse => {
                let probability = joint_false_positive_rate(stack);
                *p_parse += probability;
            }
            Finish => (),
        }
    }
}

#[derive(Debug, Clone)]
struct Optimizer {
    /// Tracks false positives across samples.
    ///
    /// If bit false_positives[i][j] is set, then the candidate pre-filter `i` returned a false
    /// positive for sample `j`. A false positive is defined as a pre-filter which passes a record,
    /// but the callback fails the record.
    prefilters: HashMap<PreFilterKind, PreFilterEntry>,

    /// Parsing function
    parser: ParserCallbackFn,

    /// Record iterator
    record_iterator: RecordIteratorFn,

    /// Measures the average duration of invoking the full parser.
    parse_cost: time::Duration,

    /// Measures the average record length in bytes.
    record_length: usize,

    /// The number of samples tested.
    records_processed: usize,
}

impl Optimizer {
    /// Generates a new `Optimizer` given the candidate prefilter sets.
    fn from(prefilters: Vec<Vec<PreFilterKind>>,
            parser: ParserCallbackFn,
            record_iterator: RecordIteratorFn) -> Optimizer {

        /*
        let mut prefilter_map = HashMap::new();

        for prefilter_set in prefilters.into_iter() {
           // These are the prefilters that are OR'd together. If we choose one of these in a
           // schedule, we should choose all of them.
        }
        */

        Optimizer {
            prefilters: HashMap::new(),
            parser: parser,
            record_iterator: record_iterator,
            parse_cost: time::Duration::nanoseconds(0),
            record_length: 0,
            records_processed: 0,
        }
    }

    /// Reset internal state.
    fn reset(&mut self) {
        self.records_processed = 0;
        self.record_length = 0;
        self.parse_cost = time::Duration::nanoseconds(0);
    }

    /// Sample `data` to obtain measurements of the passthrough rates of each of the prefilters,
    /// the estimated cost of a the parser function, and the average lenght of a record.
    fn sample(&mut self, data: &[u8]) {
        // Reset state before sampling.
        self.reset();
        // The index of the record currently being processed as an offset from `data`.
        let mut base = 0;

        while self.records_processed < MAX_SAMPLES {
            // The record currently being processed.
            let record = unsafe { data.as_ptr().offset(base as isize) };
            // The length of the record.
            let record_length = (self.record_iterator)(record) as usize;

            self.record_length += record_length;

            // Tracks whether a prefilters is found in the current record.
            let mut found = 0;

            // Evaluate each of the candidate pre-filters on the record, recording whether
            // the pre-filter passed the record.
            for (key, entry) in self.prefilters.iter_mut() {
                if key.evaluate(data.get(base..base + record_length).unwrap()) {
                    entry.false_positives.set(self.records_processed, true);
                    found += 1;
                }
            }

            // If all the prefilters were found, we need to run the full parser to check
            // if they are all false positives. If any of them failed, since each one can only
            // return a false positive, all the other ones which passed are false positives.
            if found == self.prefilters.len() {
                let start = time::PreciseTime::now();
                let passed = (self.parser)(record, std::ptr::null_mut()) as i32;
                let end = time::PreciseTime::now();
                self.parse_cost = self.parse_cost + start.to(end);

                if passed != 0 {
                    // the callback passed too, so these aren't false positives; the record actually
                    // passed!
                    for (_, entry) in self.prefilters.iter_mut() {
                        entry.false_positives.set(self.records_processed, false);
                    }
                }
            }

            self.records_processed += 1;

            // Check if we are finished processing the input dataset.
            // If not, proceed to the next record.
            if base + record_length + 1 > data.len() {
                break;
            } else {
                base += record_length + 1;
            }
        }

        // Get the average record length.
        if self.records_processed > 0 {
            self.record_length /= self.records_processed;
        }
    }

    fn optimize(&mut self) {

    }
}

/// Returns a cascade of ordered prefilters to execute using Sparser's optimization algorithm.
pub fn generate_cascade(data: &[u8],
                        prefilters: Vec<Vec<PreFilterKind>>,
                        parser: ParserCallbackFn,
                        rec_iter: RecordIteratorFn) {

    // Initialize an optimizer.
    let mut optimizer = Optimizer::from(prefilters, parser, rec_iter);
    optimizer.sample(data);
    // TODO this should return a plan or something.
    optimizer.optimize();
}

/// Returns the joint false positive rate of the passed masks.
fn joint_false_positive_rate(masks: &Vec<BitVec>) -> f64 {
    if masks.len() == 0 {
        return 1.0;
    }

    let mut joint = masks[0].clone();
    for mask in masks.iter() {
        joint.intersect(mask);
    }

    let set_bits = joint.iter().filter(|x| *x).count();
    (set_bits as f64) / (joint.len() as f64)
}
