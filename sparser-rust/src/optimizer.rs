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

/// Holds data returned by the sampler.
#[derive(Debug, Clone)]
struct SampleData {
    /// Tracks false positives across samples.
    ///
    /// If bit false_positives[i][j] is set, then the candidate pre-filter `i` returned a false
    /// positive for sample `j`. A false positive is defined as a pre-filter which passes a record,
    /// but the callback fails the record.
    false_positives: Vec<BitVec>,

    /// Measures the average duration of invoking the full parser.
    parse_cost: time::Duration,

    /// Measures the average record length.
    record_length: usize,

    /// The number of samples tested.
    records_processed: usize,
}

impl SampleData {
    /// Generates a new `SampleData` given the number of candidates and samples.
    fn with_size(num_samples: usize, num_candidates: usize) -> SampleData {
        SampleData {
            false_positives: vec![BitVec::from_elem(num_samples, false); num_candidates],
            parse_cost: time::Duration::nanoseconds(0),
            record_length: 0,
            records_processed: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Operator {
    PreFilter {
        kind: PreFilterKind,
        on_true: Box<Operator>,
        on_false: Box<Operator>,
    },
    Parse,
    Finish,
}

/// A plan is just a tree of operators.
type SparserPlan = Operator;

/// Internal recursive helper for `cost`.
fn cost_internal(plan: &SparserPlan,
        probs: &HashMap<PreFilterKind, BitVec>,
        stack: &mut Vec<BitVec>,
        p_pfs: &mut HashMap<PreFilterKind, f64>,
        p_parse: &mut f64) {
    use self::Operator::*;
    match *plan {
        PreFilter { ref kind, ref on_true, ref on_false } => {
            // Add the probability into the map.
            // TODO memoize this.
            let probability = joint_false_positive_rate(stack);
            { // scope borrow of p_pfs
                let prob = p_pfs.entry(kind.clone()).or_insert(0.0);
                *prob += probability;
            }

            // Recurse.
            let bitvec = probs.get(kind).unwrap().clone();
            stack.push(bitvec);
            cost_internal(on_true, probs, stack, p_pfs, p_parse);
            let mut bitvec = stack.pop().unwrap();

            bitvec.negate();
            stack.push(bitvec);
            cost_internal(on_false, probs, stack, p_pfs, p_parse);
            stack.pop();
        }
        Parse => {
            let probability = joint_false_positive_rate(stack);
            *p_parse += probability;
        }
        Finish => (),
    }
}

/// Computes the cost of a sparser plan, which is a tree of operators such as prefilters, parsing,
/// or aborting the computation on the current record.
fn cost(plan: &SparserPlan,
        probs: &HashMap<PreFilterKind, BitVec>,
        parse_cost: f64) -> f64 {
    // Stack to track probabilities, used to compute joint probabilities and nodes in the tree.
    let ref mut stack = vec![];
    // Stores the probabilities of executing a prefilter.
    let ref mut p_pfs = HashMap::new();
    // Probability of parsing the full result.
    let ref mut p_parse = 0.0;

    cost_internal(plan, probs, stack, p_pfs, p_parse);

    let mut total_cost = 0.0;
    for (key, _) in probs.iter() {
        total_cost += p_pfs.get(key).unwrap_or(&0.0) * key.cost(1000);
    }
    total_cost += *p_parse * parse_cost;
    total_cost
}

/// Returns a cascade of ordered prefilters to execute using Sparser's optimization algorithm.
pub fn generate_cascade(data: &[u8],
                        prefilters: &Vec<Vec<PreFilterKind>>,
                        parser: ParserCallbackFn,
                        rec_iter: RecordIteratorFn) {

    // TODO handle multiple filter sets...
    let prefilters = prefilters.get(0).unwrap();
    let _sample_data = sample(data, prefilters, parser, rec_iter);
}

fn sample(data: &[u8],
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
            result.parse_cost = result.parse_cost + start.to(end);

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

/// Computes the cost of a linear cascade `plan`, composed of a linear list of prefilters. The
/// masks represent the false positive bitmasks computed during sampling, and the parse cost is the
/// cost of using the full parser. The cost roughly represents the number of nanoseconds required
/// to process the cascade.
fn cascade_cost(plan: &Vec<PreFilterKind>,
        masks: &Vec<&BitVec>,
        parse_cost: &time::Duration,
        record_length: usize) -> f64 {

    let mut total_cost = 0.0;
    let mut joint = BitVec::from_elem(plan.len(), true);

    for (filter, mask) in plan.iter().zip(masks.iter()) {
        let set_bits = joint.iter().filter(|x| *x).count();
        let joint_probability = (set_bits as f64) / (joint.len() as f64);

        // This is the expected cost of the filter: P[p0,p1,p2..pn] * Cn where
        // pi is the random variable designating whether filter i passed.
        // Cn is the cost of the current filter.
        let cost = filter.cost(record_length) * joint_probability;
        total_cost += cost;

        // Captures the probability of the next filter passing.
        joint.intersect(mask);
    }

    // Factor in the final parsing cost, if all the filters passthrough.
    let set_bits = joint.iter().filter(|x| *x).count();
    let joint_probability = (set_bits as f64) / (joint.len() as f64);

    let parse_cost = (parse_cost.num_nanoseconds().unwrap() as f64) * joint_probability;
    total_cost += parse_cost;
    total_cost
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
