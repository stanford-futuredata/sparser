//! Filter expressions that users may add to Sparser.

use super::prefilters::{PreFilterSet, PreFilterKind};

use std::ops::{Not, BitAnd, BitOr, BitAndAssign, BitOrAssign};
use std::vec;

/// FilterKinds supported by Sparser.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FilterKind {
    ExactMatch(String),
    KeyValueSearch {
        key: String,
        value: String,
        delimiters: Vec<String>,
    },

    // Combinations of other filters.
    Or(Box<FilterKind>, Box<FilterKind>),
    And(Box<FilterKind>, Box<FilterKind>),
    Not(Box<FilterKind>),
}

////////////////////////////////////////////////////////////////////////////////
// Inherent Methods
////////////////////////////////////////////////////////////////////////////////

impl FilterKind {
    /// Creates a new exact match filter expression.
    pub fn exact_match<S>(t: S) -> FilterKind
        where S: Into<String> {
            FilterKind::ExactMatch(t.into())
        }

    /// Creates a new key-value search filter expression.
    pub fn key_value_search<S, T, U>(key: S, value: T, delimiters: Vec<U>) -> FilterKind
        where
        S: Into<String>,
        T: Into<String>,
        U: Into<String>, {
            FilterKind::KeyValueSearch {
                key: key.into(),
                value: value.into(),
                delimiters: delimiters.into_iter().map(|e| e.into()).collect(),
            }
        }

    /// Converts this filter expression to Disjunctive Normal Form as an in-place transformation.
    pub fn to_dnf(&mut self) {
        // Apply DeMorgan's Laws repeatedly until fixpoint.
        self.transform(&FilterKind::demorgans_law);
        // Apply the Distribute Law repeatedly until fixpoint.
        self.transform(&FilterKind::distributive_law);
    }

    /// Converts this filter expression as a vector of vectors. Each inner vector represents a
    /// conjunction of filters, where each vector of conjunctions is joined by disjunctions.
    ///
    /// Notes:
    /// The filter expression should be converted to DNF using `to_dnf` first.
    pub fn into_filter_sets(self) -> Vec<Vec<FilterKind>> {
        let mut result = vec![];
        let mut stack = vec![];
        self.into_filter_sets_internal(&mut stack, &mut result);

        assert_eq!(stack.len(), 0);
        return result;
    }

    /// Internal recursive implementation of `into_filter_sets`.
    fn into_filter_sets_internal(self,
                                    stack: &mut Vec<Vec<FilterKind>>,
                                    result: &mut Vec<Vec<FilterKind>>) {
        use self::FilterKind::*;
        match self {
            Or(lhs, rhs) => {
                lhs.into_filter_sets_internal(stack, result);
                rhs.into_filter_sets_internal(stack, result);
            },
            And(lhs, rhs) => {
                stack.push(vec![]);
                lhs.into_filter_sets_internal(stack, result);
                // Nested `Or` expressions may leave empty sets.
                let vec = stack.pop().unwrap();
                if vec.len() != 0 {
                    result.push(vec);
                }

                stack.push(vec![]);
                rhs.into_filter_sets_internal(stack, result);
                let vec = stack.pop().unwrap();
                if vec.len() != 0 {
                    result.push(vec);
                }
            },
            other => {
                if stack.len() == 0 {
                    result.push(vec![other]);
                } else {
                    stack.last_mut().unwrap().push(other);
                }
            }
        }
    }

    /// Returns mutable references to each of the subfilters of `self`.
    fn subfilters_mut(&mut self) -> vec::IntoIter<&mut FilterKind> {
        use self::FilterKind::*;
        match *self {
            Or(ref mut lhs, ref mut rhs) => vec![lhs.as_mut(), rhs.as_mut()],
            And(ref mut lhs, ref mut rhs) => vec![lhs.as_mut(), rhs.as_mut()],
            Not(ref mut child) => vec![child.as_mut()],

            // List explicitly so we don't forget to add new ones.
            ExactMatch(_) | KeyValueSearch { .. } => vec![],
        }.into_iter()
    }

    /// Recursively transforms an expression in place by running a function on it and optionally
    /// replacing it with another expression.
    fn transform<F>(&mut self, func: &F) where F: Fn(&mut FilterKind) -> Option<FilterKind> {
        if let Some(e) = func(self) {
            *self = e;
            return self.transform(func);
        }
        for c in self.subfilters_mut() {
            c.transform(func);
        }
    }

    /// Applies DeMorgan's Laws to this filter expression, returning Some if
    /// the expression changed or None otherwise.
    ///
    /// This function changes
    ///
    /// ~(p & q) to ~p | ~q
    /// and
    /// ~(p | q) to ~p & ~q
    fn demorgans_law(fk: &mut FilterKind) -> Option<FilterKind> {
        use self::FilterKind::*;

        let mut changed = None;
        if let Not(ref child) = *fk {
            match *child.as_ref() {
                And(ref lhs, ref rhs) => {
                    let new_lhs = Not(lhs.clone());
                    let new_rhs = Not(rhs.clone());
                    changed = Some(Or(Box::new(new_lhs), Box::new(new_rhs)));
                }
                Or(ref lhs, ref rhs) => {
                    let new_lhs = Not(lhs.clone());
                    let new_rhs = Not(rhs.clone());
                    changed = Some(And(Box::new(new_lhs), Box::new(new_rhs)));
                }
                _ => (),
            }
        }
        changed
    }

    /// Applies the Distributive Law to this filter expression, returning Some if
    /// the expression changed or None otherwise.
    ///
    /// This function changes
    ///
    /// p | (q & r) to (p | q) & (p | r)
    fn distributive_law(fk: &mut FilterKind) -> Option<FilterKind> {
        use self::FilterKind::*;

        let mut changed = None;
        if let Or(ref lhs, ref rhs) = *fk {
            if let And(ref lhs2, ref rhs2) = *rhs.as_ref() {
                let new_lhs = Or(lhs.clone(), lhs2.clone());
                let new_rhs = Or(lhs.clone(), rhs2.clone());
                changed = Some(And(Box::new(new_lhs), Box::new(new_rhs)));
            }
        }
        changed
    }
}

////////////////////////////////////////////////////////////////////////////////
// PreFilterSet Implementation for FilterKind
////////////////////////////////////////////////////////////////////////////////

/// Convert user-defined filters to pre-filters.
impl PreFilterSet for FilterKind {
    fn prefilter_set(&self) -> Vec<PreFilterKind> {
        use super::prefilters::PreFilterKind::*;
        use super::prefilters::{MAX_CANDIDATES, MAX_WORD_LENGTH};
        let mut candidates = vec![];
        match *self {
            FilterKind::ExactMatch(ref s) => {
                let substrings = all_substrings(s, MAX_WORD_LENGTH);
                let num_to_add = ::std::cmp::min(substrings.len(), MAX_CANDIDATES - candidates.len());
                candidates.extend(substrings.into_iter().take(num_to_add).map(|e| WordSearch(e.into())));
            }
            // Other conversions are currently unsupported.
            _ => unimplemented!()
        }
        candidates
    }
}

/// Returns all substrings of `s` of up to length `max` (and of at least length 2).
/// The returned substrings all fit exactly within a word, i.e., are 2, 4, or 8 bytes long.
fn all_substrings(s: &str, max: usize) -> Vec<&str> {
    let mut substrings = vec![];
    let max = ::std::cmp::min(s.len(), max);

    // Don't search for things that are too small.
    if max < 2 {
        return substrings;
    }

    let mut lens = vec![];

    // Clamp the max to an even number that fits into a single word (2, 4, or 8 bytes).
    if max >= 8 {
        lens.push(8);
    }
    if max >= 4 {
        lens.push(4);
    }
    lens.push(2);

    for len in lens.iter().rev() {
        for start in 0..(s.len() - len + 1) {
            substrings.push(&s[start..start + len]);
        }
    }
    substrings
}

////////////////////////////////////////////////////////////////////////////////
// Operator Overloading for FilterKind
////////////////////////////////////////////////////////////////////////////////

impl Not for FilterKind {
    type Output = FilterKind;

    fn not(self) -> FilterKind {
        FilterKind::Not(Box::new(self))
    }
}

impl BitAnd for FilterKind {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        FilterKind::And(Box::new(self), Box::new(rhs))
    }
}

impl BitAndAssign for FilterKind {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = FilterKind::And(Box::new(self.clone()), Box::new(rhs));
    }
}

impl BitOr for FilterKind {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        FilterKind::Or(Box::new(self), Box::new(rhs))
    }
}

impl BitOrAssign for FilterKind {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = FilterKind::Or(Box::new(self.clone()), Box::new(rhs));
    }
}

#[cfg(test)]
use self::FilterKind::*;

#[cfg(test)]
fn boxed_match<S>(string: S) -> Box<FilterKind>
where S: Into<String> {
    Box::new(ExactMatch(string.into()))
}

#[test]
fn basic_or() {
    // ~(a | b)
    let mut test = Not(Box::new(Or(boxed_match("a"), boxed_match("b"))));

    // ~a & ~b
    let expect = And(Box::new(Not(boxed_match("a"))), Box::new(Not(boxed_match("b"))));

    test.to_dnf();
    assert_eq!(test, expect);
}

#[test]
fn basic_and() {
    // ~(a & b)
    let mut test = Not(Box::new(And(boxed_match("a"), boxed_match("b"))));

    // ~a | ~b
    let expect = Or(Box::new(Not(boxed_match("a"))), Box::new(Not(boxed_match("b"))));

    test.to_dnf();
    assert_eq!(test, expect);
}


#[test]
fn basic_distributive() {
    let p = boxed_match("p");
    let q_or_r = And(boxed_match("q"), boxed_match("r"));
    // p & (q | r)
    let mut test = Or(p, Box::new(q_or_r));


    let p_or_q = Or(boxed_match("p"), boxed_match("q"));
    let p_or_r = Or(boxed_match("p"), boxed_match("r"));
    // (p | q) & (p | r)
    let expect = And(Box::new(p_or_q), Box::new(p_or_r));

    test.to_dnf();
    assert_eq!(test, expect);
}

#[test]
fn with_operators() {
    // p & (q | r)
    let test = FilterKind::exact_match("p") & (FilterKind::exact_match("q") | FilterKind::exact_match("r"));

    let p = boxed_match("p");
    let q_and_r = Or(boxed_match("q"), boxed_match("r"));
    // p & (q | r)
    let expect = And(p, Box::new(q_and_r));

    assert_eq!(test, expect);
}

#[test]
fn filter_sets_1() {
    use std::collections::HashSet;

    let p_or_q = Or(boxed_match("p"), boxed_match("q"));
    let r_or_s = Or(boxed_match("r"), boxed_match("s"));
    let t = boxed_match("t");

    // (p | q) & (r | s) & t
    let test = And(Box::new(And(Box::new(p_or_q), Box::new(r_or_s))), t);

    // Get the filter sets as a hashset of hashsets so we can compare them.
    let sets: Vec<_> = test.into_filter_sets().into_iter().map(|e| e.into_iter().collect::<HashSet<_>>()).collect();

    let ref expect1: HashSet<_> = vec![FilterKind::exact_match("p"), FilterKind::exact_match("q")].into_iter().collect();
    let ref expect2: HashSet<_> = vec![FilterKind::exact_match("r"), FilterKind::exact_match("s")].into_iter().collect();
    let ref expect3: HashSet<_> = vec![FilterKind::exact_match("t")].into_iter().collect();

    assert_eq!(sets.len(), 3);
    assert!(sets.contains(expect1));
    assert!(sets.contains(expect2));
    assert!(sets.contains(expect3));
}


#[test]
fn filter_sets_2() {
    use std::collections::HashSet;

    let p_or_q_or_r = Or(Box::new(Or(boxed_match("p"), boxed_match("q"))), boxed_match("r"));
    let t_or_u = Or(boxed_match("t"), boxed_match("u"));
    let v_or_w = Or(boxed_match("v"), boxed_match("w"));
    let x = Not(boxed_match("x"));

    let test = And(Box::new(p_or_q_or_r), Box::new(t_or_u));
    let test = And(Box::new(test), Box::new(v_or_w));
    // (p | q | r) & (t | u) & (v | w) & ~x
    let test = And(Box::new(test), Box::new(x));

    // Get the filter sets as a hashset of hashsets so we can compare them.
    let sets: Vec<_> = test.into_filter_sets().into_iter().map(|e| e.into_iter().collect::<HashSet<_>>()).collect();

    let ref expect1: HashSet<_> = vec![FilterKind::exact_match("p"),
    FilterKind::exact_match("q"),
    FilterKind::exact_match("r")].into_iter().collect();
    let ref expect2: HashSet<_> = vec![FilterKind::exact_match("t"), FilterKind::exact_match("u")].into_iter().collect();
    let ref expect3: HashSet<_> = vec![FilterKind::exact_match("v"), FilterKind::exact_match("w")].into_iter().collect();
    let ref expect4: HashSet<_> = vec![Not(boxed_match("x"))].into_iter().collect();

    assert_eq!(sets.len(), 4);
    assert!(sets.contains(expect1));
    assert!(sets.contains(expect2));
    assert!(sets.contains(expect3));
    assert!(sets.contains(expect4));
}

#[test]
fn all_substrings_1() {
    let input = "12345678";

    let substrs = all_substrings(input, 8);
    assert_eq!(substrs, vec!["12", "23", "34", "45", "56", "67", "78",
               "1234", "2345", "3456", "4567", "5678", "12345678"]);

    let substrs = all_substrings(input, 4);
    assert_eq!(substrs, vec!["12", "23", "34", "45", "56", "67", "78",
               "1234", "2345", "3456", "4567", "5678"]);

    let substrs = all_substrings(input, 2);
    assert_eq!(substrs, vec!["12", "23", "34", "45", "56", "67", "78"]);
}
