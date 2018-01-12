//! Expressions that may be added to Sparser for processing.

use std::ops::{Not, BitAnd, BitOr, BitAndAssign, BitOrAssign};
use std::vec;

/// FilterKinds supported by Sparser.
#[derive(Debug, Clone, PartialEq)]
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

    /// Converts this filter expression to Conjunctive Normal Form as an in-place transformation.
    pub fn to_cnf(&mut self) {
        // Apply DeMorgan's Law repeatedly until fixpoint.
        self.transform(&FilterKind::demorgans_law);
        // Apply the Distribute Law repeatedly until fixpoint.
        self.transform(&FilterKind::distributive_law);
    }

    /// Internal recursive implementation of `filter_sets`.
    fn filter_sets_internal<'a, 'b, 'c>(&'a self,
                                        stack: &'b mut Vec<Vec<&'a FilterKind>>,
                                        result: &'c mut Vec<Vec<&'a FilterKind>>) {
        use self::FilterKind::*;
        match *self {
            And(ref lhs, ref rhs) => {
                lhs.filter_sets_internal(stack, result);
                rhs.filter_sets_internal(stack, result);
            },
            Or(ref lhs, ref rhs) => {
                stack.push(vec![]);
                lhs.filter_sets_internal(stack, result);
                rhs.filter_sets_internal(stack, result);
                result.push(stack.pop().unwrap());
            },
            ref other => {
                if stack.len() == 0 {
                    result.push(vec![&other]);
                } else {
                    stack.last_mut().unwrap().push(&other);
                }
            }
        }
    }

    /// Returns this filter expression as a vector of vectors. Each inner vector represents a
    /// disjunction of filters, where each vector of disjucntions is joined into a conjunction.
    /// 
    /// Notes:
    /// The filter expression should be converted to CNF using `to_cnf` first.
    pub fn filter_sets(&self) -> Vec<Vec<&FilterKind>> {
        let mut result = vec![];
        let mut stack = vec![];
        self.filter_sets_internal(&mut stack, &mut result);

        assert_eq!(stack.len(), 0);
        return result;
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
    /// p & (q | r) to (p & q) | (p & r)
    fn distributive_law(fk: &mut FilterKind) -> Option<FilterKind> {
        use self::FilterKind::*;

        let mut changed = None;
        if let And(ref lhs, ref rhs) = *fk {
            if let Or(ref lhs2, ref rhs2) = *rhs.as_ref() {
                let new_lhs = And(lhs.clone(), lhs2.clone());
                let new_rhs = And(lhs.clone(), rhs2.clone());
                changed = Some(Or(Box::new(new_lhs), Box::new(new_rhs)));
            }
        }
        changed
    }
}

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

    test.to_cnf();
    assert_eq!(test, expect);
}

#[test]
fn basic_and() {
    // ~(a & b)
    let mut test = Not(Box::new(And(boxed_match("a"), boxed_match("b"))));

    // ~a | ~b
    let expect = Or(Box::new(Not(boxed_match("a"))), Box::new(Not(boxed_match("b"))));

    test.to_cnf();
    assert_eq!(test, expect);
}


#[test]
fn basic_distributive() {
    let p = boxed_match("p");
    let q_and_r = Or(boxed_match("q"), boxed_match("r"));
    // p & (q | r)
    let mut test = And(p, Box::new(q_and_r));


    let p_and_q = And(boxed_match("p"), boxed_match("q"));
    let p_and_r = And(boxed_match("p"), boxed_match("r"));
    // (p & q) | (p & r)
    let expect = Or(Box::new(p_and_q), Box::new(p_and_r));

    test.to_cnf();
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
