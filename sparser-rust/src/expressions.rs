//! Expressions that may be added to Sparser for processing.

use std::ops::{Not, BitAnd, BitOr, BitAndAssign, BitOrAssign};

/// FilterKinds supported by Sparser.
#[derive(Debug, Clone)]
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
    pub fn new_exact_match<S>(t: S) -> FilterKind
        where S: Into<String> {
            FilterKind::ExactMatch(t.into())
        }

    /// Creates a new key-value search filter expression.
    pub fn new_key_value_search<S, T, U>(key: S, value: T, delimiters: Vec<U>) -> FilterKind
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
