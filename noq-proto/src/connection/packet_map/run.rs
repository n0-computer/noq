//! Storage for the values of one run of consecutive packet numbers.

use alloc::collections::{VecDeque, vec_deque};
use core::{iter, mem, option, slice};

/// Capacity up to which the last run is not shrunk.
///
/// Keeps a small window of packets in flight from reallocating on every packet.
const MIN_LAST_RUN_CAPACITY: usize = 16;

/// The values of a run of consecutive packet numbers, in packet number order.
///
/// A single value is stored inline, so an isolated packet costs no allocation. A run is never
/// empty: the map removes a run instead of taking its last value.
#[derive(Debug)]
pub(super) enum Run<T> {
    One(T),
    Many(VecDeque<T>),
}

impl<T> Run<T> {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(values) => values.len(),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        match self {
            Self::One(value) => (index == 0).then_some(value),
            Self::Many(values) => values.get(index),
        }
    }

    /// Appends `value` after the last value.
    pub(super) fn push_back(&mut self, value: T) {
        match self {
            Self::Many(values) => values.push_back(value),
            Self::One(_) => {
                let Self::One(first) = mem::replace(self, Self::Many(VecDeque::new())) else {
                    unreachable!("matched Self::One above");
                };
                *self = Self::Many(VecDeque::from([first, value]));
            }
        }
    }

    /// Removes the first value, unless it is the only one.
    pub(super) fn pop_front(&mut self) -> Option<T> {
        match self {
            Self::Many(values) if values.len() > 1 => values.pop_front(),
            _ => None,
        }
    }

    /// Removes the last value, unless it is the only one.
    pub(super) fn pop_back(&mut self) -> Option<T> {
        match self {
            Self::Many(values) if values.len() > 1 => values.pop_back(),
            _ => None,
        }
    }

    /// Splits the run, keeping the values before `at` and returning the rest.
    ///
    /// `at` must be within `1..len`. The smaller side moves to a new allocation, so the larger
    /// side keeps the existing one.
    pub(super) fn split_off(&mut self, at: usize) -> Self {
        let Self::Many(values) = self else {
            panic!("a run with one value cannot be split");
        };
        let len = values.len();
        assert!(
            0 < at && at < len,
            "split point {at} out of range for {len} values"
        );
        if at <= len - at {
            let front = match at {
                1 => Self::One(values.pop_front().expect("at < len")),
                _ => Self::Many(values.drain(..at).collect()),
            };
            mem::replace(self, front)
        } else if at == len - 1 {
            Self::One(values.pop_back().expect("at < len"))
        } else {
            Self::Many(values.split_off(at))
        }
    }

    /// Releases unused capacity.
    ///
    /// Only the last run grows, so every other run gives back its spare capacity as soon as
    /// half of it is unused. Copying then costs O(1) values per removal, amortized, and less
    /// than half of each allocation is unused.
    ///
    /// The last run waits until three quarters are unused and keeps room to grow. A window of
    /// packets in flight that hovers around one size then does not reallocate on every packet.
    pub(super) fn shrink(&mut self, is_last: bool) {
        let Self::Many(values) = self else {
            return;
        };
        let (len, capacity) = (values.len(), values.capacity());
        if is_last {
            if capacity > MIN_LAST_RUN_CAPACITY && len <= capacity / 4 {
                values.shrink_to((len * 2).max(MIN_LAST_RUN_CAPACITY));
            }
        } else if len == 1 {
            let value = values.pop_front().expect("len is 1");
            *self = Self::One(value);
        } else if len <= capacity / 2 {
            values.shrink_to_fit();
        }
    }

    pub(super) fn iter(&self) -> iter::Chain<slice::Iter<'_, T>, slice::Iter<'_, T>> {
        let (front, back) = match self {
            Self::One(value) => (slice::from_ref(value), &[][..]),
            Self::Many(values) => values.as_slices(),
        };
        front.iter().chain(back)
    }

    pub(super) fn iter_mut(&mut self) -> iter::Chain<slice::IterMut<'_, T>, slice::IterMut<'_, T>> {
        let (front, back) = match self {
            Self::One(value) => (slice::from_mut(value), &mut [][..]),
            Self::Many(values) => values.as_mut_slices(),
        };
        front.iter_mut().chain(back)
    }

    /// The number of value slots allocated or stored inline.
    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(values) => values.capacity(),
        }
    }
}

impl<T> IntoIterator for Run<T> {
    type Item = T;
    type IntoIter = iter::Chain<option::IntoIter<T>, vec_deque::IntoIter<T>>;

    fn into_iter(self) -> Self::IntoIter {
        let (first, rest) = match self {
            Self::One(value) => (Some(value), VecDeque::new()),
            Self::Many(values) => (None, values),
        };
        first.into_iter().chain(rest)
    }
}
