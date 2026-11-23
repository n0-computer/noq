//! A sorted map for packet numbers that are inserted in increasing order.

use alloc::{collections::BTreeMap, vec::Vec};
use core::ops::{Bound, RangeBounds};

use run::Run;

mod run;
#[cfg(test)]
mod tests;

/// A sorted map from packet number to `T`.
///
/// Packet numbers are inserted in increasing order, possibly with gaps, and removed in any
/// order. Memory stays proportional to the number of entries, whatever the gaps between them,
/// because the map is used for packet numbers whose removal a peer controls.
///
/// # Representation
///
/// Values are stored in runs of consecutive packet numbers. Inserting after a gap starts a new
/// run, removing a value from the middle of a run splits it in two, and a run is freed once
/// empty. Runs never merge, because inserts only append. Without loss there is a single run,
/// so the map behaves like one `VecDeque`.
///
/// The tree key of a run is a lower bound for its first packet number, not that number itself:
/// removing the first value of a run only advances [`Span::start`] and leaves the tree alone.
/// The keys stay ordered because no run ever grows towards lower packet numbers, and only the
/// last run grows at all. Every key therefore stays above all packet numbers of the earlier
/// runs.
///
/// # Costs
///
/// - Insert: O(1) when appending to the last run.
/// - Get and remove: O(log runs). Removing from either end of a run moves no values. Removing
///   from the middle moves the smaller side into a new run. A value only moves when it is on
///   the smaller side, so it moves at most about 2 * log2(n) times in its lifetime.
/// - Memory: less than 2 slots per value in every run but the last, which may hold up to 4
///   slots per value, or 16 slots if that is more.
#[derive(Debug)]
pub(super) struct PacketMap<T> {
    runs: BTreeMap<u64, Span<T>>,
}

impl<T> Default for PacketMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PacketMap<T> {
    pub(super) fn new() -> Self {
        Self {
            runs: BTreeMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Inserts `value` at `pn`, replacing any value already stored there.
    ///
    /// `pn` should be larger than every packet number inserted before. A smaller one is still
    /// stored correctly, but only by a slower path.
    pub(super) fn insert(&mut self, pn: u64, value: T) {
        let Some(mut last) = self.runs.last_entry() else {
            self.runs.insert(pn, Span::new(pn, value));
            return;
        };
        let span = last.get_mut();
        let end = span.last();
        debug_assert!(end < pn, "packet number {pn} inserted at or below {end}");
        if end.checked_add(1) == Some(pn) {
            span.values.push_back(value);
        } else if end < pn {
            // A skipped packet number: the current last run will not grow any more.
            span.values.shrink(false);
            self.runs.insert(pn, Span::new(pn, value));
        } else {
            self.insert_out_of_order(pn, value);
        }
    }

    /// Inserts `value` at `pn`, which is at or below the last packet number.
    fn insert_out_of_order(&mut self, pn: u64, value: T) {
        if let Some((&key, span)) = self.runs.range_mut(..=pn).next_back() {
            if let Some(index) = span.index(pn) {
                *span.values.get_mut(index).expect("index is in range") = value;
                return;
            }
            if pn < span.start {
                // `pn` lies between the run's key and its first packet number. Key the run by
                // its first packet number, so that the new run sorts before it.
                let span = self.runs.remove(&key).expect("key was just found");
                self.runs.insert(span.start, span);
            }
        }
        self.runs.insert(pn, Span::new(pn, value));
    }

    pub(super) fn get(&self, pn: u64) -> Option<&T> {
        let (_, span) = self.runs.range(..=pn).next_back()?;
        span.values.get(span.index(pn)?)
    }

    pub(super) fn remove(&mut self, pn: u64) -> Option<T> {
        let last_key = *self.runs.last_key_value()?.0;
        let (&key, span) = self.runs.range_mut(..=pn).next_back()?;
        let index = span.index(pn)?;
        let is_last = key == last_key;
        let len = span.values.len();
        if len == 1 {
            let span = self.runs.remove(&key).expect("key was just found");
            return span.values.into_iter().next();
        }
        let value = if index == 0 {
            span.start += 1;
            span.values.pop_front()
        } else if index == len - 1 {
            span.values.pop_back()
        } else {
            let mut back = span.values.split_off(index);
            let value = back.pop_front();
            // Only the back half can be the last run now.
            span.values.shrink(false);
            back.shrink(is_last);
            self.runs.insert(
                pn + 1,
                Span {
                    start: pn + 1,
                    values: back,
                },
            );
            return value;
        };
        span.values.shrink(is_last);
        value
    }

    /// Removes the values for which `f` returns false.
    ///
    /// `f` is called for every value before anything is removed, so a panic in `f` leaves the
    /// map unchanged apart from what `f` itself modified.
    pub(super) fn retain(&mut self, mut f: impl FnMut(u64, &mut T) -> bool) {
        let mut removed = Vec::new();
        for span in self.runs.values_mut() {
            let start = span.start;
            for (index, value) in span.values.iter_mut().enumerate() {
                let pn = start + index as u64;
                if !f(pn, value) {
                    removed.push(pn);
                }
            }
        }
        for pn in removed {
            self.remove(pn);
        }
    }

    /// Iterates over the entries in `range` in ascending order.
    pub(super) fn iter_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, &T)> + '_ {
        let (runs, lo, hi) = match bounds(range) {
            Some((lo, hi)) => {
                // The run holding `lo` may be keyed below it.
                let first = self
                    .runs
                    .range(..=lo)
                    .next_back()
                    .map_or(lo, |(&key, _)| key);
                (self.runs.range(first..=hi), lo, hi)
            }
            None => (self.runs.range(0..0), 0, 0),
        };
        runs.flat_map(move |(_, span)| {
            let start = span.start;
            // Indices of the values with packet numbers in `lo..=hi`.
            let skip = usize::try_from(lo.saturating_sub(start)).unwrap_or(usize::MAX);
            let end = match hi.checked_sub(start) {
                Some(n) => usize::try_from(n).map_or(usize::MAX, |n| n.saturating_add(1)),
                None => 0,
            };
            span.values
                .iter()
                .enumerate()
                .take(end)
                .skip(skip)
                .map(move |(index, value)| (start + index as u64, value))
        })
    }

    /// Iterates over all entries in ascending order.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, &T)> + '_ {
        self.iter_range(..)
    }

    /// Iterates over all values in ascending order of their packet numbers.
    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
        self.runs
            .values_mut()
            .flat_map(|span| span.values.iter_mut())
    }

    /// Turns the map into an iterator over all entries in ascending order.
    pub(super) fn into_iter(self) -> impl Iterator<Item = (u64, T)> {
        self.runs.into_values().flat_map(|span| {
            let start = span.start;
            span.values
                .into_iter()
                .enumerate()
                .map(move |(index, value)| (start + index as u64, value))
        })
    }
}

/// A run and the packet number of its first value.
#[derive(Debug)]
struct Span<T> {
    start: u64,
    values: Run<T>,
}

impl<T> Span<T> {
    fn new(pn: u64, value: T) -> Self {
        Self {
            start: pn,
            values: Run::One(value),
        }
    }

    /// The packet number of the last value.
    fn last(&self) -> u64 {
        self.start + (self.values.len() - 1) as u64
    }

    /// The index of `pn` in this run, if the run holds it.
    fn index(&self, pn: u64) -> Option<usize> {
        let index = usize::try_from(pn.checked_sub(self.start)?).ok()?;
        (index < self.values.len()).then_some(index)
    }
}

/// Returns the inclusive bounds of `range`, or `None` if it is empty.
fn bounds(range: impl RangeBounds<u64>) -> Option<(u64, u64)> {
    let lo = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.checked_add(1)?,
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.checked_sub(1)?,
        Bound::Unbounded => u64::MAX,
    };
    (lo <= hi).then_some((lo, hi))
}
