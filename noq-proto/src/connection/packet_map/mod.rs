//! A sorted map for packet numbers that are inserted in increasing order.

use alloc::collections::BTreeMap;
use core::ops::Range;

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

    /// Inserts `value` at `pn`.
    ///
    /// Fails, leaving the map unchanged, if `pn` is not larger than every packet number
    /// inserted before. The map relies on that order: only the last run ever grows, which keeps
    /// the tree keys valid.
    pub(super) fn insert(&mut self, pn: u64, value: T) -> Result<(), ()> {
        let Some(mut last) = self.runs.last_entry() else {
            self.runs.insert(pn, Span::new(pn, value));
            return Ok(());
        };
        let span = last.get_mut();
        let end = span.last();
        if pn <= end {
            return Err(());
        }
        if pn == end + 1 {
            span.values.push_back(value);
        } else {
            // A skipped packet number: the current last run will not grow any more.
            span.values.shrink(false);
            self.runs.insert(pn, Span::new(pn, value));
        }
        Ok(())
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

    /// Iterates over the entries with packet numbers in `range`, in ascending order.
    pub(super) fn iter_range(&self, range: Range<u64>) -> impl Iterator<Item = (u64, &T)> + '_ {
        let Range { start, end } = range;
        // The run holding `start` may be keyed below it.
        let first = self
            .runs
            .range(..=start)
            .next_back()
            .map_or(start, |(&key, _)| key);
        self.runs
            .range(first..end.max(first))
            .flat_map(move |(_, span)| {
                // The index of `pn` in the run, or 0 if `pn` is below the run.
                let index =
                    |pn: u64| usize::try_from(pn.saturating_sub(span.start)).unwrap_or(usize::MAX);
                span.entries().take(index(end)).skip(index(start))
            })
    }

    /// Iterates over all entries in ascending order.
    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, &T)> + '_ {
        self.runs.values().flat_map(Span::entries)
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

    /// Iterates over the entries of this run in ascending order.
    fn entries(&self) -> impl Iterator<Item = (u64, &T)> + '_ {
        let start = self.start;
        self.values
            .iter()
            .enumerate()
            .map(move |(index, value)| (start + index as u64, value))
    }
}
