use std::fmt::{self, Write};
use std::iter::Sum;
use std::ops::{Add, Range, Sub};

use tinyvec::TinyVec;

/// A set of u64 values optimized for long runs and random insert/delete/contains
///
/// `ArrayRangeSet` uses an array representation, where each array entry represents
/// a range.
///
/// The array-based RangeSet provides 2 benefits:
/// - There exists an inline representation, which avoids the need of heap
///   allocating ACK ranges for SentFrames for small ranges.
/// - Iterating over ranges should usually be faster since there is only
///   a single cache-friendly contiguous range.
///
/// `ArrayRangeSet` is especially useful for tracking ACK ranges where the amount
/// of ranges is usually very low (since ACK numbers are in consecutive fashion
/// unless reordering or packet loss occur).
#[derive(Default, PartialEq, Eq)]
pub(crate) struct ArrayRangeSet<const N: usize = ARRAY_RANGE_SET_INLINE_CAPACITY, T: Default = u64>(
    TinyVec<[Range<T>; N]>,
);

impl<const N: usize, T: fmt::Debug + Default> fmt::Debug for ArrayRangeSet<N, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('[')?;
        let mut first = true;
        for range in self.0.iter() {
            if !first {
                f.write_char(',')?;
            }
            write!(f, "{range:?}")?;
            first = false;
        }
        f.write_char(']')?;
        Ok(())
    }
}

/// The capacity of elements directly stored in [`ArrayRangeSet`]
///
/// An inline capacity of 2 is chosen to keep `SentFrame` below 128 bytes.
pub(crate) const ARRAY_RANGE_SET_INLINE_CAPACITY: usize = 2;

impl<const N: usize> Clone for ArrayRangeSet<N> {
    fn clone(&self) -> Self {
        // tinyvec keeps the heap representation after clones.
        // We rather prefer the inline representation for clones if possible,
        // since clones (e.g. for storage in `SentFrames`) are rarely mutated
        if self.0.is_inline() || self.0.len() > ARRAY_RANGE_SET_INLINE_CAPACITY {
            return Self(self.0.clone());
        }

        let mut vec = TinyVec::new();
        vec.extend_from_slice(self.0.as_slice());
        Self(vec)
    }
}

impl<const N: usize, T> ArrayRangeSet<N, T>
where
    T: Default
        + Clone
        + Copy
        + PartialOrd
        + Ord
        + From<u32>
        + Add<T, Output = T>
        + Sub<T, Output = T>
        + Sum,
{
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = Range<T>> + '_ {
        self.0.iter().cloned()
    }

    pub(crate) fn range_count(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn elts_count(&self) -> T {
        self.0.iter().map(|r| r.end - r.start).sum()
    }

    pub(crate) fn contains(&self, x: T) -> bool {
        for range in self.0.iter() {
            if range.start > x {
                // We only get here if there was no prior range that contained x
                return false;
            } else if range.contains(&x) {
                return true;
            }
        }
        false
    }

    pub(crate) fn iter_range(&self, range: Range<T>) -> impl Iterator<Item = Range<T>> + '_ {
        self.iter().filter_map(move |r| {
            if r.end > range.start && r.start < range.end {
                Some(r.start.max(range.start)..r.end.min(range.end))
            } else {
                None
            }
        })
    }

    pub(crate) fn insert_one(&mut self, x: T) -> bool {
        self.insert(x..x + T::from(1u32))
    }

    pub(crate) fn insert(&mut self, x: Range<T>) -> bool {
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        let mut idx = 0;
        while idx != self.0.len() {
            let range = &mut self.0[idx];

            if range.start > x.end {
                // The range is fully before this range and therefore not extensible.
                // Add a new range to the left
                self.0.insert(idx, x);
                return true;
            } else if range.start > x.start {
                // The new range starts before this range but overlaps.
                // Extend the current range to the left
                // Note that we don't have to merge a potential left range, since
                // this case would have been captured by merging the right range
                // in the previous loop iteration
                result = true;
                range.start = x.start;
            }

            // At this point we have handled all parts of the new range which
            // are in front of the current range. Now we handle everything from
            // the start of the current range

            if x.end <= range.end {
                // Fully contained
                return result;
            } else if x.start <= range.end {
                // Extend the current range to the end of the new range.
                // Since it's not contained it must be bigger
                range.end = x.end;

                // Merge all follow-up ranges which overlap
                while idx != self.0.len() - 1 {
                    let curr = self.0[idx].clone();
                    let next = self.0[idx + 1].clone();
                    if curr.end >= next.start {
                        self.0[idx].end = next.end.max(curr.end);
                        self.0.remove(idx + 1);
                    } else {
                        break;
                    }
                }

                return true;
            }

            idx += 1;
        }

        // Insert a range at the end
        self.0.push(x);
        true
    }

    pub(crate) fn remove(&mut self, x: Range<T>) -> bool {
        let mut result = false;

        if x.is_empty() {
            // Don't try to deal with ranges where x.end <= x.start
            return false;
        }

        let mut idx = 0;
        while idx != self.0.len() && x.start != x.end {
            let range = self.0[idx].clone();

            if x.end <= range.start {
                // The range is fully before this range
                return result;
            } else if x.start >= range.end {
                // The range is fully after this range
                idx += 1;
                continue;
            }

            // The range overlaps with this range
            result = true;

            let left = range.start..x.start;
            let right = x.end..range.end;
            if left.is_empty() && right.is_empty() {
                self.0.remove(idx);
            } else if left.is_empty() {
                self.0[idx] = right;
                idx += 1;
            } else if right.is_empty() {
                self.0[idx] = left;
                idx += 1;
            } else {
                self.0[idx] = right;
                self.0.insert(idx, left);
                idx += 2;
            }
        }

        result
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn pop_min(&mut self) -> Option<Range<T>> {
        if !self.0.is_empty() {
            Some(self.0.remove(0))
        } else {
            None
        }
    }

    pub(crate) fn min(&self) -> Option<T> {
        self.iter().next().map(|x| x.start)
    }

    pub(crate) fn max(&self) -> Option<T> {
        self.iter().next_back().map(|x| x.end - T::from(1))
    }
}

/// Functions which need `Range<T>` to impl IntoIterator for u64.
///
/// `Range<T>` only implements [`IntoIterator`] for types implementing `std::iter::Step`,
/// but that trait is unstable. We can work around this by duplicating these functions for
/// [`u32`] and [`u64`]. Only we don't currently use the u32 version so it is u64-only for
/// now.
impl<const N: usize> ArrayRangeSet<N, u64> {
    pub(crate) fn elts(&self) -> impl Iterator<Item = u64> + '_ {
        self.iter().flatten()
    }
}

#[cfg(test)]
impl proptest::arbitrary::Arbitrary for ArrayRangeSet {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        // Generate 1-8 ranges. Each range is defined by a gap from the previous and a size.
        // We use small values to keep encoding reasonable.
        prop::collection::vec((1u64..100, 1u64..50), 1..8)
            .prop_map(|gaps_and_sizes| {
                let mut ranges = Self::new();
                let mut pos = 0u64;
                for (gap, size) in gaps_and_sizes {
                    let start = pos + gap;
                    let end = start + size;
                    ranges.insert(start..end);
                    pos = end;
                }
                ranges
            })
            .boxed()
    }
}
