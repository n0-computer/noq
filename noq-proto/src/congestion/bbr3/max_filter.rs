use core::fmt::Debug;

const MAX_FILTER_LEN: usize = 3;

/// Tracks the maximum value of a data stream over a fixed time window.
#[derive(Copy, Clone, Debug)]
pub(super) struct MaxFilter {
    window: u64,
    // sample on index 0 has the maximum value followed in descending order
    // by samples on index 1 and then 2
    samples: [MaxSample; MAX_FILTER_LEN],
}

impl MaxFilter {
    pub(super) fn new(window: u64) -> Self {
        Self {
            window,
            samples: [Default::default(); MAX_FILTER_LEN],
        }
    }
    pub(super) fn get_max(&self) -> u64 {
        self.samples[0].value.unwrap_or(0)
    }

    /// Sets how many rounds a sample stays in the window, from the next update on.
    pub(super) fn set_window(&mut self, window: u64) {
        self.window = window;
    }

    /// Update the tracked maximum with a new `measurement` at `current_round`.
    ///
    /// `current_round` represents a sequence number counting upwards from 0 monotonically
    /// `measurement` is what is tracked as the max values over time
    pub(super) fn update_max(&mut self, current_round: u64, measurement: u64) {
        let sample = MaxSample {
            round: current_round,
            value: Some(measurement),
        };

        if self.samples[0].value.is_none()  // uninitialised
            ||  sample.value >= self.samples[0].value // found new max?
            ||  sample.round.saturating_sub(self.samples[2].round) > self.window
        // nothing left in window?
        {
            self.samples.fill(sample); // forget earlier samples
            return;
        }

        if sample.value >= self.samples[1].value {
            self.samples[1] = sample;
            self.samples[2] = sample;
        } else if sample.value >= self.samples[2].value {
            self.samples[2] = sample;
        }

        self.subwin_update(sample);
    }

    /// As time advances, update the 1st, 2nd, and 3rd choices.
    fn subwin_update(&mut self, sample: MaxSample) {
        let dt = sample.round.saturating_sub(self.samples[0].round);
        if dt > self.window {
            /*
             * Passed entire window without a new sample so make 2nd
             * choice the new sample & 3rd choice the new 2nd choice.
             * we may have to iterate this since our 2nd choice
             * may also be outside the window (we checked on entry
             * that the third choice was in the window).
             */
            self.samples[0] = self.samples[1];
            self.samples[1] = self.samples[2];
            self.samples[2] = sample;
            if sample.round.saturating_sub(self.samples[0].round) > self.window {
                self.samples[0] = self.samples[1];
                self.samples[1] = self.samples[2];
                self.samples[2] = sample;
            }
        } else if self.samples[1].round == self.samples[0].round && dt > self.window / 4 {
            /*
             * We've passed a quarter of the window without a new sample
             * so take a 2nd choice from the 2nd quarter of the window.
             */
            self.samples[2] = sample;
            self.samples[1] = sample;
        } else if self.samples[2].round == self.samples[1].round && dt > self.window / 2 {
            /*
             * We've passed half the window without finding a new sample
             * so take a 3rd choice from the last half of the window
             */
            self.samples[2] = sample;
        }
    }
}

impl Default for MaxFilter {
    fn default() -> Self {
        Self {
            window: 10,
            samples: [Default::default(); MAX_FILTER_LEN],
        }
    }
}

/// Tracks the maximum delivery rate over the current and the previous ProbeBW cycle.
///
/// equivalent to the BBR.max_bw filter: "two time slots: the previous ProbeBW cycle and the
/// current ProbeBW cycle". One slot per cycle, as Linux BBRv3 keeps `bw_hi[2]`, holds exactly two
/// cycles, where [`MaxFilter`] with a window of 2 keeps samples for up to three.
/// <https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-07.html#section-5.5.6>
#[derive(Copy, Clone, Debug, Default)]
pub(super) struct MaxBwFilter {
    /// The previous cycle's maximum, then the current cycle's.
    slots: [u64; 2],
}

impl MaxBwFilter {
    /// Returns the maximum over both cycles.
    pub(super) fn get_max(&self) -> u64 {
        Ord::max(self.slots[0], self.slots[1])
    }

    /// Adds a sample to the current cycle.
    pub(super) fn update_max(&mut self, sample: u64) {
        self.slots[1] = Ord::max(self.slots[1], sample);
    }

    /// Starts a new cycle and forgets the oldest one.
    ///
    /// Does nothing while the current cycle has no sample, as in Linux, so a cycle that measured
    /// nothing cannot erase the one before it.
    pub(super) fn advance(&mut self) {
        if self.slots[1] == 0 {
            return;
        }
        self.slots = [self.slots[1], 0];
    }
}

#[derive(Debug, Copy, Clone, Default)]
struct MaxSample {
    /// `round` count, not a timestamp as per <https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-05.html#section-5.5.1>
    /// can also be a count of cycle as per <https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-05.html#section-5.5.6>
    round: u64,
    value: Option<u64>,
}

// Based on Linux kernel code released here
// <https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=f672258391b42a5c7cc2732c9c063e56a85c8dbe>
//
// Kathleen Nichols' algorithm for tracking the maximum
// value of a data stream over some fixed time interval.  (E.g.,
// the maximum Bandwidth achieved over the past 3 rounds.) It uses constant
// space and constant time per update yet almost always delivers
// the same maximum as an implementation that has to keep all the
// data in the window.
//
// The algorithm keeps track of the best, 2nd best & 3rd highest max
// values, maintaining an invariant that the measurement time of
// the n'th best >= n-1'th best. It also makes sure that the three
// values are widely separated in the time window since that bounds
// the worst case error when that data is monotonically increasing
// over the window.
//
// Upon getting a new max, we can forget everything earlier because
// it has no value - the new max is >= everything else in the window
// by definition, and it samples the most recent one. So we restart fresh on
// every new max and overwrites 2nd & 3rd choices. The same property
// holds for 2nd & 3rd best.

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let round = 25;
        let mut max_filter = MaxFilter::default();
        max_filter.update_max(round + 1, 100);
        assert_eq!(100, max_filter.get_max());
        max_filter.update_max(round + 3, 120);
        assert_eq!(120, max_filter.get_max());
        max_filter.update_max(round + 5, 160);
        assert_eq!(160, max_filter.get_max());
        max_filter.update_max(round + 7, 100);
        assert_eq!(160, max_filter.get_max());
        max_filter.update_max(round + 10, 100);
        assert_eq!(160, max_filter.get_max());
        max_filter.update_max(round + 14, 100);
        assert_eq!(160, max_filter.get_max());
        max_filter.update_max(round + 16, 100);
        assert_eq!(100, max_filter.get_max());
        max_filter.update_max(round + 18, 130);
        assert_eq!(130, max_filter.get_max());
    }

    #[test]
    fn max_bw_filter_keeps_two_cycles() {
        let mut filter = MaxBwFilter::default();
        filter.update_max(100);
        filter.advance();
        filter.update_max(50);
        assert_eq!(filter.get_max(), 100);
        // The 100 sample's cycle is now two cycles old.
        filter.advance();
        filter.update_max(10);
        assert_eq!(filter.get_max(), 50);
    }

    #[test]
    fn max_bw_filter_advance_without_a_sample_keeps_the_window() {
        let mut filter = MaxBwFilter::default();
        filter.update_max(100);
        filter.advance();
        filter.advance();
        filter.advance();
        assert_eq!(filter.get_max(), 100);
    }
}
