use std::time::Duration;

use hdrhistogram::Histogram;

#[derive(Default)]
pub struct Stats {
    pub total_size: u64,
    pub total_duration: Duration,
    pub streams: usize,
    pub stream_stats: StreamStats,
}

impl Stats {
    pub fn stream_finished(&mut self, stream_result: TransferResult) {
        self.total_size += stream_result.size;
        self.streams += 1;

        self.stream_stats
            .duration_hist
            .record(stream_result.duration.as_millis() as u64)
            .unwrap();
        self.stream_stats
            .throughput_hist
            .record(stream_result.throughput as u64)
            .unwrap();
    }

    pub fn print(&self, stat_name: &str) {
        println!("Overall {stat_name} stats:\n");
        println!(
            "Transferred {} bytes on {} streams in {:4.2?} ({:.2} MiB/s)\n",
            self.total_size,
            self.streams,
            self.total_duration,
            throughput_bps(self.total_duration, self.total_size) / 1024.0 / 1024.0
        );

        println!("Stream {stat_name} metrics:\n");

        println!("      │  Throughput   │ Duration ");
        println!("──────┼───────────────┼──────────");

        let print_metric = |label: &'static str, get_metric: fn(&Histogram<u64>) -> u64| {
            println!(
                " {} │ {:7.2} MiB/s │ {:>9.2?}",
                label,
                get_metric(&self.stream_stats.throughput_hist) as f64 / 1024.0 / 1024.0,
                Duration::from_millis(get_metric(&self.stream_stats.duration_hist))
            );
        };

        print_metric("AVG ", |hist| hist.mean() as u64);
        print_metric("P0  ", |hist| hist.value_at_quantile(0.00));
        print_metric("P10 ", |hist| hist.value_at_quantile(0.10));
        print_metric("P50 ", |hist| hist.value_at_quantile(0.50));
        print_metric("P90 ", |hist| hist.value_at_quantile(0.90));
        print_metric("P100", |hist| hist.value_at_quantile(1.00));
    }
}

pub struct StreamStats {
    pub duration_hist: Histogram<u64>,
    pub throughput_hist: Histogram<u64>,
}

impl Default for StreamStats {
    fn default() -> Self {
        Self {
            duration_hist: Histogram::<u64>::new(3).unwrap(),
            throughput_hist: Histogram::<u64>::new(3).unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct TransferResult {
    pub duration: Duration,
    pub size: u64,
    pub throughput: f64,
}

impl TransferResult {
    pub fn new(duration: Duration, size: u64) -> Self {
        let throughput = throughput_bps(duration, size);
        Self {
            duration,
            size,
            throughput,
        }
    }
}

pub fn throughput_bps(duration: Duration, size: u64) -> f64 {
    (size as f64) / (duration.as_secs_f64())
}

/// Per-direction raw counters for the datagram benchmark.
///
/// `sent_*` are filled by the sender; `recv_*` by the receiver. Under `send_mode = drop`
/// the sender may drop oldest queued datagrams, so `recv_*` can be strictly less than
/// `sent_*` — the difference is the loss ratio.
#[derive(Default, Debug, Clone)]
pub struct DatagramCounters {
    pub sent_bytes: u64,
    pub sent_packets: u64,
    pub send_elapsed: Duration,
    pub recv_bytes: u64,
    pub recv_packets: u64,
    pub recv_elapsed: Duration,
}

impl DatagramCounters {
    /// Merge another set of counters (e.g. across multiple client connections).
    pub fn merge(&mut self, other: &Self) {
        self.sent_bytes += other.sent_bytes;
        self.sent_packets += other.sent_packets;
        self.recv_bytes += other.recv_bytes;
        self.recv_packets += other.recv_packets;
        // Use the max elapsed so aggregate throughput isn't inflated by overlapping runs.
        self.send_elapsed = self.send_elapsed.max(other.send_elapsed);
        self.recv_elapsed = self.recv_elapsed.max(other.recv_elapsed);
    }
}

/// A printable datagram benchmark report.
#[derive(Debug, Clone)]
pub struct DatagramReport {
    pub direction: String,
    pub packet_size: usize,
    pub send_mode: String,
    pub congestion: String,
    pub counters: DatagramCounters,
}

impl DatagramReport {
    pub fn print(&self, label: &str) {
        let c = &self.counters;
        let send_mibps = if c.send_elapsed.is_zero() {
            0.0
        } else {
            throughput_bps(c.send_elapsed, c.sent_bytes) / 1024.0 / 1024.0
        };
        let send_pps = if c.send_elapsed.is_zero() {
            0.0
        } else {
            (c.sent_packets as f64) / c.send_elapsed.as_secs_f64()
        };
        let recv_mibps = if c.recv_elapsed.is_zero() {
            0.0
        } else {
            throughput_bps(c.recv_elapsed, c.recv_bytes) / 1024.0 / 1024.0
        };
        let recv_pps = if c.recv_elapsed.is_zero() {
            0.0
        } else {
            (c.recv_packets as f64) / c.recv_elapsed.as_secs_f64()
        };
        // Loss is only computable when the counters hold both sides of the same
        // flood (direction `both`, or a merged aggregate). A pure receiver has
        // `sent_packets == 0` and cannot know how much the peer sent.
        let loss_pct = (c.sent_packets > 0)
            .then(|| (1.0 - (c.recv_packets as f64) / (c.sent_packets as f64)) * 100.0);

        println!();
        println!("{label} datagram stats:");
        println!(
            "  direction={direction} packet-size={ps} send-mode={sm} congestion={cc}",
            direction = self.direction,
            ps = self.packet_size,
            sm = self.send_mode,
            cc = self.congestion,
        );
        if c.sent_packets > 0 {
            println!(
                "  sent:     {sb} bytes ({sp} packets) in {se:?}   -> {smib:.2} MiB/s   {spps:.0} pps",
                sb = c.sent_bytes,
                sp = c.sent_packets,
                se = c.send_elapsed,
                smib = send_mibps,
                spps = send_pps,
            );
        }
        if c.recv_packets > 0 {
            let loss = match loss_pct {
                Some(pct) => format!("   (loss: {pct:.2}%)"),
                None => String::new(),
            };
            println!(
                "  received: {rb} bytes ({rp} packets) in {re:?}   -> {rmib:.2} MiB/s   {rpps:.0} pps{loss}",
                rb = c.recv_bytes,
                rp = c.recv_packets,
                re = c.recv_elapsed,
                rmib = recv_mibps,
                rpps = recv_pps,
            );
        }
    }
}
