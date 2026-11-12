//! BBRv3 scenarios driven through a real connection pair.
//!
//! These tests exercise the controller through the transport's own call order (sends, empty
//! polls, ACK processing and loss detection) instead of calling the controller directly.

use alloc::{sync::Arc, vec::Vec};

use super::util::{
    BwLimitConfig, BwLimitedRouting, ConnPair, ConnPairBuilder, Pair, min_opt, subscribe,
};
use crate::{
    Dir, Duration, Instant, PathId, Side::*, StreamId, WriteError, congestion::Bbr3Config,
};

/// One-way latency of the simulated links, for a 40 ms round trip.
const ONE_WAY: Duration = Duration::from_millis(20);
/// Bottleneck rate of the default simulated link: 10 Mbit/s.
const LINK_RATE: u64 = 1_250_000;
/// Size of one video frame: two datagrams at the 1200 byte MTU the tests start with.
const FRAME: usize = 2000;
/// Time between two video frames: 10 frames per second.
const FRAME_INTERVAL: Duration = Duration::from_millis(100);

/// Returns a builder for a pair over a 40 ms link of `link_rate` bytes per second whose client
/// uses BBRv3.
fn bbr3_pair_builder(link_rate: u64) -> ConnPairBuilder {
    let mut builder =
        ConnPair::builder()
            .disable_mtud_discovery()
            .with_routes(BwLimitedRouting::new(
                Pair::CLIENT_ADDR,
                Pair::SERVER_ADDR,
                Instant::now(),
                BwLimitConfig {
                    bytes_per_second: link_rate,
                    buffer_size: 100 * 1500,
                    marks_ce: false,
                    latency: ONE_WAY,
                },
            ));
    let mut bbr3 = Bbr3Config::default();
    // Reproducible probe timing.
    bbr3.probe_rng_seed([3; 16]);
    builder
        .client_transport_cfg
        .congestion_controller_factory(Arc::new(bbr3));
    builder
}

/// Drives both endpoints until `deadline`, recording when the client sent each datagram.
fn drive_until(pair: &mut ConnPair, deadline: Instant, sent_at: &mut Vec<Instant>) {
    loop {
        let before = pair.conn_mut(Client).stats().udp_tx.datagrams;
        pair.drive_client();
        let after = pair.conn_mut(Client).stats().udp_tx.datagrams;
        sent_at.extend((before..after).map(|_| pair.time));
        pair.drive_server();
        match min_opt(pair.client.next_wakeup(), pair.server.next_wakeup()) {
            Some(next) if next <= deadline => pair.time = pair.time.max(next),
            _ => {
                pair.time = deadline;
                return;
            }
        }
    }
}

/// Returns the client's current pacing rate in bytes per second and congestion window.
fn client_rate_and_window(pair: &ConnPair) -> (u64, u64) {
    let metrics = pair
        .conn(Client)
        .congestion_state(PathId::ZERO)
        .expect("path 0 exists")
        .metrics();
    let rate = metrics.pacing_rate.expect("BBRv3 reports a pacing rate");
    (rate, metrics.congestion_window)
}

/// Returns the client's bandwidth estimate in bytes per second.
fn client_bandwidth_estimate(pair: &ConnPair) -> u64 {
    pair.conn(Client)
        .congestion_state(PathId::ZERO)
        .and_then(|cc| cc.metrics().bandwidth_estimate)
        .expect("BBRv3 has a bandwidth estimate")
        / 8
}

/// What the client's controller and pacer did while it sent video frames.
#[derive(Debug)]
struct VideoRun {
    /// Lowest pacing rate after a frame, in bytes per second.
    lowest_rate: u64,
    /// Largest congestion window after a frame, in bytes.
    largest_window: u64,
    /// Frames whose second datagram left after the frame was written, with that delay.
    split_frames: Vec<(u32, Duration)>,
}

/// Sends `frames` video frames from the client, and as many from the server half a frame
/// later, as in a call. The first `warmup` frames are not measured.
fn send_video(pair: &mut ConnPair, frames: u32, warmup: u32) -> VideoRun {
    let stream = pair.streams(Client).open(Dir::Uni).unwrap();
    let reply = pair.streams(Server).open(Dir::Uni).unwrap();

    let start = pair.time;
    let mut sent_at = Vec::new();
    let mut run = VideoRun {
        lowest_rate: u64::MAX,
        largest_window: 0,
        split_frames: Vec::new(),
    };
    for frame in 0..frames {
        let written_at = start + FRAME_INTERVAL * frame;
        drive_until(pair, written_at, &mut sent_at);
        sent_at.clear();
        let written = pair.send_stream(Client, stream).write(&[0; FRAME]).unwrap();
        assert_eq!(written, FRAME);
        drive_until(pair, written_at + FRAME_INTERVAL / 2, &mut sent_at);
        // Everything the client sends before the peer's frame belongs to its own frame.
        let frame_sent_at = core::mem::take(&mut sent_at);
        // The peer's frame makes the client also write ACK-only packets between its frames.
        pair.send_stream(Server, reply).write(&[0; FRAME]).unwrap();
        drive_until(pair, written_at + FRAME_INTERVAL, &mut sent_at);

        if frame < warmup {
            continue;
        }
        let (rate, window) = client_rate_and_window(pair);
        run.lowest_rate = run.lowest_rate.min(rate);
        run.largest_window = run.largest_window.max(window);
        // The frame takes two datagrams. A control frame may add a third, which the pacer may
        // hold back, so only the second datagram's departure counts.
        let mut frame_sent_at = frame_sent_at;
        frame_sent_at.sort();
        let second = *frame_sent_at
            .get(1)
            .unwrap_or_else(|| panic!("frame {frame} sent {frame_sent_at:?}"));
        if second > written_at {
            run.split_frames.push((frame, second - written_at));
        }
    }
    run
}

/// Reads everything the server has received on `stream`, returning the byte count.
fn drain(pair: &mut ConnPair, stream: StreamId) -> usize {
    let mut recv = pair.recv_stream(Server, stream);
    let Ok(mut chunks) = recv.read(true) else {
        return 0;
    };
    let mut received = 0;
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        received += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    received
}

/// Sends `bytes` on a new client stream as fast as the connection allows, returning the time
/// until the server has read all of it.
fn bulk_transfer(pair: &mut ConnPair, bytes: usize) -> Duration {
    let stream = pair.streams(Client).open(Dir::Uni).unwrap();
    let start = pair.time;
    let give_up = start + Duration::from_secs(60);
    let mut remaining = bytes;
    let mut received = 0;
    let mut sent_at = Vec::new();
    while received < bytes {
        assert!(
            pair.time < give_up,
            "bulk transfer stalled at {received} bytes"
        );
        if remaining > 0 {
            let chunk = remaining.min(10_000);
            match pair
                .send_stream(Client, stream)
                .write(&[0; 10_000][..chunk])
            {
                Ok(written) => remaining -= written,
                Err(WriteError::Blocked) => {}
                Err(e) => panic!("write failed: {e}"),
            }
        }
        let deadline = pair.time + Duration::from_millis(5);
        drive_until(pair, deadline, &mut sent_at);
        received += drain(pair, stream);
    }
    pair.time - start
}

/// A low-rate, application-limited flow keeps a pacing rate far above its own rate, and each
/// frame leaves as one burst.
///
/// The flow sends a two-datagram frame every 100 ms over a 40 ms round trip, so it is idle
/// between frames. Every delivery-rate sample is therefore application-limited, which must
/// neither end Startup nor pull `max_bw` down to the application's own rate. The peer sends its
/// own frames, so the client also writes ACK-only packets, which are not application data.
/// <https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-07.html#section-4.1.2.4>
#[test]
fn app_limited_video_keeps_pacing_rate_and_whole_frames() {
    let app_rate = FRAME as f64 / FRAME_INTERVAL.as_secs_f64();

    let _guard = subscribe();
    let mut pair = bbr3_pair_builder(LINK_RATE).connect();
    let run = send_video(&mut pair, 300, 20);

    assert!(
        run.lowest_rate as f64 >= 10.0 * app_rate,
        "pacing rate fell to {} B/s, {:.1}x the application's {app_rate} B/s",
        run.lowest_rate,
        run.lowest_rate as f64 / app_rate
    );
    // Startup grows cwnd only while it is below `max_inflight`, which an application-limited
    // flow's small bandwidth samples keep near a few frames' worth.
    let link_bdp = LINK_RATE as f64 * (2 * ONE_WAY).as_secs_f64();
    assert!(
        run.largest_window as f64 <= 2.0 * link_bdp,
        "cwnd grew to {} bytes on a {app_rate} B/s flow",
        run.largest_window
    );
    assert!(
        run.split_frames.is_empty(),
        "{} frames were split by the pacer, e.g. {:?}",
        run.split_frames.len(),
        &run.split_frames[..run.split_frames.len().min(5)]
    );
}

/// On a slow link the pacer still releases a two-datagram frame as one burst.
///
/// After a bulk transfer has taught BBR a 1 Mbit/s link, it paces at about that rate, where
/// 10 ms of pacing credit is less than one datagram. The pacer bucket holds at least the send
/// quantum of `2 * SMSS`, so a frame written after an idle gap still leaves at once.
/// <https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-07.html#section-5.6.3>
#[test]
fn slow_link_video_frames_leave_whole() {
    const SLOW_LINK: u64 = 125_000;

    let _guard = subscribe();
    let mut pair = bbr3_pair_builder(SLOW_LINK).connect();
    bulk_transfer(&mut pair, 500_000);
    assert!(
        client_bandwidth_estimate(&pair) < 2 * SLOW_LINK,
        "BBR did not learn the slow link"
    );
    let run = send_video(&mut pair, 100, 0);

    assert!(
        run.split_frames.is_empty(),
        "{} of 100 frames were split by the pacer, e.g. {:?}",
        run.split_frames.len(),
        &run.split_frames[..run.split_frames.len().min(5)]
    );
}

/// A bulk transfer fills a 10 Mbit/s bottleneck and learns its rate.
#[test]
fn bulk_transfer_fills_the_link() {
    const BULK: usize = 4_000_000;

    let _guard = subscribe();
    let mut pair = bbr3_pair_builder(LINK_RATE).connect();
    let took = bulk_transfer(&mut pair, BULK);
    let goodput = BULK as f64 / took.as_secs_f64();
    assert!(
        goodput >= 0.85 * LINK_RATE as f64,
        "goodput {goodput:.0} B/s is {:.2} of the {LINK_RATE} B/s link",
        goodput / LINK_RATE as f64
    );
    let learned = client_bandwidth_estimate(&pair);
    assert!(
        learned as f64 >= 0.9 * LINK_RATE as f64,
        "learned {learned} B/s on a {LINK_RATE} B/s link"
    );
}

/// Keepalives on an idle connection do not erase the bandwidth estimate.
///
/// A keepalive PING is one small packet with nothing else in flight. Its delivery-rate sample
/// is application-limited, so it can only raise `max_bw`, never lower it, and it must not end
/// a probe cycle that would age the max-bw filter.
/// <https://github.com/n0-computer/noq/issues/852>
#[test]
fn keepalives_keep_the_bandwidth_estimate() {
    let _guard = subscribe();
    let mut builder = bbr3_pair_builder(LINK_RATE);
    builder
        .client_transport_cfg
        .keep_alive_interval(Some(Duration::from_secs(1)));
    let mut pair = builder.connect();

    bulk_transfer(&mut pair, 2_000_000);
    let mut sent_at = Vec::new();
    let deadline = pair.time + Duration::from_secs(1);
    drive_until(&mut pair, deadline, &mut sent_at);
    let learned = client_bandwidth_estimate(&pair);

    // Five idle minutes, with only keepalives on the wire.
    let deadline = pair.time + Duration::from_secs(300);
    drive_until(&mut pair, deadline, &mut sent_at);
    let after_idle = client_bandwidth_estimate(&pair);
    assert!(
        after_idle as f64 >= 0.9 * learned as f64,
        "bandwidth estimate fell from {learned} to {after_idle} B/s over idle keepalives"
    );
}
