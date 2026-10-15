//! Tests for RESET_STREAM_AT, see
//! <https://datatracker.ietf.org/doc/html/draft-ietf-quic-reliable-stream-reset>

use alloc::{boxed::Box, sync::Arc, vec::Vec};

use assert_matches::assert_matches;
use tracing::info;

use crate::{
    ConnectionHandle, Dir, Event, ReadError, ResetStreamAtError, StreamEvent, StreamId,
    TransportConfig, VarInt,
};

use super::util::{Pair, client_config, server_config, subscribe, validate_incoming};

const MSG: &[u8] = b"header, then body";
const RELIABLE_SIZE: u64 = 6;
const ERROR: VarInt = VarInt(42);

/// Reads everything currently readable from a server stream, and the error that ended the read
fn server_read(pair: &mut Pair, ch: ConnectionHandle, s: StreamId) -> (Vec<u8>, Option<ReadError>) {
    let mut recv = pair.server_recv(ch, s);
    let mut chunks = recv.read(true).unwrap();
    let mut data = Vec::new();
    let err = loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => data.extend_from_slice(&chunk.bytes),
            Ok(None) => break None,
            Err(e) => break Some(e),
        }
    };
    let _ = chunks.finalize();
    (data, err)
}

#[test]
fn delivers_reliable_data() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();

    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    info!("resetting stream");
    pair.client_send(client_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();

    assert_matches!(
        pair.server_conn_mut(server_ch).poll(),
        Some(Event::Stream(StreamEvent::Opened { dir: Dir::Uni }))
    );
    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let (data, err) = server_read(&mut pair, server_ch, s);
    assert_eq!(data, &MSG[..RELIABLE_SIZE as usize]);
    assert_eq!(err, Some(ReadError::Reset(ERROR)));

    let client = pair.client_conn_mut(client_ch);
    assert_eq!(client.stats().frame_tx.reset_stream_at, 1);
    assert_eq!(client.stats().frame_tx.reset_stream, 0);
    assert_eq!(pair.client_streams(client_ch).send_streams(), 0);
    assert_matches!(pair.client_conn_mut(client_ch).poll(), None);
}

#[test]
fn retransmits_reliable_data() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();

    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    pair.drive_client();
    info!("losing stream data");
    pair.server.inbound.clear();

    pair.client_send(client_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();

    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let (data, err) = server_read(&mut pair, server_ch, s);
    assert_eq!(data, &MSG[..RELIABLE_SIZE as usize]);
    assert_eq!(err, Some(ReadError::Reset(ERROR)));
    assert_eq!(pair.client_streams(client_ch).send_streams(), 0);
}

#[test]
fn after_finish() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();

    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    pair.client_send(client_ch, s).finish().unwrap();
    pair.drive_client();
    info!("losing stream data and FIN");
    pair.server.inbound.clear();

    // The FIN hasn't been acknowledged, so the stream can still be reset
    pair.client_send(client_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();

    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let (data, err) = server_read(&mut pair, server_ch, s);
    assert_eq!(data, &MSG[..RELIABLE_SIZE as usize]);
    assert_eq!(err, Some(ReadError::Reset(ERROR)));
    // No `Finished` event for reset streams
    assert_matches!(pair.client_conn_mut(client_ch).poll(), None);
}

#[test]
fn stop_sending_abandons_reliable_data() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();

    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    pair.drive();
    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    pair.server_recv(server_ch, s).stop(VarInt(7)).unwrap();
    pair.drive_server();

    // STOP_SENDING is in flight while resetting
    pair.client_send(client_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();

    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::Stream(StreamEvent::Stopped { id, error_code: VarInt(7) })) if id == s
    );
    assert_eq!(pair.client_streams(client_ch).send_streams(), 0);
}

#[test]
fn not_negotiated() {
    let _guard = subscribe();
    let mut transport = TransportConfig::default();
    transport.reset_stream_at(false);
    let mut server = server_config();
    server.transport_config(Arc::new(transport));
    let mut pair = Pair::new(Default::default(), server);
    let (client_ch, server_ch) = pair.connect();

    // The server didn't advertise support, so the client can't send RESET_STREAM_AT
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    assert_eq!(
        pair.client_send(client_ch, s)
            .reset_at(ERROR, RELIABLE_SIZE),
        Err(ResetStreamAtError::NotNegotiated)
    );
    // The stream is unaffected
    pair.client_send(client_ch, s).finish().unwrap();
    pair.drive();
    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let (data, err) = server_read(&mut pair, server_ch, s);
    assert_eq!(data, MSG);
    assert_eq!(err, None);

    // The client did, so the server can
    let s = pair.server_streams(server_ch).open(Dir::Uni).unwrap();
    pair.server_send(server_ch, s).write(MSG).unwrap();
    pair.server_send(server_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();
    assert_matches!(pair.client_streams(client_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let mut recv = pair.client_recv(client_ch, s);
    let mut chunks = recv.read(true).unwrap();
    assert_matches!(chunks.next(usize::MAX), Ok(Some(chunk)) if chunk.bytes == MSG[..RELIABLE_SIZE as usize]);
    assert_matches!(chunks.next(usize::MAX), Err(ReadError::Reset(ERROR)));
    let _ = chunks.finalize();
}

#[test]
fn zero_rtt() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let config = client_config();

    // Establish a connection to get a session ticket
    let client_ch = pair.begin_connect(config.clone());
    pair.drive();
    pair.server.assert_accept();
    let now = pair.time;
    pair.client_conn_mut(client_ch)
        .close(now, VarInt(0), [][..].into());
    pair.drive();

    info!("resuming session");
    let client_ch = pair.begin_connect(config);
    assert!(pair.client_conn_mut(client_ch).has_0rtt());
    // Support is remembered from the previous connection
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(MSG).unwrap();
    pair.client_send(client_ch, s)
        .reset_at(ERROR, RELIABLE_SIZE)
        .unwrap();
    pair.drive();

    assert!(pair.client_conn_mut(client_ch).accepted_0rtt());
    let server_ch = pair.server.assert_accept();
    assert_matches!(pair.server_streams(server_ch).accept(Dir::Uni), Some(stream) if stream == s);
    let (data, err) = server_read(&mut pair, server_ch, s);
    assert_eq!(data, &MSG[..RELIABLE_SIZE as usize]);
    assert_eq!(err, Some(ReadError::Reset(ERROR)));
}
