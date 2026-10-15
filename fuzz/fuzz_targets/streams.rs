#![no_main]
#![cfg(fuzzing)]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

extern crate proto;
use proto::fuzzing::{ConnectionState, ResetStream, ResetStreamAt, Retransmits, StreamsState};
use proto::{Dir, Side, StreamId, VarInt};
use proto::{SendStream, Streams};

#[derive(Arbitrary, Debug)]
struct StreamParams {
    side: Side,
    max_remote_uni: u16,
    max_remote_bi: u16,
    send_window: u16,
    receive_window: u16,
    stream_receive_window: u16,
    dir: Dir,
    peer_reset_stream_at: bool,
}

#[derive(Arbitrary, Debug)]
enum Operation {
    Open,
    Accept(Dir),
    Finish(StreamId),
    ReceivedStopSending(StreamId, VarInt),
    ReceivedReset(ResetStream),
    ReceivedResetAt(ResetStreamAt),
    Reset(StreamId),
    ResetAt(StreamId, u64),
}

fuzz_target!(|input: (StreamParams, Vec<Operation>)| {
    let (params, operations) = input;
    let (mut pending, conn_state) = (Retransmits::default(), ConnectionState::established());
    let mut state = StreamsState::new(
        params.side,
        params.max_remote_uni.into(),
        params.max_remote_bi.into(),
        params.send_window.into(),
        params.receive_window.into(),
        params.stream_receive_window.into(),
    );
    state.set_peer_reset_stream_at(params.peer_reset_stream_at);

    for operation in operations {
        match operation {
            Operation::Open => {
                Streams::new(&mut state, &conn_state).open(params.dir);
            }
            Operation::Accept(dir) => {
                Streams::new(&mut state, &conn_state).accept(dir);
            }
            Operation::Finish(id) => {
                let _ = SendStream::new(id, &mut state, &mut pending, &conn_state).finish();
            }
            Operation::ReceivedStopSending(sid, err_code) => {
                Streams::new(&mut state, &conn_state)
                    .state()
                    .received_stop_sending(sid, err_code);
            }
            Operation::ReceivedReset(rs) => {
                let _ = Streams::new(&mut state, &conn_state)
                    .state()
                    .received_reset(rs);
            }
            Operation::ReceivedResetAt(rs) => {
                let _ = Streams::new(&mut state, &conn_state)
                    .state()
                    .received_reset_at(rs);
            }
            Operation::Reset(id) => {
                let _ =
                    SendStream::new(id, &mut state, &mut pending, &conn_state).reset(0u32.into());
            }
            Operation::ResetAt(id, reliable_size) => {
                let _ = SendStream::new(id, &mut state, &mut pending, &conn_state)
                    .reset_at(0u32.into(), reliable_size);
            }
        }
    }
});
