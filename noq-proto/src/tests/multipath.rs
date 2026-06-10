//! Tests for multipath

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use testresult::TestResult;
use tracing::info;

use crate::{
    ClientConfig, ConnectionId, ConnectionIdGenerator, Endpoint, EndpointConfig, FourTuple,
    LOCAL_CID_COUNT, NetworkChangeHint, PathId, PathStatus, RandomConnectionIdGenerator,
    ServerConfig, Side::*, TransportConfig, cid_queue::CidQueue,
};
use crate::{
    ClosePathError, Dir, Event, PathAbandonReason, PathEvent, StreamEvent, TransportErrorCode,
    n0_nat_traversal,
};

use super::util::{
    ConnPair, ManyToManyRouting, Pair, SimpleFirewallRouting, client_config, min_opt,
    server_config, subscribe,
};

const MAX_PATHS: u32 = 3;

#[test]
fn non_zero_length_cids() {
    let _guard = subscribe();
    let multipath_transport_cfg = Arc::new(TransportConfig {
        max_concurrent_multipath_paths: NonZeroU32::new(3 as _),
        // Assume a low-latency connection so pacing doesn't interfere with the test
        initial_rtt: Duration::from_millis(10),
        ..TransportConfig::default()
    });
    let server_cfg = Arc::new(ServerConfig {
        transport: multipath_transport_cfg.clone(),
        ..server_config()
    });
    let server = Endpoint::new(Default::default(), Some(server_cfg), true);

    struct ZeroLenCidGenerator;

    impl ConnectionIdGenerator for ZeroLenCidGenerator {
        fn generate_cid(&mut self) -> ConnectionId {
            ConnectionId::new(&[])
        }

        fn cid_len(&self) -> usize {
            0
        }

        fn cid_lifetime(&self) -> Option<std::time::Duration> {
            None
        }
    }

    let mut ep_config = EndpointConfig::default();
    ep_config.cid_generator(Arc::new(|| Box::new(ZeroLenCidGenerator)));
    let client = Endpoint::new(Arc::new(ep_config), None, true);

    let mut pair = Pair::new_from_endpoint(client, server);
    let client_cfg = ClientConfig {
        transport: multipath_transport_cfg,
        ..client_config()
    };
    pair.begin_connect(client_cfg);
    pair.drive();
    let accept_err = pair
        .server
        .accepted
        .take()
        .expect("server didn't try connecting")
        .expect_err("server did not raise error for connection");
    match accept_err {
        crate::ConnectionError::TransportError(error) => {
            assert_eq!(error.code, crate::TransportErrorCode::PROTOCOL_VIOLATION);
        }
        _ => panic!("Not a TransportError"),
    }
}

#[test]
fn path_acks() {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let stats = pair.stats(Client);
    assert!(stats.frame_rx.path_acks > 0);
    assert!(stats.frame_tx.path_acks > 0);
}

#[test]
fn path_status() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let prev_status = pair.set_path_status(Client, PathId::ZERO, PathStatus::Backup)?;
    assert_eq!(prev_status, PathStatus::Available);

    // Send the frame to the server
    pair.drive();

    assert_eq!(
        pair.remote_path_status(Server, PathId::ZERO),
        Some(PathStatus::Backup)
    );

    let client_stats = pair.stats(Client);
    assert_eq!(client_stats.frame_tx.path_status_available, 0);
    assert_eq!(client_stats.frame_tx.path_status_backup, 1);
    assert_eq!(client_stats.frame_rx.path_status_available, 0);
    assert_eq!(client_stats.frame_rx.path_status_backup, 0);

    let server_stats = pair.stats(Server);
    assert_eq!(server_stats.frame_tx.path_status_available, 0);
    assert_eq!(server_stats.frame_tx.path_status_backup, 0);
    assert_eq!(server_stats.frame_rx.path_status_available, 0);
    assert_eq!(server_stats.frame_rx.path_status_backup, 1);
    Ok(())
}

#[test]
fn path_close_last_path() {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Closing the last path via the local API is not allowed.
    // Use Connection::close() to end the connection instead.
    assert_matches!(
        pair.close_path(Client, PathId::ZERO, 0u8.into()),
        Err(ClosePathError::LastOpenPath)
    );

    // Connection should still be alive
    assert!(!pair.is_closed(Client));
    assert!(!pair.is_closed(Server));
}

#[test]
fn cid_issued_multipath() {
    let _guard = subscribe();
    const ACTIVE_CID_LIMIT: u64 = crate::cid_queue::CidQueue::LEN as _;
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let client_stats = pair.stats(Client);
    dbg!(&client_stats);

    // The client does not send NEW_CONNECTION_ID frames when multipath is enabled as they
    // are all sent after the handshake is completed.
    assert_eq!(client_stats.frame_tx.new_connection_id, 0);
    assert_eq!(
        client_stats.frame_tx.path_new_connection_id,
        MAX_PATHS as u64 * ACTIVE_CID_LIMIT
    );

    // The server sends NEW_CONNECTION_ID frames before the handshake is completed.
    // Multipath is only enabled *after* the handshake completes.  The first server-CID is
    // not issued but assigned by the client and changed by the server.
    assert_eq!(
        client_stats.frame_rx.new_connection_id,
        ACTIVE_CID_LIMIT - 1
    );
    assert_eq!(
        client_stats.frame_rx.path_new_connection_id,
        (MAX_PATHS - 1) as u64 * ACTIVE_CID_LIMIT
    );
}

#[test]
fn multipath_cid_rotation() {
    let _guard = subscribe();
    const CID_TIMEOUT: Duration = Duration::from_secs(2);

    let cid_generator_factory: fn() -> Box<dyn ConnectionIdGenerator> =
        || Box::new(*RandomConnectionIdGenerator::new(8).set_lifetime(CID_TIMEOUT));

    // Only test cid rotation on server side to have a clear output trace
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .with_server_endpoint_cfg(EndpointConfig {
            connection_id_generator_factory: Arc::new(cid_generator_factory),
            ..Default::default()
        })
        .connect();

    let mut round: u64 = 1;
    let mut stop = pair.time;
    let end = pair.time + 5 * CID_TIMEOUT;

    let mut active_cid_num = CidQueue::LEN as u64 + 1;
    active_cid_num = active_cid_num.min(LOCAL_CID_COUNT);
    let mut left_bound = 0;
    let mut right_bound = active_cid_num - 1;

    while pair.time < end {
        stop += CID_TIMEOUT;
        // Run a while until PushNewCID timer fires
        while pair.time < stop {
            if !pair.step()
                && let Some(time) = min_opt(pair.client.next_wakeup(), pair.server.next_wakeup())
            {
                pair.time = time;
            }
        }
        info!(
            "Checking active cid sequence range before {:?} seconds",
            round * CID_TIMEOUT.as_secs()
        );
        let _bound = (left_bound, right_bound);
        for path_id in 0..MAX_PATHS {
            assert_matches!(pair.conn(Server).active_local_path_cid_seq(path_id), _bound);
        }
        round += 1;
        left_bound += active_cid_num;
        right_bound += active_cid_num;
        pair.drive_server();
    }

    let stats = pair.stats(Server);

    // Server sends CIDs for PathId::ZERO before multipath is negotiated.
    assert_eq!(stats.frame_tx.new_connection_id, (CidQueue::LEN - 1) as u64);

    // For the first batch the PathId::ZERO CIDs have already been sent.
    let initial_batch: u64 = (MAX_PATHS - 1) as u64 * CidQueue::LEN as u64;
    // Each round expires all CIDs, so they all get re-issued.
    let each_round: u64 = MAX_PATHS as u64 * CidQueue::LEN as u64;
    // The final round only pushes one set of CIDs with expires_before, the round is not run
    // to completion to wait for the expiry messages from the client.
    let final_round: u64 = MAX_PATHS as u64;
    let path_new_cids = initial_batch + (round - 2) * each_round + final_round;
    debug_assert_eq!(path_new_cids, 73);
    assert_eq!(stats.frame_tx.path_new_connection_id, path_new_cids);

    // We don't retire any CIDs before multipath is negotiated.
    assert_eq!(stats.frame_tx.retire_connection_id, 0);

    // Server expires the CID of the initial sent by the client.
    assert_eq!(stats.frame_tx.path_retire_connection_id, 1);

    // Client only sends CIDs after multipath is negotiated.
    assert_eq!(stats.frame_rx.new_connection_id, 0);

    // Client does not expire CIDs, only the initial set for all the paths.
    assert_eq!(
        stats.frame_rx.path_new_connection_id,
        MAX_PATHS as u64 * CidQueue::LEN as u64
    );
    assert_eq!(stats.frame_rx.retire_connection_id, 0);

    // Test stops before last batch of retirements is sent.
    let path_retire_cids = MAX_PATHS as u64 * CidQueue::LEN as u64 * (round - 2);
    debug_assert_eq!(path_retire_cids, 60);
    assert_eq!(stats.frame_rx.path_retire_connection_id, path_retire_cids);
}

#[test]
fn issue_max_path_id() -> TestResult {
    let _guard = subscribe();

    // We enable multipath but initially do not allow any paths to be opened.
    // The client is allowed to create more paths immediately.
    let mut builder = ConnPair::builder().enable_multipath();
    builder
        .server_transport_cfg
        .max_concurrent_multipath_paths(1);
    let mut pair = builder.connect();

    pair.drive();
    info!("connected");

    // Server should only have sent NEW_CONNECTION_ID frames for now.
    let server_new_cids = CidQueue::LEN as u64 - 1;
    let mut server_path_new_cids = 0;
    let stats = pair.stats(Server);
    assert_eq!(stats.frame_tx.max_path_id, 0);
    assert_eq!(stats.frame_tx.new_connection_id, server_new_cids);
    assert_eq!(stats.frame_tx.path_new_connection_id, server_path_new_cids);

    // Client should have sent PATH_NEW_CONNECTION_ID frames for PathId::ZERO.
    let client_new_cids = 0;
    let mut client_path_new_cids = CidQueue::LEN as u64;
    assert_eq!(stats.frame_rx.new_connection_id, client_new_cids);
    assert_eq!(stats.frame_rx.path_new_connection_id, client_path_new_cids);

    // Server increases MAX_PATH_ID.
    pair.set_max_concurrent_paths(Server, MAX_PATHS)?;
    pair.drive();
    let stats = pair.stats(Server);

    // Server should have sent MAX_PATH_ID and new CIDs
    server_path_new_cids += (MAX_PATHS as u64 - 1) * CidQueue::LEN as u64;
    assert_eq!(stats.frame_tx.max_path_id, 1);
    assert_eq!(stats.frame_tx.new_connection_id, server_new_cids);
    assert_eq!(stats.frame_tx.path_new_connection_id, server_path_new_cids);

    // Client should have sent CIDs for new paths
    client_path_new_cids += (MAX_PATHS as u64 - 1) * CidQueue::LEN as u64;
    assert_eq!(stats.frame_rx.new_connection_id, client_new_cids);
    assert_eq!(stats.frame_rx.path_new_connection_id, client_path_new_cids);

    Ok(())
}

/// A copy of [`issue_max_path_id`], but reordering the `MAX_PATH_ID` frame
/// that's sent from the server to the client, so that some `NEW_CONNECTION_ID`
/// frames arrive with higher path IDs than the most recently received
/// `MAX_PATH_ID` frame on the client side.
#[test]
fn issue_max_path_id_reordered() -> TestResult {
    let _guard = subscribe();

    // We enable multipath but initially do not allow any paths to be opened.
    // The client is allowed to create more paths immediately.
    let mut builder = ConnPair::builder().enable_multipath();
    builder
        .server_transport_cfg
        .max_concurrent_multipath_paths(1);
    let mut pair = builder.connect();

    pair.drive();
    info!("connected");

    // Server should only have sent NEW_CONNECTION_ID frames for now.
    let server_new_cids = CidQueue::LEN as u64 - 1;
    let mut server_path_new_cids = 0;
    let stats = pair.stats(Server);
    assert_eq!(stats.frame_tx.max_path_id, 0);
    assert_eq!(stats.frame_tx.new_connection_id, server_new_cids);
    assert_eq!(stats.frame_tx.path_new_connection_id, server_path_new_cids);

    // Client should have sent PATH_NEW_CONNECTION_ID frames for PathId::ZERO.
    let client_new_cids = 0;
    let mut client_path_new_cids = CidQueue::LEN as u64;
    assert_eq!(stats.frame_rx.new_connection_id, client_new_cids);
    assert_eq!(stats.frame_rx.path_new_connection_id, client_path_new_cids);

    // Server increases MAX_PATH_ID, but we reorder the frame
    pair.set_max_concurrent_paths(Server, MAX_PATHS)?;
    pair.drive_server();
    // reorder the frames on the incoming side
    pair.reorder_inbound(Client);
    pair.drive();
    let stats = pair.stats(Server);

    // Server should have sent MAX_PATH_ID and new CIDs
    server_path_new_cids += (MAX_PATHS as u64 - 1) * CidQueue::LEN as u64;
    assert_eq!(stats.frame_tx.max_path_id, 1);
    assert_eq!(stats.frame_tx.new_connection_id, server_new_cids);
    assert_eq!(stats.frame_tx.path_new_connection_id, server_path_new_cids);

    // Client should have sent CIDs for new paths
    client_path_new_cids += (MAX_PATHS as u64 - 1) * CidQueue::LEN as u64;
    assert_eq!(stats.frame_rx.new_connection_id, client_new_cids);
    assert_eq!(stats.frame_rx.path_new_connection_id, client_path_new_cids);

    Ok(())
}

#[test]
fn open_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );
    Ok(())
}

#[test]
fn open_path_key_update() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;

    // Do a key-update at the same time as opening the new path.
    pair.force_key_update(Client);

    pair.drive();
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );
    Ok(())
}

/// Client starts opening a path but the server fails to validate the path
///
/// The client should receive an event closing the path.
#[test]
fn open_path_validation_fails_server_side() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let different_addr = FourTuple {
        remote: SocketAddr::new([9, 8, 7, 6].into(), 5),
        local_ip: None,
    };
    assert_ne!(different_addr.remote, Pair::SERVER_ADDR);
    assert_ne!(different_addr.remote, Pair::CLIENT_ADDR);
    let path_id = pair.open_path(Client, different_addr, PathStatus::Available)?;

    // block the server from receiving anything
    while pair.blackhole_step(true, false) {}
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(crate::PathEvent::Abandoned { id, reason: PathAbandonReason::ValidationFailed  })) if id == path_id
    );

    assert!(pair.poll(Server).is_none());
    Ok(())
}

/// Client starts opening a path but the client fails to validate the path
///
/// The server should receive an event close the path
#[test]
fn open_path_validation_fails_client_side() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // make sure the new path cannot be validated using the existing path
    let new_addr = SocketAddr::new([9, 8, 7, 6].into(), 5);
    assert_ne!(new_addr, Pair::SERVER_ADDR);
    assert_ne!(new_addr, Pair::CLIENT_ADDR);
    pair.routes.as_basic_mut().client_addr = new_addr;

    let network_path = FourTuple {
        remote: pair.routes.public_server_addr(),
        local_ip: None,
    };
    let path_id = pair.open_path(Client, network_path, PathStatus::Available)?;

    // Make sure the client's path open makes it through to the server and is processed.
    pair.drive_client();
    pair.drive_server();

    info!("dropping client inbound queue");
    pair.client.inbound.clear();

    // Sever the connection and run it to idle.
    // This makes sure that
    // - path validation can't succeed because path responses don't make it through and
    // - the server needs to decide to close the path on its own, because path abandons
    //   don't make it through.
    while pair.blackhole_step(true, true) {}

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Abandoned { id, reason: PathAbandonReason::ValidationFailed  }))
            if id == path_id
    );
    Ok(())
}

/// Client opens a path, then abandons, then calls open_path_ensure.
///
/// In the end there should be an open path.
#[test]
fn open_path_ensure_after_abandon() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();
    let mut second_client_addr = pair.routes.as_basic().client_addr;
    let mut second_server_addr = pair.routes.as_basic().server_addr;
    second_client_addr.set_port(second_client_addr.port() + 1);
    second_server_addr.set_port(second_server_addr.port() + 1);
    pair.routes = ManyToManyRouting::simple_symmetric(
        [pair.routes.as_basic().client_addr, second_client_addr],
        [pair.routes.as_basic().server_addr, second_server_addr],
    )
    .into();

    let second_path = FourTuple {
        local_ip: Some(second_client_addr.ip()),
        remote: second_server_addr,
    };

    info!("opening path 1");
    let path_id = pair.open_path(Client, second_path, PathStatus::Available)?;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id  })) if id == path_id
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id  })) if id == path_id
    );

    info!("closing path {path_id}");
    pair.close_path(Client, path_id, 0u8.into())?;
    pair.drive();

    // The path should be closed:
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id,
            reason: PathAbandonReason::ApplicationClosed { error_code }
        }))
            if id == path_id && error_code == 0u8.into()
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Abandoned {
            id,
            reason: PathAbandonReason::RemoteAbandoned { error_code }
        }))
            if id == path_id && error_code == 0u8.into()
    );

    pair.drive();

    // The path should be discarded:
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Discarded { id, .. })) if id == path_id
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Discarded { id, .. })) if id == path_id
    );

    info!("opening path 2");
    let (path_id, existed) = pair.open_path_ensure(Client, second_path, PathStatus::Available)?;
    pair.drive();

    assert!(!existed);

    // The path should have been opened:
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id  })) if id == path_id
    );

    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id  })) if id == path_id
    );
    Ok(())
}

#[test]
fn close_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    assert_ne!(path_id, PathId::ZERO);

    let stats0 = pair.stats(Client);
    assert_eq!(stats0.frame_tx.path_abandon, 0);
    assert_eq!(stats0.frame_rx.path_abandon, 0);
    assert_eq!(stats0.frame_tx.max_path_id, 0);
    assert_eq!(stats0.frame_rx.max_path_id, 0);

    info!("closing path 0");
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();

    let stats1 = pair.stats(Client);
    assert_eq!(stats1.frame_tx.path_abandon, 1);
    assert_eq!(stats1.frame_rx.path_abandon, 1);
    assert_eq!(stats1.frame_tx.max_path_id, 1);
    assert_eq!(stats1.frame_rx.max_path_id, 1);
    assert!(stats1.frame_tx.path_new_connection_id > stats0.frame_tx.path_new_connection_id);
    assert!(stats1.frame_rx.path_new_connection_id > stats0.frame_rx.path_new_connection_id);
    Ok(())
}

/// Regression: We never emit [`PathEvent::Established`] after [`PathEvent::Abandoned`].
///
/// It may happen that a PATH_RESPONSE validating a path is received after a
/// PATH_ABANDON (due to reordering or the different path latencies). We used to
/// emit a [`PathEvent::Establish`] *after* [`PathEvent::Abandoned`], which makes
/// no sense. This was fixed, and this test ensures that this doesn't happen again.
#[test]
fn no_establish_after_abandon() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    assert_ne!(path_id, PathId::ZERO);

    // Client sends PATH_CHALLENGE
    pair.drive_client();
    pair.advance_time();
    // Server receives PATH_CHALLENGE and replies with PATH_RESPONSE and its
    // own PATH_CHALLENGE. Server has *not* validated the path yet.
    pair.drive_server();
    pair.advance_time();
    // Client receives PATH_RESPONSE and PATH_CHALLENGE. It has now validated
    // the path, and sends a PATH_RESPONSE. We hold back the packet containing
    // the PATH_RESPONSE.
    pair.drive_client();
    let withheld: Vec<_> = pair.server.inbound.drain(..).collect();
    assert!(
        !withheld.is_empty(),
        "expected the client's PATH_RESPONSE to be in flight"
    );

    // Now abandon the path on the client and deliver the PATH_ABANDON frame to
    // the server.
    pair.close_path(Client, path_id, 0u8.into())?;
    pair.advance_time();
    pair.drive_client();
    pair.drive_server();

    // Now deliver the withheld PATH_RESPONSE. It validates the *already
    // abandoned* path.
    for pkt in withheld {
        pair.server.inbound.push_back(pkt);
    }
    pair.advance_time();
    pair.drive_client();
    pair.drive_server();

    // On the client, we get Established and then Abandoned
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id }))
        if id == path_id
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id,
            reason: PathAbandonReason::ApplicationClosed { .. }
        }))
        if id == path_id
    );
    assert_matches!(pair.poll(Client), None);

    // Server only gets a PathEvent::Abandoned. This is the actual regression test:
    // Before the fix, we would get a PathEvent::Established after Abandoned.
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Abandoned {
            id,
            reason: PathAbandonReason::RemoteAbandoned { .. }
        }))
        if id == path_id
    );
    assert_matches!(pair.poll(Server), None);

    Ok(())
}

#[test]
fn close_last_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    assert_ne!(path_id, PathId::ZERO);

    info!("client closes path 0");
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;

    info!("server closes path 1");
    pair.close_path(Server, PathId(1), 0u8.into())?;

    pair.drive();

    assert!(pair.is_closed(Server));
    assert!(pair.is_closed(Client));
    Ok(())
}

#[test]
fn per_path_observed_address() -> TestResult {
    let _guard = subscribe();
    // create the endpoint pair with both address discovery and multipath enabled
    let transport_cfg = TransportConfig {
        max_concurrent_multipath_paths: NonZeroU32::new(MAX_PATHS),
        address_discovery_role: crate::address_discovery::Role::both(),
        ..TransportConfig::default()
    };
    let mut pair = ConnPair::builder()
        .with_transport_cfg(transport_cfg)
        .connect();

    info!("connected");
    pair.drive();

    // check that the client received the correct address
    let expected_addr = pair.routes.as_basic().client_addr;
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::ObservedAddr{id: PathId::ZERO, addr}))
            if addr == expected_addr
    );
    assert_matches!(pair.poll(Client), None);

    // check that the server received the correct address
    let expected_addr = pair.routes.as_basic().server_addr;
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::ObservedAddr{id: PathId::ZERO, addr}))
            if addr == expected_addr
    );
    assert_matches!(pair.poll(Server), None);

    // simulate a rebind on the client, this will close the current path and open a new one
    let our_addr = pair.routes.as_basic_mut().passive_migration(Client);
    pair.handle_network_change(Client, None);

    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id: PathId(0),
            reason: PathAbandonReason::UnusableAfterNetworkChange
        }))
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id: PathId(1) }))
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::ObservedAddr{ id: PathId(1), addr })) if addr == our_addr
    );

    Ok(())
}

#[test]
fn mtud_on_two_paths() -> TestResult {
    let _guard = subscribe();

    let mut builder = ConnPair::builder()
        .with_mtu(1200) // Start with a small MTU
        .enable_multipath();
    builder.server_transport_cfg.max_idle_timeout = None;
    builder.client_transport_cfg.max_idle_timeout = None;
    let mut pair = builder.connect();

    assert_eq!(pair.conn(Client).path_mtu(PathId::ZERO), 1200);

    // Open a 2nd path.
    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    // Ensure the path opened correctly.
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(crate::PathEvent::Established { id  })) if id == path_id
    );

    // MTU should be 1200 for both paths.
    assert_eq!(pair.conn(Client).path_mtu(PathId::ZERO), 1200);
    assert_eq!(pair.conn(Client).path_mtu(path_id), 1200);

    // The default MtuDiscoveryConfig::upper_bound is 1452, the default
    // MtuDiscoveryConfig::interval is 600s.
    pair.mtu = 1452;
    pair.time += Duration::from_secs(600);
    info!("Bumping MTU to: {}", pair.mtu);
    pair.drive();

    info!("MTU Path 0: {}", pair.conn(Client).path_mtu(PathId::ZERO));
    info!(
        "MTU Path {}: {}",
        path_id,
        pair.conn(Client).path_mtu(path_id)
    );

    // Both paths should have found the new MTU.
    assert_eq!(pair.conn(Client).path_mtu(PathId::ZERO), 1452);
    assert_eq!(pair.conn(Client).path_mtu(path_id), 1452);
    Ok(())
}

/// Closing a path locally may be rejected if this leaves the endpoint without validated paths. For
/// paths closed by the remote, however, a `PATH_ABANDON` frame must be accepted. In
/// particular, it should not kill the connection.
///
/// This is a regression test.
#[test]
fn remote_can_close_last_validated_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    pair.routes.as_basic_mut().passive_migration(Client);
    let route = FourTuple {
        remote: pair.routes.public_server_addr(),
        local_ip: None,
    };
    pair.open_path(Client, route, PathStatus::Available)?;
    pair.drive_client();
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();

    // Neither side of the connection should error on close
    let mut close = None;
    for side in [Client, Server] {
        while let Some(event) = pair.poll(side) {
            if let Event::ConnectionLost { reason } = event {
                close = Some(reason);
            }
        }
        assert_eq!(close, None);
    }

    Ok(())
}

/// With multipath and hint=None, the client defaults to non-recoverable: the old path is closed
/// with PATH_UNSTABLE_OR_POOR and a new path is opened. Data still flows on the new path.
#[test]
fn network_change_multipath_no_hint_replaces_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Simulate a passive migration + network change with no hint
    pair.routes.as_basic_mut().passive_migration(Client);
    pair.handle_network_change(Client, None);

    pair.drive();

    // A new path should be opened and the old one should be closed
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id: PathId(0),
            reason: PathAbandonReason::UnusableAfterNetworkChange
        }))
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id: PathId(1) }))
    );

    // The server sees the old path closed with PATH_UNSTABLE_OR_POOR
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Abandoned {
            id: PathId::ZERO,
            reason: PathAbandonReason::RemoteAbandoned { error_code }
        }))
        if error_code == TransportErrorCode::PATH_UNSTABLE_OR_POOR.into()
    );
    // And then sees the new path
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id: PathId(1) }))
    );
    // Both client and server see the old path as discarded
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Discarded {
            id: PathId::ZERO,
            ..
        }))
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Discarded {
            id: PathId::ZERO,
            ..
        }))
    );

    // Data should flow on the new path
    let s = pair.streams(Client).open(Dir::Uni).unwrap();
    const MSG: &[u8] = b"after network change";
    pair.send_stream(Client, s).write(MSG).unwrap();
    pair.send_stream(Client, s).finish().unwrap();
    pair.drive();

    assert_matches!(
        pair.poll(Server),
        Some(Event::Stream(StreamEvent::Opened { dir: Dir::Uni }))
    );
    assert_matches!(pair.streams(Server).accept(Dir::Uni), Some(stream) if stream == s);
    let mut recv = pair.recv_stream(Server, s);
    let mut chunks = recv.read(false).unwrap();
    assert_matches!(
        chunks.next(usize::MAX),
        Ok(Some(chunk)) if chunk.bytes == MSG
    );
    let _ = chunks.finalize();

    Ok(())
}

/// With two paths open and a selective hint, only the non-recoverable path gets replaced.
/// The recoverable path is kept and pinged for liveness.
#[test]
fn network_change_selective_hint() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open a second path
    let server_addr = pair.routes.public_server_addr();
    let second_path = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );

    // A hint that says PathId::ZERO is recoverable but the second path is not
    #[derive(Debug)]
    struct SelectiveHint(PathId);
    impl NetworkChangeHint for SelectiveHint {
        fn is_path_recoverable(&self, path_id: PathId, _network_path: FourTuple) -> bool {
            path_id == self.0
        }
    }
    let hint = SelectiveHint(PathId::ZERO);

    pair.routes.as_basic_mut().passive_migration(Client);
    pair.handle_network_change(Client, Some(&hint));

    pair.drive();

    // The second path (non-recoverable) should be replaced: a new path opens
    // PathId::ZERO (recoverable) should stay open (no Closed event for it)
    let mut client_events = Vec::new();
    while let Some(event) = pair.poll(Client) {
        client_events.push(event);
    }

    // There should be an Opened event for the replacement path
    assert!(
        client_events
            .iter()
            .any(|e| matches!(e, Event::Path(PathEvent::Established { .. }))),
        "expected an Opened event for the replacement path, got: {client_events:?}"
    );
    // PathId::ZERO should NOT have been closed
    assert!(
        !client_events.iter().any(|e| matches!(
            e,
            Event::Path(PathEvent::Discarded {
                id: PathId::ZERO,
                ..
            })
        )),
        "PathId::ZERO should not have been closed: {client_events:?}"
    );

    Ok(())
}

/// Server-side network change with two paths and a selective hint.
///
/// The non-recoverable path is abandoned, leaving only the recoverable one.
#[test]
fn network_change_server_two_paths_selective_hint() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open a second path from the client side.
    let server_addr = pair.routes.public_server_addr();
    let second_path = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );

    // Hint: The provided PathId is recoverable, others are not.
    #[derive(Debug)]
    struct SelectiveHint(PathId);
    impl NetworkChangeHint for SelectiveHint {
        fn is_path_recoverable(&self, path_id: PathId, _network_path: FourTuple) -> bool {
            path_id == self.0
        }
    }

    // Signal network change without actually changing the server's local address. This
    // means the client will not see an actual network change and keep accepting the packets
    // from the server. If the server's address would change it would discard the server's
    // packets since the server may not migrate.
    pair.handle_network_change(Server, Some(&SelectiveHint(second_path)));

    pair.drive();

    // The non-recoverable path is abandoned on the server. No replacement opens because
    // servers cannot call open_path.
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Abandoned {
            id,
            reason: PathAbandonReason::UnusableAfterNetworkChange,
        })) if id == PathId::ZERO
    );
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Discarded { id, .. })) if id == PathId::ZERO
    );
    assert_matches!(pair.poll(Server), None);

    // The client sees PathId::ZERO abandoned by the remote, then discards it.
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id: PathId::ZERO,
            reason: PathAbandonReason::RemoteAbandoned { .. },
        }))
    );
    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Discarded {
            id: PathId::ZERO,
            ..
        }))
    );
    assert_matches!(pair.poll(Client), None);

    Ok(())
}

/// Server-side network change with a single path and a non-recoverable hint.
///
/// The path cannot be closed because it is the last one.
#[test]
fn network_change_server_single_path_non_recoverable_falls_back() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Hint that says all paths are non-recoverable
    #[derive(Debug)]
    struct NonRecoverableHint;
    impl NetworkChangeHint for NonRecoverableHint {
        fn is_path_recoverable(&self, _path_id: PathId, _network_path: FourTuple) -> bool {
            false
        }
    }

    // Signal network change without actually changing the server's local address. This
    // means the client will not see an actual network change and keep accepting the packets
    // from the server. If the server's address would change it would discard the server's
    // packets since the server may not migrate.
    pair.handle_network_change(Server, Some(&NonRecoverableHint));
    pair.drive();

    // The path should NOT be abandoned. The last open path cannot be closed.
    assert_matches!(pair.poll(Server), None);
    assert_matches!(pair.poll(Client), None);

    Ok(())
}

/// Server-side network change with no hint defaults to recoverable. Both paths stay open.
#[test]
fn network_change_server_no_hint_recovers() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open a second path from the client side.
    let server_addr = pair.routes.public_server_addr();
    let second_path = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );
    assert_matches!(
        pair.poll(Server),
        Some(Event::Path(PathEvent::Established { id })) if id == second_path
    );

    // Signal network change without actually changing the server's local address. This
    // means the client will not see an actual network change and keep accepting the packets
    // from the server. If the server's address would change it would discard the server's
    // packets since the server may not migrate.
    pair.handle_network_change(Server, None);
    pair.drive();

    // No path events: the server defaults to recoverable when no hint is provided.
    // Neither path should be abandoned.
    assert_matches!(pair.poll(Server), None);
    assert_matches!(pair.poll(Client), None);

    Ok(())
}

/// Checks that the deadline given before a path fails to be considered open start only when the
/// first packet is sent.
///
/// This is a regression test. See <https://github.com/n0-computer/noq/issues/435>
#[test]
fn path_open_deadline_is_set_on_send() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;

    // Fast-forward time well past 3×PTO without letting any transmit happen on the new
    // path.
    let far_future = pair.time + Duration::from_secs(5);
    pair.handle_timeout(Client, far_future);

    assert!(
        pair.poll(Client).is_none(),
        "path was abandoned before any challenge was sent (issue #456)"
    );

    // Now let the challenge be sent and the path to be opened.
    pair.time = far_future;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Established { id })) if id == path_id,
        "path should open successfully after the challenge is sent"
    );

    Ok(())
}

#[test]
fn path_scheduling_path_status() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    info!("Setting Path 0 to PathStatus::Backup");
    let prev_status = pair.set_path_status(Client, PathId::ZERO, PathStatus::Backup)?;
    assert_eq!(prev_status, PathStatus::Available);

    // Send the frame to the server
    pair.drive();

    assert_eq!(
        pair.remote_path_status(Server, PathId::ZERO),
        Some(PathStatus::Backup)
    );

    info!("Opening Path 1 with PathStatus::Available");
    let server_addr = pair.routes.public_server_addr();
    let path_1 = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    let stats_path0_t0 = pair.conn_mut(Client).path_stats(PathId::ZERO).unwrap();
    let stats_path1_t0 = pair.conn_mut(Client).path_stats(path_1).unwrap();

    info!("Sending STREAM frame");
    let s = pair.streams(Client).open(Dir::Uni).unwrap();
    pair.send_stream(Client, s).write(b"hello").unwrap();
    pair.drive();

    let stats_path0_t1 = pair.conn_mut(Client).path_stats(PathId::ZERO).unwrap();
    let stats_path1_t1 = pair.conn_mut(Client).path_stats(path_1).unwrap();

    info!("assert");
    assert!((stats_path0_t1.udp_tx.datagrams - stats_path0_t0.udp_tx.datagrams) == 0);
    assert!((stats_path1_t1.udp_tx.datagrams - stats_path1_t0.udp_tx.datagrams) > 0);

    Ok(())
}

#[test]
fn server_abandon_last_verified_path() -> TestResult {
    // The client abandons the last verified path the server has. The server is expected to
    // send PATH_ABANDON on the abandoned path itself in this case.

    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Passively migrate the client and immediately open a second path. This way the client
    // will assume the 2nd path is validated but to the server it will be
    // un-validated. Otherwise the client would not allow closing path 0 since there would
    // be no validated path left over.
    pair.routes.as_basic_mut().passive_migration(Client);
    let route = FourTuple {
        remote: pair.routes.public_server_addr(),
        local_ip: None,
    };
    pair.open_path(Client, route, PathStatus::Available)?;
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();

    // We need to move past the Abandoned and Open events, we really only care about getting
    // the stats from the abandoned path.
    let evt = pair.poll(Server);
    assert!(matches!(
        evt,
        Some(Event::Path(PathEvent::Abandoned { .. }))
    ));
    let evt = pair.poll(Server);
    assert!(matches!(
        evt,
        Some(Event::Path(PathEvent::Established { .. }))
    ));

    let evt = pair.poll(Server);
    let Some(Event::Path(PathEvent::Discarded { path_stats, .. })) = evt else {
        panic!("did not get path discarded event");
    };

    assert_eq!(path_stats.frame_tx.path_abandon, 1);

    Ok(())
}

/// Remote abandons a non-last path: error code is propagated in the event.
#[test]
fn remote_path_abandon_with_remaining_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    let server_addr = pair.routes.public_server_addr();
    let _path_id = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    pair.close_path(Server, PathId::ZERO, 42u8.into())?;
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Path(PathEvent::Abandoned {
            id: PathId::ZERO,
            reason: PathAbandonReason::RemoteAbandoned { error_code }
        })) if error_code == 42u8.into()
    );
    assert!(!pair.is_closed(Client));
    assert!(!pair.is_closed(Server));

    Ok(())
}

/// Remote abandons the last path, no new path opened: connection closes after grace period.
#[test]
fn remote_path_abandon_last_path_closes_connection() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open a second path so we can close path 0 normally
    let server_addr = pair.routes.public_server_addr();
    let _path1 = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    // Close path 0 normally (path 1 remains)
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    // Simulate remote abandoning path 1 (now the client's last path)
    // We use force_remote_abandon because in a real scenario the PATH_ABANDON
    // arrives via a packet on the same path, which auto-creates the path on
    // the receiver if it doesn't exist, making packet-dropping approaches
    // unable to create a true last-path scenario in tests.
    pair.force_remote_abandon(Client, PathId::from(1u8));
    pair.drive();

    // After the grace period (no new path opened), the client should be closed.
    assert!(
        pair.is_closed(Client),
        "client should be closed after grace period expired"
    );

    // Verify the client saw the abandon and connection close events.
    let mut saw_abandon = false;
    let mut saw_close = false;
    while let Some(event) = pair.poll(Client) {
        match event {
            Event::Path(PathEvent::Abandoned {
                reason: PathAbandonReason::RemoteAbandoned { .. },
                ..
            }) => saw_abandon = true,
            Event::ConnectionLost { .. } => saw_close = true,
            _ => {}
        }
    }
    assert!(
        saw_abandon,
        "client should see path abandon event for last path"
    );
    assert!(saw_close, "client should see connection lost event");

    Ok(())
}

/// Remote abandons the last path, client opens a new path within grace period: connection survives.
#[test]
fn remote_path_abandon_last_path_client_opens_new() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open path 1, close path 0 normally
    let server_addr = pair.routes.public_server_addr();
    let _path1 = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    // Simulate remote abandoning path 1 (client's last path)
    pair.force_remote_abandon(Client, PathId::from(1u8));

    // Client opens a new path within the grace period
    let new_path = pair.routes.public_server_addr();
    let new_path_id = pair.open_path(
        Client,
        FourTuple::from_remote(new_path),
        PathStatus::Available,
    )?;
    pair.drive();

    assert!(!pair.is_closed(Client), "client should survive");
    assert!(!pair.is_closed(Server), "server should survive");

    let mut saw_abandon = false;
    let mut saw_opened = false;
    while let Some(event) = pair.poll(Client) {
        match event {
            Event::Path(PathEvent::Abandoned {
                reason: PathAbandonReason::RemoteAbandoned { .. },
                ..
            }) => saw_abandon = true,
            Event::Path(PathEvent::Established { id }) if id == new_path_id => saw_opened = true,
            _ => {}
        }
    }
    assert!(saw_abandon, "client should see abandon for last path");
    assert!(saw_opened, "client should see new path opened");

    Ok(())
}

#[test]
fn abandon_path_data_continues() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Open a second path
    let server_addr = pair.routes.public_server_addr();
    let path1 = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    // Drain open events
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    // Client abandons path 0 (picoquic: `picoquic_abandon_path(cnx_client, 0, 0, "test", time)`)
    info!("client abandons path 0");
    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive();

    // Drain abandon + discard events
    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    // Picoquic verification: both sides should have exactly 1 path remaining.
    // In noq, we check that path 0 is abandoned and path 1 is still alive.
    assert!(
        pair.path_status(Client, path1).is_ok(),
        "client should still have path 1"
    );
    assert!(
        pair.path_status(Server, path1).is_ok(),
        "server should still have path 1"
    );

    // Data should still flow on the remaining path (picoquic sends test_scenario_multipath)
    let s = pair.streams(Client).open(Dir::Uni).unwrap();
    const MSG: &[u8] = b"data after path abandon";
    pair.send_stream(Client, s).write(MSG).unwrap();
    pair.send_stream(Client, s).finish().unwrap();
    pair.drive();

    assert_matches!(
        pair.poll(Server),
        Some(Event::Stream(StreamEvent::Opened { dir: Dir::Uni }))
    );
    assert_matches!(pair.streams(Server).accept(Dir::Uni), Some(stream) if stream == s);
    let mut recv = pair.recv_stream(Server, s);
    let mut chunks = recv.read(false).unwrap();
    assert_matches!(
        chunks.next(usize::MAX),
        Ok(Some(chunk)) if chunk.bytes == MSG
    );
    let _ = chunks.finalize();

    // Connection alive
    assert!(!pair.is_closed(Client));
    assert!(!pair.is_closed(Server));

    Ok(())
}

/// Regression test: a NewIdentifiers reply arriving after a path is abandoned
/// must not result in the frames being queued for transmission in
/// `pending.new_cids`.
#[test]
fn new_identifiers_after_abandon_does_not_panic() -> TestResult {
    use crate::shared::{ConnectionEvent, ConnectionEventInner, IssuedCid};
    use crate::token::ResetToken;

    let _guard = subscribe();
    let mut pair = ConnPair::builder().enable_multipath().connect();

    // A second path is needed so close_path(0) is not the last open path.
    let server_addr = pair.routes.public_server_addr();
    let _path1 = pair.open_path(
        Client,
        FourTuple::from_remote(server_addr),
        PathStatus::Available,
    )?;
    pair.drive();

    let cid_seq_before = pair.conn(Client).active_local_path_cid_seq(0);

    pair.close_path(Client, PathId::ZERO, 0u8.into())?;
    pair.drive_client();
    pair.drive_server();
    pair.drive_client();

    // Inject a NewIdentifiers reply for the just-abandoned path.
    let synthetic_seq = cid_seq_before.1 + 1;
    let issued = vec![IssuedCid {
        path_id: PathId::ZERO,
        sequence: synthetic_seq,
        id: ConnectionId::new(&[0xAAu8; 8]),
        reset_token: ResetToken::from([0u8; crate::RESET_TOKEN_SIZE]),
    }];
    let late_event = ConnectionEvent(ConnectionEventInner::NewIdentifiers(
        issued, pair.time, 8, None,
    ));
    pair.handle_event(Client, late_event);

    // The CID must not have been added to local_cid_state, otherwise it would be
    // queued in `pending.new_cids` and later sent as a NEW_CONNECTION_ID frame
    // for an abandoned path.
    let cid_seq_after = pair.conn(Client).active_local_path_cid_seq(0);
    assert_eq!(cid_seq_before, cid_seq_after);

    Ok(())
}

/// Ported from picoquic `multipath_test_ab1`. Abandon + reopen cycle, 3 rounds.
#[test]
fn abandon_cycle() -> TestResult {
    let _guard = subscribe();

    let mut pair = ConnPair::builder().enable_multipath().connect();

    // Set up addresses for multiple paths
    let routing = pair.routes.as_basic();
    let mut addrs_client = vec![routing.client_addr];
    let mut addrs_server = vec![routing.server_addr];
    for i in 1..6u16 {
        let mut ca = routing.client_addr;
        ca.set_port(ca.port() + i);
        addrs_client.push(ca);
        let mut sa = routing.server_addr;
        sa.set_port(sa.port() + i);
        addrs_server.push(sa);
    }
    pair.routes =
        ManyToManyRouting::simple_symmetric(addrs_client.clone(), addrs_server.clone()).into();

    // Cycle: open a second path, abandon path 0, verify cleanup, repeat with new paths.
    // Each cycle uses a fresh pair of addresses.
    let mut current_path = PathId::ZERO;
    for cycle in 0..3u16 {
        let addr_idx = (cycle as usize) + 1;
        let new_path_net = FourTuple {
            local_ip: Some(addrs_client[addr_idx].ip()),
            remote: addrs_server[addr_idx],
        };

        info!("cycle {cycle}: opening new path on addr index {addr_idx}");
        let new_path = pair.open_path(Client, new_path_net, PathStatus::Available)?;
        pair.drive();

        // Drain events
        while pair.poll(Client).is_some() {}
        while pair.poll(Server).is_some() {}

        info!("cycle {cycle}: abandoning path {current_path}");
        pair.close_path(Client, current_path, 0u8.into())?;
        pair.drive();

        // Drain events (abandon + discard)
        while pair.poll(Client).is_some() {}
        while pair.poll(Server).is_some() {}

        // Verify the abandoned path is gone and the new path remains
        assert!(
            pair.path_status(Client, current_path).is_err(),
            "cycle {cycle}: abandoned path should be gone"
        );
        assert!(
            pair.path_status(Client, new_path).is_ok(),
            "cycle {cycle}: new path should be alive"
        );

        // Verify connection is alive
        assert!(
            !pair.is_closed(Client),
            "cycle {cycle}: client should be alive"
        );
        assert!(
            !pair.is_closed(Server),
            "cycle {cycle}: server should be alive"
        );

        // Picoquic verifies CID stash has >= 2 entries; we verify data still works.
        let s = pair.streams(Client).open(Dir::Uni).unwrap();
        let msg = format!("cycle {cycle}");
        pair.send_stream(Client, s).write(msg.as_bytes()).unwrap();
        pair.send_stream(Client, s).finish().unwrap();
        pair.drive();

        // Server should receive the data
        assert_matches!(
            pair.poll(Server),
            Some(Event::Stream(StreamEvent::Opened { dir: Dir::Uni }))
        );
        assert_matches!(pair.streams(Server).accept(Dir::Uni), Some(stream) if stream == s);
        let mut recv = pair.recv_stream(Server, s);
        let mut chunks = recv.read(false).unwrap();
        assert_matches!(
            chunks.next(usize::MAX),
            Ok(Some(chunk)) if chunk.bytes == msg.as_bytes()
        );
        let _ = chunks.finalize();

        current_path = new_path;
    }

    Ok(())
}

/// NAT traversal round revalidates an existing path via new PATH_CHALLENGE.
#[test]
fn nat_traversal_revalidates_existing_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .connect();

    let server_addr = pair.routes.as_basic().server_addr;
    let client_addr = pair.routes.as_basic().client_addr;

    pair.add_nat_traversal_address(Server, server_addr)?;
    pair.add_nat_traversal_address(Client, client_addr)?;
    pair.drive();

    let probed = pair.initiate_nat_traversal_round(Client)?;
    assert_eq!(probed.len(), 1);
    assert_eq!(probed[0], server_addr);
    pair.drive();

    assert_eq!(
        pair.path_status(Client, PathId::ZERO)?,
        PathStatus::Available
    );

    let challenges_before = pair.stats(Client).frame_tx.path_challenge;

    // Second round with the same addresses should trigger revalidation
    let probed = pair.initiate_nat_traversal_round(Client)?;
    assert_eq!(probed.len(), 1);
    pair.drive_bounded(20);

    let challenges_after = pair.stats(Client).frame_tx.path_challenge;
    assert!(
        challenges_after > challenges_before,
        "expected new PATH_CHALLENGE for existing path \
         (before={challenges_before}, after={challenges_after})"
    );

    Ok(())
}

/// After a silent gap, PTO backs off exponentially and can reach minutes.
/// The 2s PTO cap ensures recovery happens promptly once connectivity returns.
#[test]
fn path_recovers_after_silent_gap_via_keepalive() -> TestResult {
    let _guard = subscribe();

    let mut builder = ConnPair::builder().enable_multipath();
    builder
        .server_transport_cfg
        .default_path_max_idle_timeout(Some(Duration::from_secs(60)));
    builder
        .client_transport_cfg
        .default_path_max_idle_timeout(Some(Duration::from_secs(60)));
    let mut pair = builder.connect();

    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    let s = pair.streams(Server).open(Dir::Uni).unwrap();
    pair.send_stream(Server, s).write(&[42u8; 5000]).unwrap();
    pair.drive();

    assert_matches!(
        pair.poll(Client),
        Some(Event::Stream(StreamEvent::Opened { dir: Dir::Uni }))
    );
    assert_matches!(pair.streams(Client).accept(Dir::Uni), Some(stream) if stream == s);
    let mut recv = pair.recv_stream(Client, s);
    let mut chunks = recv.read(false).unwrap();
    let mut total_read = 0;
    while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        total_read += chunk.bytes.len();
    }
    let _ = chunks.finalize();
    info!("read {total_read} bytes before gap");
    assert!(total_read > 0, "should have received initial data");

    while pair.poll(Client).is_some() {}
    while pair.poll(Server).is_some() {}

    pair.send_stream(Server, s).write(&[43u8; 5000]).unwrap();

    info!("starting silent gap");
    let gap_start = pair.time;
    for _ in 0..10 {
        if !pair.blackhole_step(true, true) {
            break;
        }
    }
    let gap_duration = pair.time - gap_start;
    info!("gap lasted {:?}", gap_duration);

    pair.send_stream(Server, s).write(b"after gap").unwrap();
    pair.send_stream(Server, s).finish().unwrap();

    info!("gap ended, driving to recovery");
    let mut received_post_gap = false;
    for i in 0..50 {
        if pair.is_closed(Client) || pair.is_closed(Server) {
            info!("connection died at step {i}");
            break;
        }
        pair.step();

        while let Some(event) = pair.poll(Client) {
            if matches!(&event, Event::Stream(StreamEvent::Readable { .. })) {
                info!("client received data at step {i}");
                received_post_gap = true;
            }
        }
        if received_post_gap {
            break;
        }
    }

    assert!(!pair.is_closed(Client), "client should survive the gap");
    assert!(!pair.is_closed(Server), "server should survive the gap");
    assert!(
        received_post_gap,
        "client should receive data after the gap recovers"
    );

    Ok(())
}

/// Tests NAT traversal manages to open a 2nd path.
#[test]
fn test_simple_nat_traveral_opens_path() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .with_routes(SimpleFirewallRouting::new().into())
        .connect();

    info!("adding addrs");
    pair.add_nat_traversal_address(Server, SimpleFirewallRouting::SERVER_FW_ADDR)?;
    pair.add_nat_traversal_address(Client, SimpleFirewallRouting::CLIENT_FW_ADDR)?;
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(
        event,
        Event::NatTraversal(n0_nat_traversal::Event::AddressAdded(_))
    );

    info!("init NAT traversal");
    pair.initiate_nat_traversal_round(Client)?;

    // Ensure we have no more events queued
    assert_matches!(pair.poll(Client), None);
    assert_matches!(pair.poll(Server), None);

    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    let event = pair.poll(Server).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    Ok(())
}

/// Test that a PATH_CHALLENGE is added to a PATH_RESPONSE for NAT traversal.
#[test]
fn test_simple_nat_traversal_challenge_with_response() -> TestResult {
    let _guard = subscribe();
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .connect();

    info!("setting routes, adding addrs");
    pair.routes = SimpleFirewallRouting::new().into();
    pair.add_nat_traversal_address(Server, SimpleFirewallRouting::SERVER_FW_ADDR)?;
    pair.add_nat_traversal_address(Client, SimpleFirewallRouting::CLIENT_FW_ADDR)?;
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(
        event,
        Event::NatTraversal(n0_nat_traversal::Event::AddressAdded(_))
    );

    info!("init NAT traversal");
    pair.initiate_nat_traversal_round(Client)?;

    // Ensure we have no more events queued
    assert_matches!(pair.poll(Client), None);
    assert_matches!(pair.poll(Server), None);

    // Client sends probe (blocked) + REACH_OUT, server send probe. Both firewalls open.
    pair.step();

    // Client receives probe, includes its own challenge with the response.
    let stats0 = pair.stats(Client);
    pair.step();
    let stats1 = pair.stats(Client);

    // Without the challenge-with-response only a PATH_RESPONSE would have been sent.
    assert_eq!(
        stats1.frame_tx.path_response - stats0.frame_tx.path_response,
        1
    );
    assert_eq!(
        stats1.frame_tx.path_challenge - stats0.frame_tx.path_challenge,
        1
    );

    // Continue till the end.
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    let event = pair.poll(Server).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    Ok(())
}

/// Tests a "very easy NAT" for the server with a "hard NAT" for the client.
///
/// Here "very easy NAT" is an EIM+EIF NAT. The port is opened by the QAD probe. "hard NAT"
/// is an EDM NAT.
#[test]
fn test_hard_nat_client_opens_path() -> TestResult {
    let _guard = subscribe();
    let mut routing = SimpleFirewallRouting::new();
    // By configuring the server side to be open it emulates an EIM+EIF NAT: the QAD probe
    // already opened the firewall.
    routing.server_firewall_open = true;
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .with_routes(routing.into())
        .connect();

    info!("adding addrs");
    pair.add_nat_traversal_address(Server, SimpleFirewallRouting::SERVER_FW_ADDR)?;
    // By adding a dummy address the client can start NAT traversal but does not advertise
    // its real public interface, thus emulating a hard NAT. Choose the last addr in the
    // client subnet.
    let dummy_addr: SocketAddr = "[::1:ffff]:1".parse()?;
    pair.add_nat_traversal_address(Client, dummy_addr)?;
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(
        event,
        Event::NatTraversal(n0_nat_traversal::Event::AddressAdded(_))
    );

    info!("init NAT traversal");
    pair.initiate_nat_traversal_round(Client)?;

    // Ensure we have no more events queued
    assert_matches!(pair.poll(Client), None);
    assert_matches!(pair.poll(Server), None);

    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    let event = pair.poll(Server).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    Ok(())
}

/// Tests a "very easy NAT" for the client with a "hard NAT" for the server.
///
/// Here "very easy NAT" is an EIM+EIF NAT. The port is opened by the QAD probe. "hard NAT"
/// is an EDM NAT.
#[test]
fn test_hard_nat_server_opens_path() -> TestResult {
    let _guard = subscribe();
    let mut routing = SimpleFirewallRouting::new();
    // By configuring the client side to be open it emulates an EIM+EIF NAT: the QAD probe
    // already opened the firewall.
    routing.client_firewall_open = true;
    let mut pair = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .with_routes(routing.into())
        .connect();

    info!("adding addrs");
    // By adding a dummy address the client can start NAT traversal but does not advertise
    // its real public interface, thus emulating a hard NAT. Choose the last addr in the
    // server subnet.
    let dummy_addr: SocketAddr = "[::2:ffff]:1".parse()?;
    pair.add_nat_traversal_address(Server, dummy_addr)?;
    pair.add_nat_traversal_address(Client, SimpleFirewallRouting::CLIENT_FW_ADDR)?;
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(
        event,
        Event::NatTraversal(n0_nat_traversal::Event::AddressAdded(_))
    );

    info!("init NAT traversal");
    pair.initiate_nat_traversal_round(Client)?;

    // Ensure we have no more events queued
    assert_matches!(pair.poll(Client), None);
    assert_matches!(pair.poll(Server), None);

    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    let event = pair.poll(Server).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    Ok(())
}

/// If the client is not allowed to migrate, it should still be allowed to send NAT
/// traversal probes and be able to hole-punch.
#[test]
fn test_peer_may_probe() -> TestResult {
    let _guard = subscribe();

    let builder = ConnPair::builder()
        .enable_multipath()
        .enable_nat_traversal()
        .disable_mtud_discovery()
        .with_routes(SimpleFirewallRouting::new().into());
    let server_cfg = ServerConfig {
        transport: Arc::new(builder.server_transport_cfg.clone()),
        migration: false,
        ..server_config()
    };
    let mut pair = builder.with_server_cfg(server_cfg).connect();

    pair.add_nat_traversal_address(Server, SimpleFirewallRouting::SERVER_FW_ADDR)?;
    pair.add_nat_traversal_address(Client, SimpleFirewallRouting::CLIENT_FW_ADDR)?;
    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(
        event,
        Event::NatTraversal(n0_nat_traversal::Event::AddressAdded(_))
    );

    info!("init NAT traversal");
    pair.initiate_nat_traversal_round(Client)?;

    // Ensure we have no more events queued
    assert_matches!(pair.poll(Client), None);
    assert_matches!(pair.poll(Server), None);

    pair.drive();

    let event = pair.poll(Client).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    let event = pair.poll(Server).expect("should have event");
    assert_matches!(event, Event::Path(PathEvent::Established { .. }));

    Ok(())
}

#[test]
fn on_path_challenge_lost_backoff() {
    let _guard = subscribe();

    let mut pair = ConnPair::default();

    // We use two helpers so we can "skip over" unrelated stopping points from `next_wakeup`

    /// Attempts to drive the client until it has sent the expected amount of path challenges.
    ///
    /// Tries for a maximum of 10 iterations. Panics if that fails.
    fn drive_client_until_challenge_sent(pair: &mut ConnPair, expected_path_challenges_sent: u64) {
        for _ in 0..10 {
            pair.drive_client();
            if pair.stats(Client).frame_tx.path_challenge == expected_path_challenges_sent {
                return;
            }
            pair.time = pair
                .client
                .next_wakeup()
                .expect("couldn't drive client forward");
            info!("advancing to {:?} for client", pair.time - pair.epoch);
        }
        panic!(
            "client never sent PATH_CHALLENGE #{}, actual: {}",
            expected_path_challenges_sent,
            pair.stats(Client).frame_tx.path_challenge
        );
    }

    /// Attempts to drive the server until it has received the expected amount of path challenges.
    ///
    /// Tries for a maximum of 10 iterations. Panics if that fails.
    fn drive_server_until_challenge_received(
        pair: &mut ConnPair,
        expected_path_challenges_received: u64,
    ) {
        for _ in 0..10 {
            // attempt to drive the server until it receives the PATH_CHALLENGE and sends a PATH_RESPONSE
            pair.time = pair
                .server
                .next_wakeup()
                .expect("couldn't drive server forward");
            info!("advancing to {:?} for server", pair.time - pair.epoch);
            pair.drive_server();
            if pair.stats(Server).frame_rx.path_challenge == expected_path_challenges_received
                && pair.stats(Server).frame_tx.path_response == expected_path_challenges_received
            {
                return;
            }
        }
        panic!(
            "server never received PATH_CHALLENGE #{}, actual: {}",
            expected_path_challenges_received,
            pair.stats(Server).frame_rx.path_challenge
        );
    }

    pair.conn_mut(Client).trigger_path_validation();

    // Kickstart the process once to get a reference point for the last_challenge_sent
    drive_client_until_challenge_sent(&mut pair, 1);
    let mut last_challenge_send = pair.time;
    let mut last_duration = Duration::ZERO;

    const MAX_DURATION: Duration = Duration::from_secs(2); // equivalent to MAX_PTO_INTERVAL
    const MAX_ITERS: u64 = MAX_DURATION.as_millis().ilog2() as u64 + 2;

    for i in 1..=MAX_ITERS {
        drive_server_until_challenge_received(&mut pair, i);
        pair.client.inbound.clear(); // we drop the client-inbound PATH_RESPONSE
        info!("dropped client inbound");

        drive_client_until_challenge_sent(&mut pair, i + 1);
        let time = pair.time.duration_since(last_challenge_send);
        info!(?time, ?last_duration, "time since last PATH_CHALLENGE send");
        assert!(
            time >= last_duration,
            "duration between PATH_CHALLENGE sends must be monotonically increasing (backing off)"
        );
        assert!(
            time <= MAX_DURATION,
            "duration between PATH_CHALLENGE sends must be bound to a maximum of 2s"
        );
        if i == MAX_ITERS {
            assert_eq!(time, MAX_DURATION);
        }
        last_duration = time;
        last_challenge_send = pair.time;
    }

    // Now we stop dropping the client inbound once and the backoff should return back to normal:

    drive_server_until_challenge_received(&mut pair, MAX_ITERS + 1);
    pair.drive(); // Ensure the client fully processes the PATH_RESPONSE this time.
    info!("client should have processed PATH_RESPONSE");

    // The next time challenges are sent, the backoff should be reset.

    pair.conn_mut(Client).trigger_path_validation();
    drive_client_until_challenge_sent(&mut pair, MAX_ITERS + 2);
    last_challenge_send = pair.time;
    drive_server_until_challenge_received(&mut pair, MAX_ITERS + 2);
    pair.client.inbound.clear(); // we drop the client-inbound PATH_RESPONSE
    info!("dropped client inbound");
    drive_client_until_challenge_sent(&mut pair, MAX_ITERS + 3);
    let duration = pair.time.duration_since(last_challenge_send);
    assert_eq!(duration, Duration::from_millis(1));
}
