use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use proto::{
    ClosePathError, ClosedPath, PathError, PathEvent, PathId, PathStats, PathStatus,
    SetPathStatusError, TransportErrorCode,
};
use tokio::sync::watch;
use tokio_stream::{Stream, wrappers::WatchStream};

use crate::connection::{ConnectionRef, ToConnectionHandle};
use crate::{Connection, Runtime, WeakConnectionHandle};

/// Future produced by [`crate::Connection::open_path`]
pub struct OpenPath(OpenPathInner);

enum OpenPathInner {
    /// Opening a path in underway
    ///
    /// This might fail later on.
    Ongoing {
        opened: WatchStream<Result<(), PathError>>,
        path_ref: StrongPathRef,
    },
    /// Opening a path failed immediately
    Rejected {
        /// The error that occurred
        err: PathError,
    },
    /// The path is already open
    Ready { path_ref: StrongPathRef },
}

impl OpenPath {
    pub(crate) fn new(
        path_id: PathId,
        opened: watch::Receiver<Result<(), PathError>>,
        conn: ConnectionRef,
    ) -> Self {
        let path_ref = conn
            .lock_without_waking("OpenPath::new")
            .acquire_path_ref(path_id, conn.clone());
        Self(OpenPathInner::Ongoing {
            opened: WatchStream::from_changes(opened),
            path_ref,
        })
    }

    pub(crate) fn ready(path_id: PathId, conn: ConnectionRef) -> Self {
        let path_ref = conn
            .lock_without_waking("OpenPath::ready")
            .acquire_path_ref(path_id, conn.clone());
        Self(OpenPathInner::Ready { path_ref })
    }

    pub(crate) fn rejected(err: PathError) -> Self {
        Self(OpenPathInner::Rejected { err })
    }

    /// Returns the path ID of the new path being opened.
    ///
    /// If an error occurred before a path ID was allocated, `None` is returned.  In this
    /// case the future is ready and polling it will immediately yield the error.
    ///
    /// The returned value remains the same for the entire lifetime of this future.
    pub fn path_id(&self) -> Option<PathId> {
        match self.0 {
            OpenPathInner::Ongoing { ref path_ref, .. } => Some(path_ref.id),
            OpenPathInner::Rejected { .. } => None,
            OpenPathInner::Ready { ref path_ref, .. } => Some(path_ref.id),
        }
    }
}

impl Future for OpenPath {
    type Output = Result<Path, PathError>;
    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().0 {
            OpenPathInner::Ongoing {
                ref mut opened,
                ref path_ref,
            } => match ready!(Pin::new(opened).poll_next(ctx)) {
                Some(value) => Poll::Ready(value.map(|_| Path::new_from_ref(path_ref.clone()))),
                None => {
                    // This only happens if receiving a notification change failed, this means the
                    // sender was dropped. This generally should not happen so we use a transient
                    // error
                    Poll::Ready(Err(PathError::ValidationFailed))
                }
            },
            OpenPathInner::Ready { ref path_ref } => {
                Poll::Ready(Ok(Path::new_from_ref(path_ref.clone())))
            }
            OpenPathInner::Rejected { err } => Poll::Ready(Err(err)),
        }
    }
}

/// An open network transmission within a multipath-enabled connection.
///
/// As long as a [`Path`] or [`WeakPathHandle`] is alive, it is ensured that the
/// [`PathStats`] for this path are not dropped even after the path is abandoned.
///
/// [`WeakPathHandle`]: crate::path::WeakPathHandle
#[derive(Debug, Clone)]
pub struct Path {
    path_ref: StrongPathRef,
}

impl Path {
    /// Returns a [`Path`] for a path id, after checking that the path is not closed.
    pub(crate) fn new(conn: &ConnectionRef, id: PathId) -> Option<Self> {
        let path_ref = {
            let mut state = conn.lock_without_waking("Path::new");
            // TODO(flub): Using this to know if the path still exists is... hacky.
            state.inner.path_status(id).ok()?;
            state.acquire_path_ref(id, conn.clone())
        };
        Some(Self::new_from_ref(path_ref))
    }

    /// Returns a [`Path`] for a [`PathRef`] without checking if the path exists or is closed.
    fn new_from_ref(path_ref: StrongPathRef) -> Self {
        Self { path_ref }
    }

    /// Returns a [`WeakPathHandle`] for this path.
    ///
    /// Holding a [`WeakPathHandle`] does not keep a connection alive, but ensures that the
    /// path's stats are not dropped until the underlying connection is dropped, even if the
    /// path is abandoned.
    pub fn weak_handle(&self) -> WeakPathHandle {
        WeakPathHandle {
            path_ref: self.path_ref.weak_handle(),
        }
    }

    /// The [`PathId`] of this path.
    pub fn id(&self) -> PathId {
        self.path_ref.id
    }

    /// The current local [`PathStatus`] of this path.
    pub fn status(&self) -> Result<PathStatus, ClosedPath> {
        self.path_ref
            .handle
            .lock_without_waking("path status")
            .inner
            .path_status(self.id())
    }

    /// Sets the [`PathStatus`] of this path.
    ///
    /// Returns the previous status of the path.
    pub fn set_status(&self, status: PathStatus) -> Result<PathStatus, SetPathStatusError> {
        self.path_ref
            .handle
            .lock_and_wake("set path status")
            .inner
            .set_path_status(self.id(), status)
    }

    /// Returns the [`PathStats`] for this path.
    pub fn stats(&self) -> PathStats {
        self.path_ref
            .handle
            .lock_without_waking("Path::stats")
            .path_stats_for_ref(&self.path_ref)
    }

    /// Closes this path.
    ///
    /// The path is immediately considered closed by the local endpoint. Once the state is removed,
    /// after a short period of time for any in-flight packets, a [`PathEvent::Abandoned`] is
    /// returned.
    pub fn close(&self) -> Result<(), ClosePathError> {
        let mut state = self.path_ref.handle.lock_and_wake("close_path");
        state.inner.close_path(
            crate::Instant::now(),
            self.id(),
            TransportErrorCode::APPLICATION_ABANDON_PATH.into(),
        )
    }

    /// Sets the max idle timeout for a specific path
    ///
    /// See [`TransportConfig::default_path_max_idle_timeout`] for details.
    ///
    /// Returns the previous value of the setting.
    ///
    /// [`TransportConfig::default_path_max_idle_timeout`]: crate::TransportConfig::default_path_max_idle_timeout
    pub fn set_max_idle_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        let mut state = self
            .path_ref
            .handle
            .lock_and_wake("path_set_max_idle_timeout");
        let now = state.runtime.now();
        state
            .inner
            .set_path_max_idle_timeout(now, self.id(), timeout)
    }

    /// Sets the keep_alive_interval for a specific path
    ///
    /// See [`TransportConfig::default_path_keep_alive_interval`] for details.
    ///
    /// Returns the previous value of the setting.
    ///
    /// [`TransportConfig::default_path_keep_alive_interval`]: crate::TransportConfig::default_path_keep_alive_interval
    pub fn set_keep_alive_interval(
        &self,
        interval: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        let mut state = self
            .path_ref
            .handle
            .lock_and_wake("path_set_keep_alive_interval");
        state
            .inner
            .set_path_keep_alive_interval(self.id(), interval)
    }

    /// Track changes on our external address as reported by the peer.
    ///
    /// If the address-discovery extension is not negotiated, the stream will never return.
    pub fn observed_external_addr(&self) -> Result<AddressDiscovery, ClosedPath> {
        let state = self
            .path_ref
            .handle
            .lock_without_waking("per_path_observed_address");
        let path_events = state.path_events.subscribe();
        let initial_value = state.inner.path_observed_address(self.id())?;
        Ok(AddressDiscovery::new(
            self.id(),
            path_events,
            initial_value,
            state.runtime.clone(),
        ))
    }

    /// The peer's UDP address for this path.
    pub fn remote_address(&self) -> Result<SocketAddr, ClosedPath> {
        let state = self
            .path_ref
            .handle
            .lock_without_waking("per_path_remote_address");
        Ok(state.inner.network_path(self.id())?.remote())
    }

    /// The local IP used for this path, if known.
    ///
    /// Returns `Ok(None)` for clients or when the platform does not expose this information; see
    /// [`noq_udp::RecvMeta::dst_ip`](udp::RecvMeta::dst_ip) for supported platforms.
    pub fn local_ip(&self) -> Result<Option<IpAddr>, ClosedPath> {
        let state = self
            .path_ref
            .handle
            .lock_without_waking("per_path_local_ip");
        Ok(state.inner.network_path(self.id())?.local_ip())
    }

    /// Ping the remote endpoint over this path.
    pub fn ping(&self) -> Result<(), ClosedPath> {
        let mut state = self.path_ref.handle.lock_and_wake("ping");
        state.inner.ping_path(self.id())
    }
}

impl PartialEq for Path {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
            && self.path_ref.handle.stable_id() == other.path_ref.handle.stable_id()
    }
}

/// Weak handle for a [`Path`] that does not keep the connection alive.
///
/// As long as a [`WeakPathHandle`] for a path exists, that path's final stats will not be dropped even if
/// the path was abandoned.
///
/// The [`WeakPathHandle`] can be upgraded to a [`Path`] as long as its [`Connection`] has not been dropped.
///
/// [`Connection`]: crate::Connection
#[derive(Debug, Clone)]
pub struct WeakPathHandle {
    path_ref: WeakPathRef,
}

impl PartialEq for WeakPathHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
            && self
                .path_ref
                .handle
                .is_same_connection(&other.path_ref.handle)
    }
}

impl Eq for WeakPathHandle {}

impl WeakPathHandle {
    /// Returns the [`PathId`] of this path.
    pub fn id(&self) -> PathId {
        self.path_ref.id
    }

    /// Upgrades to a [`Path`].
    ///
    /// Returns `None` if the connection was dropped.
    pub fn upgrade(&self) -> Option<Path> {
        Some(Path {
            path_ref: self.path_ref.upgrade()?,
        })
    }

    /// Returns the path statistics for this path.
    ///
    /// # Panics
    ///
    /// Panics if `conn` is not the [`Connection`] this path belongs to. If you
    /// cannot ensure this invariant, use [`Self::upgrade`] and then [`Path::stats`]
    /// instead.
    pub fn stats(&self, conn: &Connection) -> PathStats {
        assert!(
            conn.weak_handle().is_same_connection(&self.path_ref.handle),
            "WeakPathHandle::stats called with wrong Connection"
        );
        conn.path_stats_for_ref(&self.path_ref)
    }
}

/// Owner side of a path's reference counter, stored in [`State::path_refs`].
///
/// Holds the shared [`AtomicUsize`] but does not itself contribute to the count.
/// Hands out [`PathRef`] handles via [`Self::acquire`].
///
/// [`State::path_refs`]: crate::connection::State::path_refs
#[derive(Debug, Default)]
pub(crate) struct PathRefOwner {
    ref_count: Arc<AtomicUsize>,
}

impl PathRefOwner {
    /// Acquire a new [`PathRef`] handle, bumping the reference counter by 1.
    pub(crate) fn acquire<H: ToConnectionHandle>(&self, path_id: PathId, handle: H) -> PathRef<H> {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        PathRef {
            id: path_id,
            ref_count: self.ref_count.clone(),
            handle,
        }
    }
}

/// Handle to a path's reference counter and either a strong or weak connection handle.
///
/// Held by [`Path`] and [`WeakPathHandle`].
///
/// Cloning bumps the counter automatically, dropping decrements the reference counter,
/// and once the count reaches zero, uses the inner handle to clear the corresponding
/// entries from the connection state.
#[derive(Debug)]
pub(crate) struct PathRef<H: ToConnectionHandle> {
    pub(crate) id: PathId,
    handle: H,
    ref_count: Arc<AtomicUsize>,
}

impl<H: ToConnectionHandle> Clone for PathRef<H> {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        Self {
            id: self.id,
            handle: self.handle.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

impl<H: ToConnectionHandle> Drop for PathRef<H> {
    fn drop(&mut self) {
        if self.ref_count.fetch_sub(1, Ordering::Relaxed) > 1 {
            return;
        }
        if let Some(conn) = self.handle.get_or_upgrade() {
            let mut state = conn.lock_without_waking("PathRef::drop");
            // Re-check under the lock: a concurrent `Path::new` may have bumped
            // the counter back up between our `fetch_sub` and the lock.
            if self.ref_count.load(Ordering::Relaxed) > 0 {
                return;
            }
            state.path_refs.remove(&self.id);
            state.final_path_stats.remove(&self.id);
        }
    }
}

pub(crate) type StrongPathRef = PathRef<ConnectionRef>;

impl StrongPathRef {
    pub(crate) fn weak_handle(&self) -> WeakPathRef {
        PathRef {
            id: self.id,
            ref_count: self.ref_count.clone(),
            handle: self.handle.weak_handle(),
        }
    }
}

pub(crate) type WeakPathRef = PathRef<WeakConnectionHandle>;

impl WeakPathRef {
    pub(crate) fn upgrade(&self) -> Option<StrongPathRef> {
        let handle = self.handle.upgrade_to_ref()?;
        Some(PathRef {
            id: self.id,
            ref_count: self.ref_count.clone(),
            handle,
        })
    }
}

/// Stream produced by [`Path::observed_external_addr`]
///
/// This will always return the external address most recently reported by the remote over this
/// path. If the extension is not negotiated, this stream will never return.
// TODO(@divma): provide a way to check if the extension is negotiated.
pub struct AddressDiscovery {
    watcher: WatchStream<SocketAddr>,
}

impl AddressDiscovery {
    pub(super) fn new(
        path_id: PathId,
        mut path_events: tokio::sync::broadcast::Receiver<PathEvent>,
        initial_value: Option<SocketAddr>,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        let (tx, rx) = watch::channel(initial_value.unwrap_or_else(||
                // if the dummy value is used, it will be ignored
                SocketAddr::new([0, 0, 0, 0].into(), 0)));
        let filter = async move {
            loop {
                match path_events.recv().await {
                    Ok(PathEvent::ObservedAddr { id, addr: observed }) if id == path_id => {
                        tx.send_if_modified(|addr| {
                            let old = std::mem::replace(addr, observed);
                            old != *addr
                        });
                    }
                    Ok(PathEvent::Discarded { id, .. }) if id == path_id => {
                        // If the path is closed, terminate the stream
                        break;
                    }
                    Ok(_) => {
                        // ignore any other event
                    }
                    Err(_) => {
                        // A lagged error should never happen since this (detached) task is
                        // constantly reading from the channel. Therefore, if an error does happen,
                        // the stream can terminate
                        break;
                    }
                }
            }
        };

        let watcher = if initial_value.is_some() {
            WatchStream::new(rx)
        } else {
            WatchStream::from_changes(rx)
        };

        runtime.spawn(Box::pin(filter));
        // TODO(@divma): check if there's a way to ensure the future ends. AbortHandle is not an
        // option
        Self { watcher }
    }
}

impl Stream for AddressDiscovery {
    type Item = SocketAddr;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.watcher).poll_next(cx)
    }
}
