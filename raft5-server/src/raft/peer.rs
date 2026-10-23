//! Who this node talks to, and the clients it uses to talk to them.

use std::collections::HashMap;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::pb::raft5::cluster::ClusterMember;
use crate::pb::raft5::raft::raft_peer_client::RaftPeerClient;

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: String,
    pub address: String,
}

impl From<&ClusterMember> for PeerInfo {
    fn from(m: &ClusterMember) -> Self {
        Self {
            id: m.server_id.clone(),
            address: m.address.clone(),
        }
    }
}

/// gRPC clients per peer (see the module doc), shared by every role and
/// election through `Common::peer_conns`.
///
/// **Two connections per peer.** Bulk work (`AppendEntries`,
/// `InstallSnapshot`) and lease probes use separate `Channel`s to the same
/// address, so separate TCP connections.
///
/// `GetLease` is a separate RPC so that a slow `AppendEntries` cannot delay
/// it (see `raft.proto`). On one shared connection that would hold only on
/// the event loop, not in the transport: a replication batch large enough
/// to fill the socket's send buffer delays everything queued behind it,
/// and a probe must be answered within `Timing::lightweight_rpc_timeout`
/// or not at all. Miss enough probes and the follower's lease expires and
/// it campaigns: a healthy leader removed by its own replication traffic.
/// Separate connections make the guarantee hold end to end.
///
/// A synchronous mutex around maps of already-built clients, with no
/// `.await` under the lock, on purpose. Dialing under the lock would make
/// it a cluster-wide choke point: a peer that drops packets silently (not
/// refusing them) leaves `connect` hanging until the caller's timeout (up
/// to `APPEND_ENTRIES_RPC_TIMEOUT`), and every other peer's replication and
/// lease probes would queue behind it. A leader could lose its read quorum
/// because one unrelated follower was unreachable. Building a client never
/// blocks, so there is nothing to queue behind.
///
/// `Default` dials in plaintext; [`PeerConnections::new`] with a TLS config
/// dials every peer over mTLS.
#[derive(Default)]
pub(super) struct PeerConnections {
    /// How to dial peers over TLS, or `None` for plaintext. Also decides the
    /// scheme, whatever the stored address says: see `raft5_shared::tls`.
    tls: Option<ClientTlsConfig>,
    /// For `AppendEntries`, `InstallSnapshot` and `RequestVote`.
    clients: std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
    /// For `GetLease` only. See the type's doc.
    lease_clients: std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
}

impl PeerConnections {
    pub(super) fn new(tls: Option<ClientTlsConfig>) -> Self {
        Self {
            tls,
            ..Self::default()
        }
    }

    /// A client for `peer`'s bulk traffic, built (and remembered) on the first
    /// call. Never blocks: `connect_lazy` does no I/O, it only describes how to
    /// reach the peer, and the returned `Channel` dials on first use and
    /// re-dials by itself after a failure. A `RaftPeerClient` is a thin wrapper
    /// around that `Channel`, cheap to clone and safe to use from many tasks
    /// at once.
    ///
    /// `None` only for an address that is not a valid URI, or a TLS config
    /// rustls will not build: permanent configuration errors, not an
    /// unreachable peer, and nothing useful can be done about them here.
    pub(super) fn client(&self, peer: &PeerInfo) -> Option<RaftPeerClient<Channel>> {
        self.client_from(&self.clients, peer)
    }

    /// A client for `peer`'s lease probes, on its own connection, so a probe
    /// never waits behind a replication batch.
    pub(super) fn lease_client(&self, peer: &PeerInfo) -> Option<RaftPeerClient<Channel>> {
        self.client_from(&self.lease_clients, peer)
    }

    fn client_from(
        &self,
        cache: &std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
        peer: &PeerInfo,
    ) -> Option<RaftPeerClient<Channel>> {
        let mut clients = cache.lock().unwrap();
        if let Some(client) = clients.get(&peer.id) {
            return Some(client.clone());
        }
        let channel = peer_endpoint(&peer.address, self.tls.as_ref())
            .ok()?
            // Without this, throughput is limited to h2's default 65535-byte window
            // divided by the round trip: a few MB/s on anything but loopback, whatever
            // the chunk size. It also keeps a large batch on the wire for less time,
            // which is what the separate lease connection protects.
            .http2_adaptive_window(true)
            .connect_lazy();
        let client = RaftPeerClient::new(channel);
        clients.insert(peer.id.clone(), client.clone());
        Some(client)
    }

    /// Forget a peer's clients, closing their connections once the last call
    /// using them finishes. Only for a peer that left the configuration: an
    /// unreachable peer needs nothing like this, because the channels
    /// reconnect by themselves.
    pub(super) fn forget(&self, peer_id: &str) {
        self.clients.lock().unwrap().remove(peer_id);
        self.lease_clients.lock().unwrap().remove(peer_id);
    }
}

/// How to dial a peer API at `address`: over TLS with `tls`, whatever
/// scheme `address` names (see `raft5_shared::tls`), and in plaintext
/// without it. No I/O: the rustls config is built here, but nothing is
/// dialed until the endpoint connects.
///
/// Shared by `PeerConnections` and `AddServer`'s `Identify`, which dials a
/// server that is not a peer yet but must be reached exactly as one.
pub(crate) fn peer_endpoint(
    address: &str,
    tls: Option<&ClientTlsConfig>,
) -> Result<Endpoint, tonic::transport::Error> {
    let endpoint =
        Endpoint::from_shared(raft5_shared::tls::with_scheme(address, tls.is_some()))?;
    match tls {
        Some(tls) => endpoint.tls_config(tls.clone()),
        None => Ok(endpoint),
    }
}
