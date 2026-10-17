//! Who this node talks to, and the clients it talks to them with.

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

/// gRPC clients per peer (see the module doc), shared across every role
/// and every election via `Common::peer_conns`.
///
/// **Two connections per peer, not one.** Bulk work (`AppendEntries`,
/// `InstallSnapshot`) and lease probes get separate `Channel`s to the
/// same address, and therefore separate TCP connections.
///
/// `GetLease` is a separate RPC precisely so that a slow `AppendEntries`
/// can't delay it (see `raft.proto`) -- but sharing a connection made
/// that true only of the *event loop*, where a probe is answered off a
/// watch channel, and false of the transport underneath. A replication
/// batch large enough to fill the socket's send buffer delays everything
/// queued behind it on that connection, and a probe is answered inside
/// `Timing::lightweight_rpc_timeout` or not at all. Miss enough of them
/// and the follower's lease expires and it campaigns -- a healthy leader
/// deposed by its own replication traffic. Separate connections are what
/// make the claim hold end to end.
///
/// Deliberately a *synchronous* mutex around maps of already-built
/// clients, with no `.await` anywhere under the lock. An earlier version
/// dialed the peer while holding an async mutex, which made this a
/// cluster-wide chokepoint: a peer that black-holes packets (rather than
/// refusing them outright) leaves `connect` hanging until the caller's
/// own timeout -- up to `APPEND_ENTRIES_RPC_TIMEOUT` -- and every *other*
/// peer's replication and lease probing queued behind it on this one
/// lock. A leader could lose its read quorum because a single unrelated
/// follower was unreachable. Building the client can't block now, so
/// there is no window to queue behind.
///
/// `Default` dials in plaintext; [`PeerConnections::new`] with a TLS
/// config dials every peer over mTLS instead.
#[derive(Default)]
pub(super) struct PeerConnections {
    /// How to dial peers over TLS, or `None` for plaintext. Decides the
    /// scheme too, whatever the stored address says: see
    /// `raft5_shared::tls`.
    tls: Option<ClientTlsConfig>,
    /// For `AppendEntries`, `InstallSnapshot` and `RequestVote`.
    clients: std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
    /// For `GetLease` alone. See the type's doc.
    lease_clients: std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
}

impl PeerConnections {
    pub(super) fn new(tls: Option<ClientTlsConfig>) -> Self {
        Self {
            tls,
            ..Self::default()
        }
    }

    /// A client for `peer`'s bulk traffic, building (and remembering) one
    /// if this is the first call. Never blocks: `connect_lazy` performs
    /// no I/O, it just describes how to reach the peer, and the returned
    /// `Channel` dials on first use and re-dials on its own after a
    /// failure. A `RaftPeerClient` is a thin wrapper around that
    /// `Channel`, cheap to clone and safe to use concurrently from many
    /// tasks at once.
    ///
    /// `None` only for an address that isn't a valid URI at all, or a
    /// TLS config rustls won't build -- permanent configuration errors
    /// rather than an unreachable peer, and ones there is nothing useful
    /// to do about here.
    pub(super) fn client(&self, peer: &PeerInfo) -> Option<RaftPeerClient<Channel>> {
        self.client_from(&self.clients, peer)
    }

    /// A client for `peer`'s lease probes, on its own connection so that
    /// a probe is never queued behind a replication batch.
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
            // Without this, throughput is bounded by h2's default
            // 65535-byte window divided by the round trip -- a few MB/s
            // on anything but a loopback, whatever the chunk size. It
            // also keeps a large batch off the wire for less time, which
            // is what the separate lease connection above is protecting.
            .http2_adaptive_window(true)
            .connect_lazy();
        let client = RaftPeerClient::new(channel);
        clients.insert(peer.id.clone(), client.clone());
        Some(client)
    }

    /// Forgets a peer's clients, closing their connections once the last
    /// in-flight call using them finishes. Only for a peer that has left
    /// the configuration -- an unreachable peer needs nothing of the
    /// sort, since the channels reconnect by themselves.
    pub(super) fn forget(&self, peer_id: &str) {
        self.clients.lock().unwrap().remove(peer_id);
        self.lease_clients.lock().unwrap().remove(peer_id);
    }
}

/// How to dial a peer API at `address`: over TLS with `tls`, whatever
/// scheme `address` names (see `raft5_shared::tls`), and in plaintext
/// without it. No I/O -- the rustls config is built on the spot, but
/// nothing is dialed until the endpoint is connected.
///
/// Shared by `PeerConnections` and `AddServer`'s `Identify`, which dials
/// a server that is not a peer *yet* but must be reached exactly as one
/// will be.
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
