//! Who this node talks to, and the clients it talks to them with.

use std::collections::HashMap;

use tonic::transport::{Channel, Endpoint};

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

/// One gRPC client per peer (see the module doc), shared across every
/// role and every election via `Common::peer_conns`.
///
/// Deliberately a *synchronous* mutex around a map of already-built
/// clients, with no `.await` anywhere under the lock. An earlier version
/// dialed the peer while holding an async mutex, which made this a
/// cluster-wide chokepoint: a peer that black-holes packets (rather than
/// refusing them outright) leaves `connect` hanging until the caller's
/// own timeout -- up to `APPEND_ENTRIES_RPC_TIMEOUT` -- and every *other*
/// peer's replication and lease probing queued behind it on this one
/// lock. A leader could lose its read quorum because a single unrelated
/// follower was unreachable. Building the client can't block now, so
/// there is no window to queue behind.
#[derive(Default)]
pub(super) struct PeerConnections {
    clients: std::sync::Mutex<HashMap<String, RaftPeerClient<Channel>>>,
}

impl PeerConnections {
    /// A client for `peer`, building (and remembering) one if this is the
    /// first call. Never blocks: `connect_lazy` performs no I/O, it just
    /// describes how to reach the peer, and the returned `Channel` dials
    /// on first use and re-dials on its own after a failure. A
    /// `RaftPeerClient` is a thin wrapper around that `Channel`, cheap to
    /// clone and safe to use concurrently from many tasks at once.
    ///
    /// `None` only for an address that isn't a valid URI at all -- a
    /// permanent configuration error rather than an unreachable peer, and
    /// one there is nothing useful to do about here.
    pub(super) fn client(&self, peer: &PeerInfo) -> Option<RaftPeerClient<Channel>> {
        let mut clients = self.clients.lock().unwrap();
        if let Some(client) = clients.get(&peer.id) {
            return Some(client.clone());
        }
        let channel = Endpoint::from_shared(peer.address.clone())
            .ok()?
            .connect_lazy();
        let client = RaftPeerClient::new(channel);
        clients.insert(peer.id.clone(), client.clone());
        Some(client)
    }

    /// Forgets a peer's client, closing its connection once the last
    /// in-flight call using it finishes. Only for a peer that has left
    /// the configuration -- an unreachable peer needs nothing of the
    /// sort, since the channel reconnects by itself.
    pub(super) fn forget(&self, peer_id: &str) {
        self.clients.lock().unwrap().remove(peer_id);
    }
}
