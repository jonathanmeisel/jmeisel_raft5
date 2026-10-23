//! What goes into a role's event loop, what comes out of it, and the
//! errors either can carry.
//!
//! `Event` is the one inbound channel every role reads: peer RPCs from
//! `grpc::RaftPeerService`, client requests from `grpc::KvService`, and this
//! node's own timers. `Transition` is what handling one event decided, and
//! `Role` is the role the driver builds next.

use tokio::sync::oneshot;

use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseResponse, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::storage::StorageError;

/// Inbound events a role's event loop reacts to. Requests carry their own
/// response channel, so the loop can reply once it decides. Every role must
/// be able to answer AppendEntries and RequestVote, not only the leader.
pub enum Event {
    AppendEntries {
        req: AppendEntriesRequest,
        respond_to: oneshot::Sender<AppendEntriesResponse>,
    },
    AppendEntriesResponse {
        from: String,
        resp: AppendEntriesResponse,
    },
    RequestVote {
        req: RequestVoteRequest,
        respond_to: oneshot::Sender<RequestVoteResponse>,
    },
    RequestVoteResponse {
        from: String,
        resp: RequestVoteResponse,
    },
    /// Only the leader's own probe results coming back. An inbound `GetLease`
    /// never goes through this event loop (see `RaftPeerHandle::get_lease`), so
    /// it can never wait behind a slow `AppendEntries`.
    ///
    /// `expires_at` is when a grant stops counting: the duration the probe
    /// asked for, timed from when it was sent. The follower starts the same
    /// duration only when it receives the request, so timing it from the send
    /// keeps the leader's view inside the follower's promise, by however long
    /// the request took to arrive.
    GetLeaseResponse {
        from: String,
        resp: GetLeaseResponse,
        expires_at: std::time::Instant,
    },
    /// `body` yields the byte stream that `RaftLog::export_snapshot` produced
    /// (magic, version, seq and term header included), as it arrives.
    /// `grpc::RaftPeerService` pumps the gRPC chunks into it but does not touch
    /// storage, so the term check and the install both happen here, like every
    /// other inbound RPC.
    ///
    /// A reader, not a `Vec<u8>`: a snapshot is the cluster's whole applied
    /// state, and holding all of it in memory is never the right idea at any
    /// size. Dropping this event unread (which a term rejection does) closes
    /// the channel and tells the sender to stop.
    InstallSnapshot {
        header: SnapshotHeader,
        body: crate::store::ChunkReader,
        respond_to: oneshot::Sender<InstallSnapshotResponse>,
    },
    /// The leader's snapshot-send task reporting what happened, like
    /// `AppendEntriesResponse`. `last_included_seq` travels with the raw proto
    /// response (which has only `term`), so a successful install can advance
    /// `Leader::peer_match_index` as a successful `AppendEntries` does.
    InstallSnapshotResponse {
        from: String,
        resp: InstallSnapshotResponse,
        last_included_seq: u64,
    },
    ElectionTimeout,
    /// Self-notification, leader only: makes the event loop recompute
    /// `commit_seq` and `compact_seq` even when no peer response does. Needed
    /// for a cluster with no peers (nothing else would advance `commit_tx`
    /// after a client write), and it also keeps watermarks from stalling
    /// between replication rounds with peers.
    LeaderTick,
    /// A membership change to propose. Handled on the leader's event loop,
    /// the only place with the state to enforce the two guards a configuration
    /// change needs (see `leader::Leader::propose_config`).
    ProposeConfig {
        change: ConfigChange,
        respond_to: oneshot::Sender<Result<u64, RaftError>>,
    },
    /// An operator asks this node to form a single-node cluster around itself
    /// (Raft §4.4). Accepted only by a node with no configuration and an empty
    /// log: see `Follower::bootstrap`.
    Bootstrap {
        config: ClusterConfig,
        respond_to: oneshot::Sender<Result<(), RaftError>>,
    },
}

impl Event {
    /// The term in an RPC response, if this is one.
    ///
    /// Raft §5.1 ("if an RPC request or response contains term T >
    /// currentTerm, set currentTerm = T and convert to follower") is the one
    /// rule that applies the same way to every role. This applies it in one
    /// place (`Common::next_event`) instead of in each role.
    ///
    /// Responses only, on purpose. A request with a higher term needs a
    /// different answer per role before stepping down (a leader rejects the
    /// append, a follower accepts it, a candidate evaluates the vote), so those
    /// stay in the role event loops where the reply is built. A response has no
    /// reply to build: the only thing to do with a higher term is adopt it.
    ///
    /// The match is exhaustive, with no wildcard, so a new variant cannot
    /// silently skip the rule.
    pub(super) fn response_term(&self) -> Option<u64> {
        match self {
            Self::AppendEntriesResponse { resp, .. } => Some(resp.term),
            Self::RequestVoteResponse { resp, .. } => Some(resp.term),
            Self::InstallSnapshotResponse { resp, .. } => Some(resp.term),
            // `GetLeaseResponse` carries no term: a lease is granted against the term
            // the prober named, so there is never a newer term to learn from it.
            Self::GetLeaseResponse { .. }
            | Self::AppendEntries { .. }
            | Self::RequestVote { .. }
            | Self::InstallSnapshot { .. }
            | Self::ElectionTimeout
            | Self::LeaderTick
            | Self::ProposeConfig { .. }
            | Self::Bootstrap { .. } => None,
        }
    }
}

/// Which role to build next. A plain tag: unlike the role structs, it
/// carries no state, because only one role's state is alive at a time.
pub(crate) enum Role {
    Follower,
    /// A non-binding pre-vote round: see the module doc.
    PreCandidate,
    Candidate,
    Leader,
    /// Not a role: the node is shutting down (its cancellation token fired, or
    /// the event channel closed), so `run` stops instead of building another
    /// role. Every role's event loop can return this; only the driver reads it.
    Stopped,
}

/// What a role's event loop decided after handling one event.
pub(super) enum Transition {
    Continue,
    Become(Role),
}

#[derive(Debug)]
pub enum RaftError {
    /// Not, or no longer, the leader. `leader_hint` lets a client retry in the
    /// right place, like `RaftError`/`NotLeader` in client.proto.
    NotLeader {
        leader_hint: Option<String>,
    },
    Storage(StorageError),
    /// The event loop is gone (node shutting down) before it could reply.
    Disconnected,
    /// `Bootstrap` on a node already in a cluster. Refused, because a second
    /// bootstrap is how one cluster silently becomes two: see
    /// `follower::Follower::bootstrap`.
    AlreadyInitialized,
    /// A membership change is appended and not committed yet. Raft allows one
    /// at a time (§4.1): overlapping single-server changes can produce two
    /// configurations with no common majority, the exact disjoint-quorum split
    /// the rule prevents.
    ConfigChangeInProgress,
    /// This leader has not yet committed an entry from its own term, so it
    /// cannot safely propose a membership change (Ongaro 2015): until then, it
    /// cannot be sure the configuration it builds on will not be rolled back.
    NotCaughtUp,
    /// The change does not apply to the current configuration: adding a server
    /// that is already a member, removing one that is not, or removing the last
    /// voter.
    InvalidConfigChange(&'static str),
}

/// A single-server membership change (Raft §4.1: one server at a time, not
/// joint consensus). The caller says what it wants, and the leader applies
/// it to the configuration in force, so a client never sends (or races on)
/// a whole configuration.
#[derive(Clone, Debug)]
pub enum ConfigChange {
    /// Add `server_id` as a non-voting learner. It becomes a voter
    /// automatically once it catches up: see
    /// `leader::Leader::review_learner_progress`. Adding it as a voter at once
    /// would let a server that is hours behind count toward (and stall) every
    /// quorum from the moment it joined.
    AddServer {
        server_id: String,
        address: String,
        client_address: String,
    },
    RemoveServer {
        server_id: String,
    },
    /// Leader-internal: a learner has caught up and is now a voter.
    Promote {
        server_id: String,
    },
    /// Change the cluster's heartbeat, and every timeout derived from it (see
    /// `Timing`). Membership does not change. Goes through the log like any
    /// configuration change, so it takes effect on each node as that node
    /// appends it.
    SetHeartbeat {
        millis: u64,
    },
}

impl ConfigChange {
    /// `config` with this change applied, or why it cannot be.
    pub(super) fn apply_to(&self, config: &ClusterConfig) -> Result<ClusterConfig, RaftError> {
        let invalid = |why| Err(RaftError::InvalidConfigChange(why));
        match self {
            Self::AddServer {
                server_id,
                address,
                client_address,
            } => {
                if config.contains(server_id) {
                    return invalid("that server is already a member of this cluster");
                }
                Ok(config.with_member(ClusterMember {
                    server_id: server_id.clone(),
                    address: address.clone(),
                    client_address: client_address.clone(),
                    voting: false,
                }))
            }
            Self::RemoveServer { server_id } => {
                if !config.contains(server_id) {
                    return invalid("that server is not a member of this cluster");
                }
                let next = config.without_member(server_id);
                if next.voters().count() == 0 {
                    // The removal could never commit: nobody would be left to form a quorum of
                    // the new configuration.
                    return invalid("removing that server would leave the cluster with no voters");
                }
                Ok(next)
            }
            Self::Promote { server_id } => {
                if !config.contains(server_id) {
                    return invalid("that server is not a member of this cluster");
                }
                Ok(config.with_voter(server_id))
            }
            Self::SetHeartbeat { millis } => {
                super::check_heartbeat_ms(*millis).map_err(RaftError::InvalidConfigChange)?;
                Ok(ClusterConfig {
                    heartbeat_millis: *millis,
                    ..config.clone()
                })
            }
        }
    }
}
