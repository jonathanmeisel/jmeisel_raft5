//! What flows into a role's event loop, what comes back out of it, and
//! the errors either can carry.
//!
//! `Event` is the single inbound channel every role reads from -- peer
//! RPCs relayed by `grpc::RaftPeerService`, client requests relayed by
//! `grpc::KvService`, and this node's own timers alike. `Transition` is
//! what handling one event decided, and `Role` is which role the driver
//! builds next.

use tokio::sync::oneshot;

use crate::pb::raft5::cluster::{ClusterConfig, ClusterMember};
use crate::pb::raft5::raft::{
    AppendEntriesRequest, AppendEntriesResponse, GetLeaseResponse, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse, SnapshotHeader,
};
use crate::storage::StorageError;

/// Inbound events a role's event loop reacts to. Requests carry their own
/// response channel so the loop can reply once it's decided how to handle
/// them -- every role has to be able to answer AppendEntries/RequestVote,
/// not just the leader.
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
    /// Only ever the *leader's* own probe results coming back in --
    /// answering an inbound `GetLease` never goes through this event loop
    /// at all (see `RaftPeerHandle::get_lease`), specifically so it can
    /// never get stuck behind a slow in-progress `AppendEntries`.
    ///
    /// `expires_at` is when a grant stops counting: the duration the
    /// probe asked for, timed from when it was *sent*. The follower
    /// starts the same duration only once it receives the request, so
    /// timing it from the send keeps the leader's view strictly inside
    /// the follower's promise, by however long the request took to
    /// arrive.
    GetLeaseResponse {
        from: String,
        resp: GetLeaseResponse,
        expires_at: std::time::Instant,
    },
    /// `body` yields the byte stream `RaftLog::export_snapshot` produced
    /// (magic/version/seq/term header included) as it arrives off the
    /// wire -- `grpc::RaftPeerService` pumps the gRPC chunks into it but
    /// doesn't touch storage itself, so the term check and the actual
    /// install both happen here, same as every other inbound RPC.
    ///
    /// A reader rather than a `Vec<u8>` because a snapshot is the whole
    /// applied state of the cluster and there is no size at which
    /// holding all of it in memory becomes the right idea. Dropping this
    /// event without reading it -- which is what rejecting on term does
    /// -- closes the channel and tells the sender to stop.
    InstallSnapshot {
        header: SnapshotHeader,
        body: crate::store::ChunkReader,
        respond_to: oneshot::Sender<InstallSnapshotResponse>,
    },
    /// The leader's own snapshot-send task reporting back what happened,
    /// same pattern as `AppendEntriesResponse` -- `last_included_seq` is
    /// carried alongside the raw proto response (which only has `term`)
    /// so a successful install can advance `Leader::peer_match_index`
    /// the same way a successful `AppendEntries` does.
    InstallSnapshotResponse {
        from: String,
        resp: InstallSnapshotResponse,
        last_included_seq: u64,
    },
    ElectionTimeout,
    /// Self-notification, leader only: nudges the event loop to recompute
    /// `commit_seq`/`compact_seq` even when no peer response triggers it.
    /// Needed for a zero-peer cluster (nothing else would ever advance
    /// `commit_tx` after a client write there), and also keeps watermarks
    /// from stalling between replication rounds in a multi-peer one.
    LeaderTick,
    /// Self-notification: a `Config` entry was just appended to our own
    /// log, so the active configuration has changed *as of now* -- a
    /// configuration takes effect on append, not on commit (Raft §4.1).
    /// A membership change to propose. Handled on the leader's event
    /// loop, which is the only place with the state to enforce the two
    /// guards a config change needs (see `leader::Leader::propose_config`).
    ProposeConfig {
        change: ConfigChange,
        respond_to: oneshot::Sender<Result<u64, RaftError>>,
    },
    /// An operator asking this node to form a single-node cluster around
    /// itself (Raft §4.4). Only ever accepted by a node that has no
    /// configuration *and* an empty log -- see `Follower::bootstrap`.
    Bootstrap {
        config: ClusterConfig,
        respond_to: oneshot::Sender<Result<(), RaftError>>,
    },
}

impl Event {
    /// The term carried by an RPC *response*, if this is one.
    ///
    /// Raft §5.1 -- "if an RPC request or response contains term T >
    /// currentTerm, set currentTerm = T and convert to follower" -- is the
    /// one rule that applies identically to every role, and this is how it
    /// gets applied in exactly one place (`Common::next_event`) instead of
    /// being rewritten in each of them.
    ///
    /// Deliberately responses only. A *request* carrying a higher term
    /// obliges each role to answer differently before stepping down -- a
    /// leader rejects the append, a follower accepts it, a candidate
    /// evaluates the vote -- so those stay in the role event loops where
    /// the reply is composed. A response has no reply to compose: the only
    /// thing left to do with a higher term is adopt it.
    ///
    /// The match is exhaustive rather than falling through a wildcard, so
    /// a new variant cannot quietly opt out of the rule.
    pub(super) fn response_term(&self) -> Option<u64> {
        match self {
            Self::AppendEntriesResponse { resp, .. } => Some(resp.term),
            Self::RequestVoteResponse { resp, .. } => Some(resp.term),
            Self::InstallSnapshotResponse { resp, .. } => Some(resp.term),
            // `GetLeaseResponse` carries no term at all: a lease is
            // granted against the term the prober named, so there is
            // never a newer one to learn from it.
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

/// Which role to construct next. A plain tag -- unlike the role structs
/// themselves, it carries no state, since only one role's state is ever
/// alive at a time.
pub(crate) enum Role {
    Follower,
    /// Non-binding pre-vote round -- see the module doc.
    PreCandidate,
    Candidate,
    Leader,
    /// Not a role: the node is shutting down (its cancellation token
    /// fired, or the event channel closed), so `run` stops instead of
    /// building another one. Every role's event loop can return this, and
    /// only the driver ever looks at it.
    Stopped,
}

/// What a role's event loop decided after handling one event.
pub(super) enum Transition {
    Continue,
    Become(Role),
}

#[derive(Debug)]
pub enum RaftError {
    /// Not (or no longer) the leader. `leader_hint` lets a client retry in
    /// the right place, same as `RaftError`/`NotLeader` in client.proto.
    NotLeader {
        leader_hint: Option<String>,
    },
    Storage(StorageError),
    /// The event loop is gone (node shutting down) before it could reply.
    Disconnected,
    /// `Bootstrap` on a node that is already part of a cluster. Refused
    /// rather than applied, since bootstrapping twice is how one cluster
    /// silently becomes two -- see `follower::Follower::bootstrap`.
    AlreadyInitialized,
    /// A membership change is already appended and not yet committed.
    /// Raft allows only one at a time (§4.1): overlapping single-server
    /// changes can produce two configurations with no common majority,
    /// which is exactly the disjoint-quorum split the one-at-a-time rule
    /// exists to prevent.
    ConfigChangeInProgress,
    /// This leader hasn't yet committed an entry from its own term, so it
    /// can't safely propose a membership change (Ongaro 2015): until
    /// then it can't be sure the configuration it is building on won't
    /// itself be rolled back.
    NotCaughtUp,
    /// The change doesn't apply to the current configuration -- adding a
    /// server that is already a member, removing one that isn't, or
    /// removing the last voter.
    InvalidConfigChange(&'static str),
}

/// `(common, role_rx, peer_handle, commit_rx, compact_rx)` -- the state
/// machine plus the external handles a caller needs to observe/drive it.
/// A single-server membership change (Raft §4.1 -- one server at a time,
/// not joint consensus). The caller says what it wants; the leader
/// resolves it against whatever configuration is actually in force, so a
/// client never has to send (or race on) a whole configuration.
#[derive(Clone, Debug)]
pub enum ConfigChange {
    /// Add `server_id` as a non-voting learner. It is promoted to voter
    /// automatically once it has caught up -- see
    /// `leader::Leader::review_learner_progress`. Adding it as a voter
    /// outright would let a server that is hours behind count toward
    /// (and stall) every quorum from the moment it joined.
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
    /// Change the cluster's heartbeat, and with it every timeout derived
    /// from it (see `Timing`). Membership is untouched. Goes through the
    /// log like any other configuration change, so it takes effect on
    /// each node as that node appends it.
    SetHeartbeat {
        millis: u64,
    },
}

impl ConfigChange {
    /// `config` with this change applied, or why it can't be.
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
                    // The removal could never commit: there would be no
                    // one left to form a quorum of the new configuration.
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
