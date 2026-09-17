//! Where a request goes, and what the client remembers between calls.
//!
//! Only the leader can serve a write, a membership change, or a
//! linearizable read, and a node that isn't the leader answers
//! `NotLeader` rather than forwarding (Raft §6.2) -- so finding the
//! leader, and not having to find it again for every call, is the
//! client's job. `Router` is that job: it holds every client API address
//! it knows of, the `server_id` -> address map that turns a
//! `NotLeader.leader_hint` into somewhere to dial, and the connection to
//! whoever it last saw leading.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use log::{debug, warn};
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use raft5_shared::raft5::client::kv_client::KvClient;
use raft5_shared::raft5::client::{GetConfigRequest, ReadRequest, read_response};
use raft5_shared::raft5::cluster::ClusterMember;

use crate::Error;

pub(crate) type Kv = KvClient<Channel>;

/// How long a single discovery RPC -- a `GetConfig`, a leader probe --
/// may take before the scan gives up on that node and moves on. A node
/// that is merely down refuses immediately; one that is black-holing
/// packets would otherwise stall the scan for as long as the call has
/// left.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// The key the leader probe reads. Its contents are irrelevant and it is
/// never written: what the probe asks is not "what is this value" but
/// "are you the leader, and do you still hold a read quorum", and a
/// linearizable read succeeding at all is the answer (see
/// `grpc::KvService::read` -- no one else will serve one).
const PROBE_KEY: &[u8] = b"__raft5_client_leader_probe__";

/// One node's client API: how to reach it, and what to call it.
#[derive(Clone)]
pub(crate) struct Node {
    /// The address as a normalized URI, so the same node named two
    /// slightly different ways -- a seed's spelling versus the
    /// configuration's -- is recognized as one node rather than two.
    pub(crate) key: String,
    endpoint: Endpoint,
}

impl Node {
    fn new(address: &str) -> Result<Self, Error> {
        let endpoint = Endpoint::from_shared(address.to_string())
            .map_err(|_| Error::InvalidAddress(address.to_string()))?;
        Ok(Self {
            key: endpoint.uri().to_string(),
            endpoint,
        })
    }

    /// Never blocks: `connect_lazy` performs no I/O, it only describes
    /// how to reach the node. The channel dials on first use and re-dials
    /// by itself after a failure.
    pub(crate) fn connect(&self) -> Kv {
        KvClient::new(self.endpoint.clone().connect_lazy())
    }
}

/// The node a request should go to, and the connection to send it on.
#[derive(Clone)]
pub(crate) struct Leader {
    pub(crate) key: String,
    pub(crate) kv: Kv,
}

pub(crate) struct Router {
    state: Mutex<State>,
}

struct State {
    /// Every client API address we know of: the seeds we were handed,
    /// replaced by the cluster's own membership as soon as we have seen
    /// it.
    endpoints: Vec<Node>,
    /// `server_id` -> that member's client API, which is what makes a
    /// `leader_hint` actionable: the hint names a server, not an address.
    by_id: HashMap<String, Node>,
    /// Held between calls, so the common case -- the leader hasn't
    /// changed since last time -- dials nothing.
    leader: Option<Leader>,
}

impl Router {
    pub(crate) fn new(seeds: impl IntoIterator<Item = impl AsRef<str>>) -> Result<Self, Error> {
        let endpoints = seeds
            .into_iter()
            .map(|s| Node::new(s.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if endpoints.is_empty() {
            return Err(Error::NoSeeds);
        }
        Ok(Self {
            state: Mutex::new(State {
                endpoints,
                by_id: HashMap::new(),
                leader: None,
            }),
        })
    }

    /// A connection to whoever should serve the next request: the node
    /// `hint` names, else the one we last saw leading, else whoever a
    /// fresh scan turns up.
    pub(crate) async fn leader(&self, hint: Option<String>) -> Result<Leader, Error> {
        if let Some(id) = hint
            && let Some(node) = self.lookup(&id)
        {
            return Ok(self.remember(&node));
        }
        // Falling through means either no hint, or a hint naming a server
        // we have no address for -- a configuration we have never
        // fetched, or one from before that server joined. The scan below
        // refreshes it from whoever answers.
        if let Some(leader) = self.cached() {
            return Ok(leader);
        }
        self.discover().await
    }

    fn lookup(&self, server_id: &str) -> Option<Node> {
        self.state.lock().unwrap().by_id.get(server_id).cloned()
    }

    /// Drops the cached connection to `key`, if that is still who we
    /// think is leading. Called when it answers `NotLeader` or stops
    /// answering at all: either way the next call has to look again.
    pub(crate) fn forget(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        if state.leader.as_ref().is_some_and(|l| l.key == key) {
            state.leader = None;
        }
    }

    pub(crate) fn cached(&self) -> Option<Leader> {
        self.state.lock().unwrap().leader.clone()
    }

    pub(crate) fn endpoints(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .endpoints
            .iter()
            .map(|n| n.key.clone())
            .collect()
    }

    pub(crate) fn nodes(&self) -> Vec<Node> {
        self.state.lock().unwrap().endpoints.clone()
    }

    /// Asks `kv`'s node what the cluster looks like and adopts the
    /// answer. Any node can answer, leader or not: membership is local,
    /// read-only state. A node that can't answer teaches us nothing,
    /// which is not worth failing a call over.
    pub(crate) async fn refresh_config(&self, kv: &Kv) {
        let mut kv = kv.clone();
        let members =
            match tokio::time::timeout(PROBE_TIMEOUT, kv.get_config(GetConfigRequest {})).await {
                Ok(Ok(resp)) => resp.into_inner().members,
                Ok(Err(status)) => return debug!("GetConfig refused: {status}"),
                Err(_) => return debug!("GetConfig timed out"),
            };
        self.adopt(members);
    }

    /// Replaces what we know about the cluster with what the cluster
    /// itself says. Addresses we were seeded with that no longer appear
    /// in the configuration belong to servers that have left it (or were
    /// never in it): every request sent to one of those costs a round
    /// trip to be told `NotLeader`, and a stale read sent to one is
    /// answered from a log that has stopped moving.
    fn adopt(&self, members: Vec<ClusterMember>) {
        // An empty configuration is a node that hasn't been bootstrapped
        // or added to a cluster yet. It knows less than we do.
        if members.is_empty() {
            return;
        }

        let mut endpoints = Vec::new();
        let mut by_id = HashMap::new();
        for member in members {
            match Node::new(&member.client_address) {
                Ok(node) => {
                    endpoints.push(node.clone());
                    by_id.insert(member.server_id, node);
                }
                Err(_) => warn!(
                    "cluster member {} advertises an unusable client address {:?}",
                    member.server_id, member.client_address
                ),
            }
        }

        let mut state = self.state.lock().unwrap();
        state.endpoints = endpoints;
        state.by_id = by_id;
        // The node we were talking to has left the cluster. Continuing to
        // send it requests would earn nothing but refusals.
        let left = state
            .leader
            .as_ref()
            .is_some_and(|l| !state.endpoints.iter().any(|n| n.key == l.key));
        if left {
            state.leader = None;
        }
    }

    /// Finds the leader by asking every node we know of, in turn, to
    /// serve a linearizable read. Only a leader that still holds a read
    /// quorum will, so an answer is proof of leadership rather than a
    /// guess -- a deposed leader that hasn't noticed yet fails the same
    /// probe a follower does.
    async fn discover(&self) -> Result<Leader, Error> {
        let mut tried: Vec<String> = Vec::new();
        let mut last: Option<String> = None;

        while let Some(node) = self.next_untried(&tried) {
            tried.push(node.key.clone());
            let kv = node.connect();

            // Ask for the configuration before probing, even though this
            // node may well not be the leader: a client seeded with a
            // single follower has no other way to learn where the rest of
            // the cluster -- the leader included -- can be reached.
            self.refresh_config(&kv).await;

            match probe(&kv).await {
                Ok(true) => return Ok(self.remember(&node)),
                Ok(false) => last = Some(format!("{} is not the leader", node.key)),
                Err(status) => last = Some(format!("{}: {status}", node.key)),
            }
        }

        Err(Error::Unavailable(last.unwrap_or_else(|| {
            "the client knows of no cluster members to ask".to_string()
        })))
    }

    /// The first address we know of that this scan hasn't reached yet.
    /// Re-read from the state each time rather than snapshotted up front,
    /// because a `GetConfig` mid-scan can add members we were never
    /// seeded with -- including, often, the leader itself.
    fn next_untried(&self, tried: &[String]) -> Option<Node> {
        self.state
            .lock()
            .unwrap()
            .endpoints
            .iter()
            .find(|n| !tried.contains(&n.key))
            .cloned()
    }

    fn remember(&self, node: &Node) -> Leader {
        let leader = Leader {
            key: node.key.clone(),
            kv: node.connect(),
        };
        self.state.lock().unwrap().leader = Some(leader.clone());
        leader
    }
}

/// Whether this node answered a linearizable read -- see `PROBE_KEY`.
async fn probe(kv: &Kv) -> Result<bool, Status> {
    let mut kv = kv.clone();
    let request = ReadRequest {
        key: PROBE_KEY.to_vec(),
        linearizable: true,
    };
    let resp = match tokio::time::timeout(PROBE_TIMEOUT, kv.read(request)).await {
        Ok(resp) => resp?,
        Err(_) => return Err(Status::deadline_exceeded("leader probe timed out")),
    };
    Ok(matches!(
        resp.into_inner().result,
        Some(read_response::Result::Ok(_))
    ))
}

#[cfg(test)]
mod tests;
