//! Where a request goes, and what the client remembers between calls.
//!
//! Only the leader can serve anything transactional -- which is to say
//! every write and every read that has to be current -- or a membership
//! change, and a node that isn't the leader answers `NotLeader` rather
//! than forwarding (Raft §6.2). So finding the leader, and not having to
//! find it again for every call, is the client's job. `Router` is that
//! job: it holds every client API address it knows of, the `server_id`
//! -> address map that turns a `NotLeader.leader_hint` into somewhere to
//! dial, and the connection to whoever it last saw leading.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use tonic::Status;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use raft5_shared::raft5::client::kv_client::KvClient;
use raft5_shared::raft5::client::GetConfigRequest;
use raft5_shared::raft5::cluster::ClusterMember;
use raft5_shared::MAX_KV_MESSAGE_BYTES;

use crate::Error;

pub(crate) type Kv = KvClient<InterceptedService<Channel, BearerToken>>;

/// The session token, shared by every connection this client holds.
///
/// One handle behind an `Arc`, cloned into each `Node`, so that logging
/// in reaches connections built *before* the login as well as after --
/// the cached leader connection above all, which deliberately outlives
/// individual calls. Storing the token on the `Client` instead would
/// leave that cached connection sending the old one, or none.
#[derive(Clone, Default)]
pub(crate) struct BearerToken(Arc<Mutex<Option<String>>>);

impl BearerToken {
    pub(crate) fn set(&self, token: Option<String>) {
        *self.0.lock().unwrap() = token;
    }

    pub(crate) fn is_set(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }
}

impl Interceptor for BearerToken {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        // No token is not an error here. A client that has not logged in
        // is perfectly able to call `GetConfig` and `Login`, and against
        // a cluster with no secret it can call everything -- so what to
        // do about a missing token is the server's to decide, not this.
        let Some(token) = self.0.lock().unwrap().clone() else {
            return Ok(request);
        };
        let value = format!("Bearer {token}")
            .parse()
            .map_err(|_| Status::invalid_argument("the session token is not a usable header"))?;
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

/// How long a single discovery `GetConfig` may take before the scan
/// gives up on that node and moves on. A node that is merely down
/// refuses immediately; one that is black-holing packets would otherwise
/// stall the scan for as long as the call has left.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// One node's client API: how to reach it, and what to call it.
#[derive(Clone)]
pub(crate) struct Node {
    /// The address as a normalized URI, so the same node named two
    /// slightly different ways -- a seed's spelling versus the
    /// configuration's -- is recognized as one node rather than two.
    pub(crate) key: String,
    endpoint: Endpoint,
    token: BearerToken,
}

impl Node {
    /// With `tls`, the node is dialed over TLS whatever scheme `address`
    /// names -- the cluster advertises whatever its members were
    /// configured with, which for a cluster that predates TLS is
    /// `http://`, and tonic would dial that in plaintext (see
    /// `raft5_shared::tls`). Normalizing before `key` is taken also keeps
    /// a seed and the configuration's spelling of it one node, not two.
    fn new(address: &str, token: BearerToken, tls: Option<&ClientTlsConfig>) -> Result<Self, Error> {
        let mut endpoint =
            Endpoint::from_shared(raft5_shared::tls::with_scheme(address, tls.is_some()))
                .map_err(|_| Error::InvalidAddress(address.to_string()))?;
        if let Some(tls) = tls {
            endpoint = endpoint
                .tls_config(tls.clone())
                .map_err(|e| Error::Tls(e.to_string()))?;
        }
        Ok(Self {
            key: endpoint.uri().to_string(),
            endpoint,
            token,
        })
    }

    /// Never blocks: `connect_lazy` performs no I/O, it only describes
    /// how to reach the node. The channel dials on first use and re-dials
    /// by itself after a failure.
    ///
    /// The decoding limit is raised because a read can return a value as
    /// large as a whole transaction; tonic's 4 MiB default would refuse
    /// it. Encoding is unlimited by default, so sending needs nothing.
    pub(crate) fn connect(&self) -> Kv {
        KvClient::new(InterceptedService::new(
            self.endpoint.clone().connect_lazy(),
            self.token.clone(),
        ))
        .max_decoding_message_size(MAX_KV_MESSAGE_BYTES)
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
    /// Held outside `state` because the interceptor reads it on every
    /// request, from inside the connection, with no way to reach this
    /// lock.
    token: BearerToken,
    /// How every node is dialed over TLS, or `None` for plaintext --
    /// the seeds and every member learned afterwards alike.
    tls: Option<ClientTlsConfig>,
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
    /// What to log in again with when a token expires mid-session.
    /// `None` for a client that was handed a token directly, or has
    /// never logged in: there is then nothing to retry with, and an
    /// expiry is simply reported.
    credentials: Option<(String, String)>,
}

impl Router {
    pub(crate) fn new(
        seeds: impl IntoIterator<Item = impl AsRef<str>>,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self, Error> {
        let token = BearerToken::default();
        let endpoints = seeds
            .into_iter()
            .map(|s| Node::new(s.as_ref(), token.clone(), tls.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if endpoints.is_empty() {
            return Err(Error::NoSeeds);
        }
        Ok(Self {
            state: Mutex::new(State {
                endpoints,
                by_id: HashMap::new(),
                leader: None,
                credentials: None,
            }),
            token,
            tls,
        })
    }

    /// Starts using `token` on every connection, existing ones included.
    pub(crate) fn set_token(&self, token: Option<String>) {
        self.token.set(token);
    }

    pub(crate) fn has_token(&self) -> bool {
        self.token.is_set()
    }

    pub(crate) fn set_credentials(&self, credentials: Option<(String, String)>) {
        self.state.lock().unwrap().credentials = credentials;
    }

    pub(crate) fn credentials(&self) -> Option<(String, String)> {
        self.state.lock().unwrap().credentials.clone()
    }

    pub(crate) fn username(&self) -> Option<String> {
        self.state
            .lock()
            .unwrap()
            .credentials
            .as_ref()
            .map(|(username, _)| username.clone())
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
    /// which is not worth failing a call over. Returns whether a
    /// configuration was adopted.
    pub(crate) async fn refresh_config(&self, kv: &Kv) -> bool {
        self.ask(kv).await.is_some_and(|(adopted, _)| adopted)
    }

    /// One `GetConfig`: adopt whatever membership it carries, and report
    /// `(adopted, is_leader)`. `None` is a node that didn't answer.
    ///
    /// Both halves come from the one call because a scan wants both --
    /// a node that says it isn't the leader has still just told us where
    /// the rest of the cluster is, which is often how the leader is
    /// found at all.
    async fn ask(&self, kv: &Kv) -> Option<(bool, bool)> {
        let mut kv = kv.clone();
        let resp =
            match tokio::time::timeout(PROBE_TIMEOUT, kv.get_config(GetConfigRequest {})).await {
                Ok(Ok(resp)) => resp.into_inner(),
                Ok(Err(status)) => {
                    debug!("GetConfig refused: {status}");
                    return None;
                }
                Err(_) => {
                    debug!("GetConfig timed out");
                    return None;
                }
            };
        let is_leader = resp.is_leader;
        Some((self.adopt(resp.members), is_leader))
    }

    /// Replaces what we know about the cluster with what the cluster
    /// itself says. Addresses we were seeded with that no longer appear
    /// in the configuration belong to servers that have left it (or were
    /// never in it): every request sent to one of those costs a round
    /// trip to be told `NotLeader`, and a stale read sent to one is
    /// answered from a log that has stopped moving.
    ///
    /// Returns whether it did: an empty configuration is ignored.
    fn adopt(&self, members: Vec<ClusterMember>) -> bool {
        // An empty configuration is a node that hasn't been bootstrapped
        // or added to a cluster yet. It knows less than we do.
        if members.is_empty() {
            return false;
        }

        let mut endpoints = Vec::new();
        let mut by_id = HashMap::new();
        for member in members {
            match Node::new(&member.client_address, self.token.clone(), self.tls.as_ref()) {
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
        true
    }

    /// Finds the leader by asking every node we know of, in turn, for the
    /// cluster configuration, and taking the one whose answer says
    /// `is_leader`. Only a caught-up leader still holding a read quorum
    /// sets it, so it is proof of leadership rather than a guess -- a
    /// deposed leader that hasn't noticed yet says false, same as a
    /// follower.
    ///
    /// The same answer carries the membership, which is why every node
    /// reached is worth asking even once it is clear it isn't the leader:
    /// a client seeded with a single follower has no other way to learn
    /// where the rest of the cluster -- the leader included -- can be
    /// reached.
    async fn discover(&self) -> Result<Leader, Error> {
        let mut tried: Vec<String> = Vec::new();
        let mut last: Option<String> = None;

        while let Some(node) = self.next_untried(&tried) {
            tried.push(node.key.clone());
            let kv = node.connect();

            match self.ask(&kv).await {
                Some((_, true)) => return Ok(self.remember(&node)),
                Some((_, false)) => last = Some(format!("{} is not the leader", node.key)),
                None => last = Some(format!("{} did not answer", node.key)),
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

#[cfg(test)]
mod tests;
