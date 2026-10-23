//! Where a request goes, and what the client remembers between calls.
//!
//! Only the leader serves transactional work (every write, and every read
//! that must be current) and membership changes, and a node that is not
//! the leader answers `NotLeader` instead of forwarding (Raft §6.2). So the
//! client must find the leader, and should not have to find it again for
//! every call. `Router` does that. It holds:
//! - every client API address it knows,
//! - the `server_id` to address map that turns a `NotLeader.leader_hint`
//!   into somewhere to dial,
//! - the connection to the node it last saw leading.

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
/// One handle behind an `Arc`, cloned into each `Node`, so a login reaches
/// connections built before it as well as after, above all the cached
/// leader connection, which outlives single calls on purpose. A token
/// stored on the `Client` would leave that cached connection sending the
/// old token, or none.
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
        // No token is not an error here. A client that has not logged in can call
        // `GetConfig` and `Login`, and on a cluster with no secret it can call
        // everything. What to do about a missing token is the server's decision.
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

/// How long one discovery `GetConfig` may take before the scan gives up on
/// that node and moves on. A node that is down refuses at once; one that
/// drops packets silently would otherwise stall the scan for the rest of
/// the call's time.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// One node's client API: how to reach it, and what to call it.
#[derive(Clone)]
pub(crate) struct Node {
    /// The address as a normalized URI, so one node named two slightly
    /// different ways (a seed's spelling and the configuration's) is
    /// recognized as one node, not two.
    pub(crate) key: String,
    endpoint: Endpoint,
    token: BearerToken,
}

impl Node {
    /// With `tls`, the node is dialed over TLS whatever scheme `address` names.
    /// The cluster advertises what its members were configured with, which for
    /// a cluster configured without TLS is `http://`, and tonic would dial that
    /// in plaintext (see `raft5_shared::tls`). Normalizing before `key` is taken
    /// also keeps a seed and the configuration's spelling of it as one node.
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

    /// Never blocks: `connect_lazy` does no I/O, it only describes how to
    /// reach the node. The channel dials on first use and re-dials by itself
    /// after a failure.
    ///
    /// The decode limit is raised because a read can return a value as large as
    /// a whole transaction, which tonic's 4 MiB default would refuse. Encoding
    /// has no limit by default, so sending needs nothing.
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
    /// Kept outside `state` because the interceptor reads it on every request,
    /// from inside the connection, with no way to reach this lock.
    token: BearerToken,
    /// How every node (the seeds and every member learned later) is dialed
    /// over TLS, or `None` for plaintext.
    tls: Option<ClientTlsConfig>,
}

struct State {
    /// Every client API address we know: the seeds we were given, replaced by
    /// the cluster's own membership as soon as we see it.
    endpoints: Vec<Node>,
    /// `server_id` to that member's client API. This makes a `leader_hint`
    /// usable: the hint names a server, not an address.
    by_id: HashMap<String, Node>,
    /// Kept between calls, so the common case (the leader has not changed)
    /// dials nothing.
    leader: Option<Leader>,
    /// What to log in with again when a token expires mid-session. `None` for
    /// a client that was given a token directly, or never logged in: then
    /// there is nothing to retry with, and the expiry is reported.
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

    /// Start using `token` on every connection, existing ones included.
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

    /// A connection to the node that should serve the next request: the node
    /// `hint` names, else the one we last saw leading, else whichever node a
    /// new scan finds.
    pub(crate) async fn leader(&self, hint: Option<String>) -> Result<Leader, Error> {
        if let Some(id) = hint
            && let Some(node) = self.lookup(&id)
        {
            return Ok(self.remember(&node));
        }
        // Getting here means no hint, or a hint naming a server we have no
        // address for (a configuration we never fetched, or one from before that
        // server joined). The scan below refreshes it from whoever answers.
        if let Some(leader) = self.cached() {
            return Ok(leader);
        }
        self.discover().await
    }

    fn lookup(&self, server_id: &str) -> Option<Node> {
        self.state.lock().unwrap().by_id.get(server_id).cloned()
    }

    /// Drop the cached connection to `key`, if that node is still who we think
    /// leads. Called when it answers `NotLeader` or stops answering: either way
    /// the next call must look again.
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

    /// Ask `kv`'s node what the cluster looks like, and adopt the answer. Any
    /// node can answer, leader or not: membership is local, read-only state. A
    /// node that cannot answer teaches us nothing, which is not worth failing a
    /// call over. Returns whether a configuration was adopted.
    pub(crate) async fn refresh_config(&self, kv: &Kv) -> bool {
        self.ask(kv).await.is_some_and(|(adopted, _)| adopted)
    }

    /// One `GetConfig`: adopt the membership it carries, and report
    /// `(adopted, is_leader)`. `None` is a node that did not answer.
    ///
    /// Both come from one call because a scan wants both: a node that says it
    /// is not the leader has still told us where the rest of the cluster is,
    /// which is often how the leader is found.
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

    /// Replace what we know about the cluster with what the cluster says.
    /// Seed addresses that are no longer in the configuration belong to
    /// servers that left (or were never in it): every request to one costs a
    /// round trip to hear `NotLeader`, and a stale read there is answered from
    /// a log that no longer moves.
    ///
    /// Returns whether it adopted anything: an empty configuration is ignored.
    fn adopt(&self, members: Vec<ClusterMember>) -> bool {
        // An empty configuration means a node not yet bootstrapped or added to a
        // cluster. It knows less than we do.
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
        // The node we were talking to has left the cluster. More requests to it
        // would get nothing but refusals.
        let left = state
            .leader
            .as_ref()
            .is_some_and(|l| !state.endpoints.iter().any(|n| n.key == l.key));
        if left {
            state.leader = None;
        }
        true
    }

    /// Find the leader by asking every known node in turn for the cluster
    /// configuration, and taking the one whose answer says `is_leader`. Only a
    /// caught-up leader that still holds a read quorum sets it, so it proves
    /// leadership; a deposed leader that has not noticed yet says false, like a
    /// follower.
    ///
    /// The same answer carries the membership, so every node reached is worth
    /// asking even once it is clearly not the leader: a client seeded with one
    /// follower has no other way to learn where the rest of the cluster,
    /// leader included, is.
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

    /// The first known address this scan has not reached yet. Read from the
    /// state each time, not copied at the start, because a `GetConfig` mid-scan
    /// can add members we were never seeded with, often including the leader.
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
