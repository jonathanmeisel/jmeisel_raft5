//! Cluster membership, the heartbeat, and bootstrap (plain or from a backup).

use std::future::Future;

use tonic::Status;

use raft5_shared::raft5::client::{
    AddServerRequest, BootstrapFromBackupChunk, BootstrapRequest, ConfigChangeResponse,
    GetConfigRequest, RemoveServerRequest, SetHeartbeatRequest, bootstrap_from_backup_chunk,
    config_change_response,
};

use crate::{Client, Error, Reply, status_error};
use crate::routing::Kv;

/// The cluster's membership, as [`Client::members`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    /// The id of the member that answered, if it was leading when it
    /// answered. Through a [`Client`] it nearly always is, because the client
    /// asks the leader. `None` from a leader not yet ready to say so (see
    /// `GetConfigResponse.is_leader`).
    pub leader: Option<String>,
    pub members: Vec<Member>,
}

/// One member of the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub server_id: String,
    /// Where the other servers replicate to it: its peer API, the address
    /// given to [`Client::add_server`].
    pub peer_address: String,
    /// Where clients reach it.
    pub client_address: String,
    /// Whether it votes and counts toward the commit quorum. A server added
    /// with [`Client::add_server`] starts as a non-voting learner, and the
    /// leader promotes it once it keeps up in real time. So `true` for a new
    /// server means "it has caught up".
    pub voting: bool,
}

impl Client {
    /// Add a server to the cluster, at the address of its peer API (the one
    /// other nodes replicate to). The leader dials it there (over the
    /// cluster's peer mTLS, if any) to ask the server its id and client
    /// address. It joins as a non-voting learner and is promoted
    /// automatically when it catches up (Raft §4.2.1), so this one call is the
    /// whole operation.
    ///
    /// The scheme of `peer_address` does not matter: the leader uses TLS or
    /// not, as its own configuration says.
    ///
    /// Returns once the change commits. Only one membership change can be in
    /// flight at a time; a call that collides with another is retried like
    /// any other temporary refusal.
    pub async fn add_server(&self, peer_address: &str) -> Result<(), Error> {
        let request = AddServerRequest {
            peer_address: peer_address.to_string(),
        };
        self.change_config(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.add_server(request).await.map(config_reply) }
        })
        .await
    }

    /// Remove a server from the cluster. Returns once the change commits.
    pub async fn remove_server(&self, server_id: &str) -> Result<(), Error> {
        let request = RemoveServerRequest {
            server_id: server_id.to_string(),
        };
        self.change_config(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.remove_server(request).await.map(config_reply) }
        })
        .await
    }

    /// Change the cluster's heartbeat interval, from which every election and
    /// lease timeout is derived. Returns once the change commits. Raise it for
    /// nodes far apart: it should be well above the round trip between any two
    /// servers.
    pub async fn set_heartbeat(&self, heartbeat_ms: u64) -> Result<(), Error> {
        let request = SetHeartbeatRequest {
            heartbeat_millis: heartbeat_ms,
        };
        self.change_config(
            |mut kv: Kv| async move { kv.set_heartbeat(request).await.map(config_reply) },
        )
        .await
    }

    /// The cluster's heartbeat interval, as the node this client talks to
    /// sees it. Like a stale [`read`], it may briefly lag a change just made.
    ///
    /// [`read`]: Client::read
    pub async fn heartbeat_ms(&self) -> Result<u64, Error> {
        self.call(|mut kv: Kv| async move {
            kv.get_config(GetConfigRequest {})
                .await
                .map(|resp| Reply::Ok(resp.into_inner().heartbeat_millis))
        })
        .await
    }

    /// Every member of the cluster, and whether each votes yet, from the
    /// leader's configuration (a follower's may briefly lag). A server added
    /// with [`add_server`] shows as non-voting until it catches up.
    ///
    /// [`add_server`]: Client::add_server
    pub async fn members(&self) -> Result<Membership, Error> {
        self.call(|mut kv: Kv| async move {
            kv.get_config(GetConfigRequest {}).await.map(|resp| {
                let resp = resp.into_inner();
                let leader = resp
                    .is_leader
                    .then(|| resp.identity.map(|me| me.server_id))
                    .flatten();
                Reply::Ok(Membership {
                    leader,
                    members: resp
                        .members
                        .into_iter()
                        .map(|m| Member {
                            server_id: m.server_id,
                            peer_address: m.address,
                            client_address: m.client_address,
                            voting: m.voting,
                        })
                        .collect(),
                })
            })
        })
        .await
    }

    /// Form a single-node cluster around the node named `server_id` (Raft
    /// §4.4), which must be one of this client's endpoints. Every other node
    /// joins that cluster through [`add_server`].
    ///
    /// Sent to each endpoint in turn. A node refuses a `server_id` that is not
    /// its own, so the guard picks which node acts, and a node already in a
    /// cluster refuses outright. Unlike every other call, this one does not
    /// retry: bootstrap cannot be undone, and a node that just started may need
    /// a moment before it accepts one.
    ///
    /// `heartbeat_ms` is the new cluster's heartbeat interval, or `None` for
    /// the server's default. See [`set_heartbeat`].
    ///
    /// [`add_server`]: Client::add_server
    /// [`set_heartbeat`]: Client::set_heartbeat
    pub async fn bootstrap(&self, server_id: &str, heartbeat_ms: Option<u64>) -> Result<(), Error> {
        if server_id.is_empty() {
            return Err(Error::InvalidArgument(
                "bootstrap needs the id of the node to bootstrap",
            ));
        }

        let request = BootstrapRequest {
            server_id: server_id.to_string(),
            heartbeat_millis: heartbeat_ms.unwrap_or(0),
        };
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.bootstrap(request.clone()).await {
                Ok(resp) => match config_reply(resp) {
                    Reply::Ok(()) => return Ok(()),
                    // The cluster's answer, not this node's state, so the next node would
                    // give the same one.
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    // A membership change has no key, no precondition, and no transaction.
                    // Every other variant cannot happen here, and each means the same thing
                    // anyway: this node said no.
                    _ => last = Some(format!("{} refused to bootstrap", node.key)),
                },
                // Report this, do not fold it into "nobody could serve it" below: an
                // operator whose token is missing, or whose identity is not the one this
                // node was started with, must be told so, not told the cluster was busy.
                // Same reasoning as in `call_inner`.
                Err(status) if status.code() == tonic::Code::Unauthenticated => {
                    return Err(Error::Unauthenticated(status.message().to_string()));
                }
                Err(status) if status.code() == tonic::Code::PermissionDenied => {
                    return Err(Error::PermissionDenied(status.message().to_string()));
                }
                Err(status) => last = Some(format!("{}: {status}", node.key)),
            }
        }

        Err(Error::Unavailable(last.unwrap_or_else(|| {
            format!("no endpoint of this client is called {server_id:?}")
        })))
    }

    /// Form a single-node cluster around the node named `server_id`, as
    /// [`bootstrap`] does, holding the contents of the backup that `backup`
    /// reads. Every other node then joins through [`add_server`], and gets the
    /// data by snapshot transfer.
    ///
    /// - The node must be in no cluster and hold no data.
    /// - Keys keep the seqs they had in the source cluster.
    /// - Accounts come from the backup, passwords included. The administrator
    ///   the node was started with is created only if the backup has no
    ///   account with that username; when it has one, the backed-up password
    ///   stays. This client must be logged in as that startup administrator,
    ///   as for [`bootstrap`].
    /// - The token signing secret is not in the backup: the new cluster uses
    ///   whatever secret it was started with.
    ///
    /// The node checks the backup before it becomes a cluster. A bad backup
    /// is [`Error::Rejected`] with the reason, and leaves the node as it was,
    /// so the call can be repeated. Not retried: the backup is read once.
    ///
    /// [`bootstrap`]: Client::bootstrap
    /// [`add_server`]: Client::add_server
    pub async fn bootstrap_from_backup<R>(
        &self,
        server_id: &str,
        heartbeat_ms: Option<u64>,
        mut backup: R,
    ) -> Result<(), Error>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        use tokio::io::AsyncReadExt;

        if server_id.is_empty() {
            return Err(Error::InvalidArgument(
                "bootstrap needs the id of the node to bootstrap",
            ));
        }

        // Find the node by its own identity first: the backup can be read only
        // once, so it must go to the right node on the first try.
        let mut target = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            if let Ok(config) = kv.get_config(GetConfigRequest {}).await
                && config
                    .get_ref()
                    .identity
                    .as_ref()
                    .is_some_and(|m| m.server_id == server_id)
            {
                target = Some(kv);
                break;
            }
        }
        let Some(mut kv) = target else {
            return Err(Error::Unavailable(format!(
                "no endpoint of this client is called {server_id:?}"
            )));
        };

        // The reader runs in its own task and feeds a bounded channel, so only
        // a few chunks are in memory however large the backup is.
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let header = BootstrapFromBackupChunk {
            part: Some(bootstrap_from_backup_chunk::Part::Header(
                BootstrapRequest {
                    server_id: server_id.to_string(),
                    heartbeat_millis: heartbeat_ms.unwrap_or(0),
                },
            )),
        };
        tx.send(header)
            .await
            .expect("the receiver is still held here");
        let reading = tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = backup.read(&mut buf).await?;
                if n == 0 {
                    return Ok::<(), std::io::Error>(());
                }
                let chunk = BootstrapFromBackupChunk {
                    part: Some(bootstrap_from_backup_chunk::Part::Data(buf[..n].to_vec())),
                };
                // The node stopped reading: its answer says why.
                if tx.send(chunk).await.is_err() {
                    return Ok(());
                }
            }
        });

        let answered = kv
            .bootstrap_from_backup(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await;
        // A local read error ends the stream early, so the node reports a
        // truncated backup. The local error says more.
        match reading.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(Error::Rejected(format!("could not read the backup: {e}"))),
            Err(e) => return Err(Error::Rejected(format!("the backup reader failed: {e}"))),
        }
        match answered {
            Ok(response) => match config_reply(response) {
                Reply::Ok(()) => {
                    self.router.refresh_config(&kv).await;
                    Ok(())
                }
                Reply::Denied { reason, detail } => Err(Error::Denied { reason, detail }),
                _ => Err(Error::Unavailable(format!(
                    "{server_id} refused to bootstrap"
                ))),
            },
            // Already in a cluster, or holding data: no retry will change it.
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                Err(Error::Rejected(status.message().to_string()))
            }
            Err(status) => Err(status_error(status)),
        }
    }

    /// `call`, and then a re-read of the membership on success: the client
    /// just changed the cluster's members, so its own view is out of date.
    async fn change_config<F, Fut>(&self, op: F) -> Result<(), Error>
    where
        F: Fn(Kv) -> Fut,
        Fut: Future<Output = Result<Reply<()>, Status>>,
    {
        self.call(op).await?;
        if let Some(leader) = self.router.cached() {
            self.router.refresh_config(&leader.kv).await;
        }
        Ok(())
    }
}

reply! {
    fn config_reply(ConfigChangeResponse, config_change_response) -> () {
        Ok(true) => Reply::Ok(()),
        Ok(false) => Reply::Unavailable("the leader did not apply the change".to_string()),
    }
}
