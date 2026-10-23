//! Snapshots, and backups taken from the current state or a snapshot.

use raft5_shared::raft5::client::{
    BackupRequest, CreateSnapshotRequest, CreateSnapshotResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, RemoveSnapshotRequest, RemoveSnapshotResponse, backup_chunk,
    create_snapshot_response, list_snapshots_response, remove_snapshot_response,
};

use crate::{Client, Error, Reply, denied, status_error};
use crate::routing::Kv;

/// What [`Client::backup`] wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupInfo {
    /// The size of the backup, in bytes.
    pub bytes: u64,
}

/// A point-in-time view of the store, kept until it is removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub id: Vec<u8>,
    /// The seq this snapshot reads at. Every write at or below this seq is
    /// visible through it; nothing above it is, however long it lives.
    pub seq: u64,
    /// What the creating client's clock said. For information only: it is
    /// one machine's wall clock, stored as given, and nothing in the cluster
    /// depends on it.
    pub created_unix_ms: u64,
}

impl Client {
    /// Save the current seq as a snapshot, so that reads and key walks at
    /// [`Consistency::Snapshot`] can see the store as it is now.
    ///
    /// `id` must be unique: creating one that exists is refused, not used to
    /// move the snapshot forward. `None` uses this machine's clock as a
    /// timestamp. The clock belongs on the client: the cluster has no wall
    /// clock, and a leader-assigned time would be replicated
    /// non-determinism. Two defaults in the same millisecond collide as a
    /// normal "already exists", which [`create_snapshot`] resolves with a
    /// suffix.
    ///
    /// Members of [`ADMINS_GROUP`] or [`BACKUP_OPERATORS_GROUP`] only: until
    /// it is removed, a snapshot keeps every version of every key that
    /// changed after it.
    ///
    /// [`create_snapshot`]: Client::create_snapshot
    /// [`Consistency::Snapshot`]: crate::Consistency::Snapshot
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    /// [`BACKUP_OPERATORS_GROUP`]: crate::BACKUP_OPERATORS_GROUP
    pub async fn create_snapshot(&self, id: Option<&[u8]>) -> Result<SnapshotInfo, Error> {
        let now = unix_millis();
        match id {
            Some(id) => self.create_snapshot_once(id, now).await,
            None => {
                let base = timestamp_id(now);
                match self.create_snapshot_once(base.as_bytes(), now).await {
                    // Two snapshots defaulted in the same millisecond. Resolve it here: the
                    // caller asked for "a snapshot of now", and the suffix is not its
                    // problem.
                    Err(Error::Rejected(_)) => {
                        let mut last = Err(Error::Rejected("snapshot id taken".into()));
                        for n in 1..16u32 {
                            let id = format!("{base}-{n}");
                            last = self.create_snapshot_once(id.as_bytes(), now).await;
                            if !matches!(last, Err(Error::Rejected(_))) {
                                break;
                            }
                        }
                        last
                    }
                    other => other,
                }
            }
        }
    }

    async fn create_snapshot_once(
        &self,
        id: &[u8],
        created_unix_ms: u64,
    ) -> Result<SnapshotInfo, Error> {
        let request = CreateSnapshotRequest {
            snapshot_id: id.to_vec(),
            created_unix_ms,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_snapshot(request).await.map(create_snapshot_reply) }
        })
        .await
    }

    /// Release a snapshot. The versions it kept can be collected, and reads
    /// at it start to fail. Members of [`ADMINS_GROUP`] or
    /// [`BACKUP_OPERATORS_GROUP`] only, and a backup operator may remove any
    /// snapshot.
    ///
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    /// [`BACKUP_OPERATORS_GROUP`]: crate::BACKUP_OPERATORS_GROUP
    pub async fn remove_snapshot(&self, id: &[u8]) -> Result<u64, Error> {
        let request = RemoveSnapshotRequest {
            snapshot_id: id.to_vec(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.remove_snapshot(request).await.map(remove_snapshot_reply) }
        })
        .await
    }

    /// Every snapshot that exists.
    ///
    /// The only way to find ids: snapshot records live in the reserved range,
    /// which [`key_iterator`] hides. Possibly stale, like any read outside a
    /// transaction.
    ///
    /// [`key_iterator`]: Client::key_iterator
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_snapshots(ListSnapshotsRequest {})
                .await
                .map(list_snapshots_reply)
        })
        .await
    }

    /// Stream a backup of the cluster's key/value state into `out`, as of now
    /// or as of the snapshot `snapshot_id`. Members of [`ADMINS_GROUP`] or
    /// [`BACKUP_OPERATORS_GROUP`] only. Returns how many bytes were written.
    ///
    /// The backup holds every key a client wrote, with its newest value, plus
    /// accounts, groups and ACLs. ACLs do not apply to it, so whoever takes
    /// one can read every key and password hash. It leaves out invitations, snapshots and
    /// the token signing secret: a cluster restored from it signs tokens with
    /// whatever secret it is started with, so old tokens work only if that is
    /// the same secret.
    ///
    /// Not retried once data has started to arrive: a backup that fails
    /// partway leaves `out` with a partial file, which a restore refuses.
    /// Write to a temporary file and rename it on success.
    ///
    /// See [`bootstrap_from_backup`] for the restore.
    ///
    /// [`bootstrap_from_backup`]: Client::bootstrap_from_backup
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    /// [`BACKUP_OPERATORS_GROUP`]: crate::BACKUP_OPERATORS_GROUP
    pub async fn backup<W>(
        &self,
        snapshot_id: Option<&[u8]>,
        out: &mut W,
    ) -> Result<BackupInfo, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt;

        let request = BackupRequest {
            snapshot_id: snapshot_id.unwrap_or_default().to_vec(),
        };
        let mut relogged_in = false;
        let mut stream = loop {
            let leader = self.router.leader(None).await?;
            match leader.kv.clone().backup(request.clone()).await {
                Ok(response) => break response.into_inner(),
                // An expired token is the common case, as in `call_inner`: log in
                // again once, if this client has credentials.
                Err(status) if status.code() == tonic::Code::Unauthenticated && !relogged_in => {
                    relogged_in = true;
                    match self.router.credentials() {
                        Some((username, password)) => {
                            self.refresh_token(&username, &password).await?;
                        }
                        None => return Err(status_error(status)),
                    }
                }
                Err(status) => return Err(status_error(status)),
            }
        };

        let mut bytes = 0u64;
        while let Some(chunk) = stream.message().await.map_err(status_error)? {
            match chunk.result {
                Some(backup_chunk::Result::Data(data)) => {
                    out.write_all(&data)
                        .await
                        .map_err(|e| Error::Rejected(format!("could not write the backup: {e}")))?;
                    bytes += data.len() as u64;
                }
                Some(backup_chunk::Result::AuthError(e)) => match denied::<()>(e) {
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    _ => unreachable!("`denied` only returns `Reply::Denied`"),
                },
                None => {
                    return Err(Error::Unavailable(
                        "the node sent an empty backup chunk".to_string(),
                    ));
                }
            }
        }
        out.flush()
            .await
            .map_err(|e| Error::Rejected(format!("could not write the backup: {e}")))?;
        Ok(BackupInfo { bytes })
    }
}

fn snapshot_info(s: raft5_shared::raft5::client::Snapshot) -> SnapshotInfo {
    SnapshotInfo {
        id: s.snapshot_id,
        seq: s.seq,
        created_unix_ms: s.created_unix_ms,
    }
}

reply! {
    fn create_snapshot_reply(CreateSnapshotResponse, create_snapshot_response) -> SnapshotInfo {
        Ok(s) => Reply::Ok(snapshot_info(s)),
    }

    fn remove_snapshot_reply(RemoveSnapshotResponse, remove_snapshot_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn list_snapshots_reply(
        ListSnapshotsResponse,
        list_snapshots_response,
        without_auth
    ) -> Vec<SnapshotInfo> {
        Ok(list) => Reply::Ok(list.snapshots.into_iter().map(snapshot_info).collect()),
    }
}

/// Milliseconds since the Unix epoch, by this machine's clock.
///
/// The cluster has no wall clock, so a timestamp only means "what the
/// client that asked believed the time was".
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A default snapshot id: the timestamp, in a form the server's id
/// character set accepts, and that sorts by time.
fn timestamp_id(unix_ms: u64) -> String {
    format!("t{unix_ms}")
}
