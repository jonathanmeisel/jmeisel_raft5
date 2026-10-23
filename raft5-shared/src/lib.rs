//! Generated protobuf and gRPC types shared by the raft5 server and client:
//! `client.proto` (the `Kv` service) and `cluster.proto` (`ClusterMember`,
//! `ClusterConfig`). See `build.rs`.

pub mod raft5 {
    pub mod cluster {
        include!(concat!(env!("OUT_DIR"), "/raft5.cluster.rs"));
    }
    pub mod client {
        include!(concat!(env!("OUT_DIR"), "/raft5.client.rs"));
    }
}

mod config;
pub mod tls;

/// The most key/value data one transaction may buffer, in raw key and
/// value bytes. The server enforces it; it lives here because the client
/// sizes its gRPC message limit from the same number.
pub const MAX_TRANSACTION_BYTES: usize = 10 * 1024 * 1024;

/// The largest `Kv` gRPC message either side must accept: a maximum-size
/// transaction plus room for the protobuf framing around it (field tags,
/// length prefixes, other fields). Generous on purpose: being a little
/// over costs nothing, and being under refuses a request that the payload
/// limit allows. tonic's 4 MiB default would refuse it in the transport,
/// where the transaction limit cannot explain itself.
pub const MAX_KV_MESSAGE_BYTES: usize = MAX_TRANSACTION_BYTES + 1024 * 1024;

/// The most keys one `Read` may name. Refused past this, not shortened:
/// the caller chose the number and knows it in advance.
///
/// In a transaction, it also caps how many read locks one call can take,
/// which is why it is not larger: every key a transactional read names is
/// held until the transaction ends, the same argument as for the server's
/// key-scan cap.
pub const MAX_KEYS_PER_READ: usize = 1024;

/// How many value bytes one `Read` response carries before it is cut
/// short and returns a prefix of the keys asked for.
///
/// Equal to `MAX_TRANSACTION_BYTES` on purpose: no writable value is
/// larger, so the first key always fits alone and every call makes
/// progress. What `MAX_KV_MESSAGE_BYTES` allows beyond this covers the
/// framing of `MAX_KEYS_PER_READ` entries many times over.
pub const MAX_READ_VALUE_BYTES: usize = MAX_TRANSACTION_BYTES;

/// The built-in group whose members are the cluster's administrators.
///
/// Created at bootstrap and administered by its own members. Membership is
/// transitive like any group's, so whoever administers a group nested in
/// this one decides who is an administrator.
pub const ADMINS_GROUP: &str = "admins";

/// The built-in group whose members may take backups and create or remove
/// snapshots without being administrators. A backup ignores ACLs and holds
/// every password hash, so membership gives read access to the whole
/// cluster.
pub const BACKUP_OPERATORS_GROUP: &str = "backup-operators";
