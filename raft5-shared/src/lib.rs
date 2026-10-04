//! Generated protobuf/gRPC types shared between the raft5 server and
//! client: the `client.proto` (`Kv` service) and `cluster.proto`
//! (`ClusterMember`/`ClusterConfig`) definitions (see `build.rs`).

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
/// value bytes. Enforced by the server; it lives here because the client
/// must size its gRPC message limit from the same number.
pub const MAX_TRANSACTION_BYTES: usize = 10 * 1024 * 1024;

/// The largest `Kv` gRPC message either side must accept: a maximal
/// transaction plus room for the protobuf framing around it (field tags,
/// length prefixes, the message's other fields). Generous on purpose --
/// being a little over is free, and being under refuses a request the
/// payload limit says is fine. tonic's 4 MiB default would refuse it in
/// the transport, where the transaction limit has no way to explain itself.
pub const MAX_KV_MESSAGE_BYTES: usize = MAX_TRANSACTION_BYTES + 1024 * 1024;
