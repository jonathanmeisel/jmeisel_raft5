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
