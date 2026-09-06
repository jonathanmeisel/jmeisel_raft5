//! Generated protobuf/gRPC types from `proto/*.proto` (see `build.rs`).

pub mod raft5 {
    pub mod common {
        include!(concat!(env!("OUT_DIR"), "/raft5.common.rs"));
    }
    pub mod client {
        include!(concat!(env!("OUT_DIR"), "/raft5.client.rs"));
    }
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/raft5.raft.rs"));
    }
}
