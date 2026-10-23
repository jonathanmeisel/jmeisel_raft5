fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // Generate only this field as `bytes::Bytes`, so chunking an encoded
        // batch is a refcounted slice, not a copy of every byte. Limited to this
        // field on purpose: doing the same to `raft5.common.KeyValue` would push
        // `Bytes` into `pb.rs`'s conversions and `storage::Mutation`, which hold
        // `Vec<u8>`, and the copy would just move there.
        .bytes(".raft5.raft.AppendEntriesChunk.entries")
        .compile_protos(&["proto/common.proto", "proto/raft.proto"], &["proto"])?;
    Ok(())
}
