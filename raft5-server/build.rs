fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // Generates this one field as `bytes::Bytes` so chunking an
        // encoded batch is a refcounted slice rather than a copy of
        // every byte. Deliberately scoped to it: doing the same to
        // `raft5.common.KeyValue` would push `Bytes` into `pb.rs`'s
        // conversions and `storage::Mutation`, which hold `Vec<u8>`, and
        // the copy would just reappear there.
        .bytes(".raft5.raft.AppendEntriesChunk.entries")
        .compile_protos(&["proto/common.proto", "proto/raft.proto"], &["proto"])?;
    Ok(())
}
