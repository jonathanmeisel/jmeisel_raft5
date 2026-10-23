//! The async interface to `storage`.
//!
//! `storage::RaftLog` is synchronous on purpose: fjall's API is, and a
//! storage layer free of an executor can be reasoned about and tested
//! without one. `Store` is the other half of that deal: the only place
//! that puts a synchronous storage call on the blocking pool, so no async
//! caller in the crate writes `spawn_blocking` itself.
//!
//! This matters for more than neatness. Blocking an executor thread on
//! disk I/O, even briefly, stalls every other task on that thread,
//! including the heartbeats and lease probes that decide whether this node
//! is seen as alive. With one place that dispatches storage work, a new
//! call site cannot forget the rule.
//!
//! `ChunkReader` and `ChunkWriter` cover the other direction.
//! `RaftLog::export_snapshot` and `install_snapshot` are written against
//! `std::io::Write`/`Read` and stream, but their callers are async code
//! that moves bytes over gRPC one message at a time. These two types join
//! the two sides without holding a whole snapshot in a `Vec<u8>`: a
//! blocking `Read`/`Write` on one side, an async channel on the other, and
//! a bounded amount in flight between them.

use std::io::{self, Read, Write};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::storage::{self, RaftLog};

/// How much a `ChunkWriter` collects before it hands a chunk across, and
/// so the size of one `SnapshotChunk` on the wire.
///
/// `export_snapshot` writes in small pieces (a length prefix, a key, a
/// value), and a channel send per piece would cost far more than the copy
/// this avoids. Small enough that `CHUNK_BACKLOG` of them is modest memory,
/// and far below any message size limit, which is all the wire needs.
const CHUNK_SIZE: usize = 64 * 1024;

/// How many chunks may be in flight between the async side and the
/// blocking side. This times `CHUNK_SIZE` is the whole memory cost of a
/// snapshot transfer, however large the snapshot is, which is the purpose
/// of these two types.
pub const CHUNK_BACKLOG: usize = 16;

/// An async handle to this node's log. Cheap to clone (it is only an
/// `Arc`), so every component that needs storage holds its own.
#[derive(Clone)]
pub struct Store {
    log: Arc<RaftLog>,
}

impl Store {
    pub fn new(log: Arc<RaftLog>) -> Self {
        Self { log }
    }

    /// Run `f` against the log on the blocking pool.
    ///
    /// Takes a closure instead of mirroring each `RaftLog` method, on purpose:
    /// several callers need a composite operation (match, truncate and append
    /// for an inbound `AppendEntries`; read the tail and build a batch for
    /// replication) whose steps must not interleave with another writer. One
    /// closure is one blocking task, so composing inside `f` keeps that
    /// property; composing here, from several `run` calls, would silently lose
    /// it.
    ///
    /// A panic in `f` propagates. `spawn_blocking` fails to join only if its
    /// closure panicked, so unwrapping here costs nothing and states the policy
    /// once: a storage panic is fail-stop (see `raft`'s module doc for why
    /// continuing past one is worse).
    pub async fn run<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&RaftLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || f(&log))
            .await
            .expect("storage task panicked")
    }

    /// `run`, but without waiting for the result.
    ///
    /// For the one caller that must use a storage operation's output while it
    /// still runs: streaming a snapshot to a peer, where awaiting the export
    /// first is the buffer-everything behavior this avoids. It still goes
    /// through here, not `spawn_blocking` at the call site, so this module stays
    /// the only place that calls it.
    pub fn spawn<T, F>(&self, f: F) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce(&RaftLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || f(&log))
    }

    /// `run`, for callers whose policy on a storage failure is to stop, not
    /// to continue with state they could not read or write. `what`
    /// completes the sentence "failed to ...".
    ///
    /// This is most of the state machine: a role's event loop cannot recover
    /// from, for example, failing to read the term at a log index, and
    /// guessing is how a consensus implementation silently breaks its own
    /// safety. The exceptions are paths that can report the failure (a
    /// client write) or retry it (storage maintenance); those use `run` and
    /// handle the `Result` themselves.
    pub async fn expect<T, F>(&self, what: &'static str, f: F) -> T
    where
        F: FnOnce(&RaftLog) -> storage::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        self.run(f)
            .await
            .unwrap_or_else(|e| panic!("failed to {what}: {e}"))
    }
}

/// A blocking `Read` fed by an async producer.
///
/// `install_snapshot` wants a `Read` and runs on the blocking pool, while
/// the chunks arrive on a gRPC stream in async code. This joins them:
/// `blocking_recv` parks the blocking-pool thread (which that pool is for)
/// until the next chunk arrives, so the snapshot is read as it comes in,
/// not assembled first.
///
/// The producer closing its side is end of stream, exactly what a `Read`
/// returning 0 means. So a truncated transfer shows up as
/// `install_snapshot` failing to parse, not as a silent short read.
pub struct ChunkReader {
    rx: mpsc::Receiver<Vec<u8>>,
    /// The rest of the chunk being handed out.
    current: Vec<u8>,
    at: usize,
}

impl ChunkReader {
    pub fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            at: 0,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.at == self.current.len() {
            match self.rx.blocking_recv() {
                Some(chunk) => {
                    self.current = chunk;
                    self.at = 0;
                }
                None => return Ok(0), // producer finished or gave up
            }
        }
        let n = buf.len().min(self.current.len() - self.at);
        buf[..n].copy_from_slice(&self.current[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// The mirror image: a blocking `Write` that hands its bytes to an async
/// consumer, in chunks, with a bounded number in flight.
///
/// A failed send means the consumer is gone (the peer hung up, or the RPC
/// was cancelled). That is reported as a broken pipe, so `export_snapshot`
/// stops walking the keyspace instead of finishing an export nobody will
/// read.
pub struct ChunkWriter {
    tx: mpsc::Sender<Vec<u8>>,
    pending: Vec<u8>,
}

impl ChunkWriter {
    pub fn new(tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self {
            tx,
            pending: Vec::with_capacity(CHUNK_SIZE),
        }
    }

    fn hand_over(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.pending, Vec::with_capacity(CHUNK_SIZE));
        self.tx.blocking_send(chunk).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "nobody is receiving this snapshot any more",
            )
        })
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);
        if self.pending.len() >= CHUNK_SIZE {
            self.hand_over()?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.hand_over()
    }
}
