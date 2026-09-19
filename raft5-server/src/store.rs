//! The async face of `storage`.
//!
//! `storage::RaftLog` is deliberately synchronous -- fjall's API is, and
//! keeping the storage layer free of an executor is what lets it be
//! reasoned about and tested without one. `Store` is the other half of
//! that bargain: the single place where a synchronous storage call is put
//! on the blocking pool, so that no async caller anywhere in the crate
//! writes `spawn_blocking` itself.
//!
//! That matters for more than tidiness. Blocking an executor thread on
//! disk I/O -- even briefly -- stalls every other task scheduled on it,
//! including the heartbeats and lease probes that decide whether this node
//! is still considered alive. Having exactly one place that dispatches
//! storage work means that rule cannot be forgotten at a new call site.
//!
//! `ChunkReader`/`ChunkWriter` are the other direction of the same
//! bargain. `RaftLog::export_snapshot` and `install_snapshot` are
//! written against `std::io::Write`/`Read` and already stream -- but
//! both of their callers live in async code that moves the bytes over
//! gRPC a message at a time, and the only way to join those two worlds
//! used to be a `Vec<u8>` holding the entire snapshot. These are the
//! seam instead: a blocking `Read`/`Write` on one side, an async
//! channel on the other, and a bounded amount in flight between them.

use std::io::{self, Read, Write};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::storage::{self, RaftLog};

/// How much a `ChunkWriter` gathers before handing a chunk across, and
/// so how large one `SnapshotChunk` is on the wire.
///
/// `export_snapshot` writes in small pieces -- a length prefix, a key, a
/// value -- and a channel send per piece would cost far more than the
/// copy this avoids. Small enough that `CHUNK_BACKLOG` of them is a
/// modest amount of memory, and far under any message size limit, which
/// is all the wire asks of it.
const CHUNK_SIZE: usize = 64 * 1024;

/// How many chunks may be in flight between the async side and the
/// blocking one. This times `CHUNK_SIZE` is the whole memory cost of a
/// snapshot transfer, however large the snapshot is -- which is the
/// entire point of these two types.
pub const CHUNK_BACKLOG: usize = 16;

/// An async handle to this node's log. Cheap to clone -- it is an `Arc`
/// and nothing else -- so every component that needs storage holds its
/// own rather than borrowing one.
#[derive(Clone)]
pub struct Store {
    log: Arc<RaftLog>,
}

impl Store {
    pub fn new(log: Arc<RaftLog>) -> Self {
        Self { log }
    }

    /// Runs `f` against the log on the blocking pool.
    ///
    /// Takes a closure rather than mirroring `RaftLog`'s methods one for
    /// one, and deliberately: several callers need a *composite*
    /// operation -- match-then-truncate-then-append for an inbound
    /// `AppendEntries`, or read-tail-then-build-batch for replication --
    /// whose steps must not be interleaved with another writer's. One
    /// closure is one blocking task, so composing inside `f` is what
    /// keeps that property; composing out here, from several `run` calls,
    /// would silently lose it.
    ///
    /// A panic in `f` propagates. `spawn_blocking` only fails to join if
    /// its closure panicked, so unwrapping that here -- rather than at
    /// each of the callers that used to -- costs nothing and says the
    /// policy once: a storage panic is fail-stop (see `raft`'s module
    /// doc on why continuing past one is worse than not).
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

    /// `run`, for the callers whose policy on a storage failure is to
    /// stop rather than carry on with state they could not read or write.
    /// `what` completes the sentence "failed to ...".
    ///
    /// This is most of the state machine: a role's event loop has no
    /// meaningful way to recover from, say, being unable to read the term
    /// at a log index -- proceeding on a guess is how a consensus
    /// implementation silently violates its own safety properties. The
    /// exceptions are the paths that genuinely can report the failure
    /// onward (a client write) or retry it (storage maintenance), and
    /// those use `run` and match on the `Result` themselves.
    /// `run`, but without waiting for the result.
    ///
    /// For the one caller that has to consume a storage operation's
    /// output *while it is still running* -- streaming a snapshot out to
    /// a peer, where awaiting the export first is exactly the
    /// buffer-it-all behaviour being removed. Still goes through here
    /// rather than calling `spawn_blocking` at the call site, so this
    /// module remains the only place that does.
    pub fn spawn<T, F>(&self, f: F) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce(&RaftLog) -> T + Send + 'static,
        T: Send + 'static,
    {
        let log = Arc::clone(&self.log);
        tokio::task::spawn_blocking(move || f(&log))
    }

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
/// `install_snapshot` wants a `Read` and runs on the blocking pool;
/// the chunks arrive on a gRPC stream in async code. This is the join:
/// `blocking_recv` parks the blocking-pool thread (which is what that
/// pool is for) until the next chunk arrives, so the snapshot is
/// consumed as it comes in rather than assembled first.
///
/// The producer closing its side is end-of-stream, which is exactly what
/// a `Read` returning 0 means -- so a truncated transfer surfaces as
/// `install_snapshot` failing to parse, not as a silent short read.
pub struct ChunkReader {
    rx: mpsc::Receiver<Vec<u8>>,
    /// What's left of the chunk currently being handed out.
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
/// A send failing means the consumer is gone -- the peer hung up, or the
/// RPC was cancelled. That is reported as a broken pipe so
/// `export_snapshot` stops walking the keyspace instead of finishing an
/// export nobody is listening for.
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
