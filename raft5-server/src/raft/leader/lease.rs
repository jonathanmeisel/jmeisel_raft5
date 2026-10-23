//! Asking peers whether they still recognize this leader.
//!
//! Separate from replication (see `super::replication`) on purpose, and
//! much cheaper: a lease probe does no disk I/O on either side, so a
//! follower busy appending a large batch can still answer one quickly. That
//! is the reason for the split: it lets a leader learn that it lost its read
//! quorum without waiting on `AppendEntries`.

use super::*;

/// Send one `GetLease` to `peer`, like the other peer RPCs, within the
/// tight `timeout` (`Timing::lightweight_rpc_timeout`: this does no disk
/// I/O, so it should always be fast). `None` on any failure: the caller (a
/// lease-probing task that runs continuously) retries on its next probe.
async fn get_lease_from_peer(
    peer: &PeerInfo,
    req: GetLeaseRequest,
    conns: &PeerConnections,
    timeout: Duration,
) -> Option<GetLeaseResponse> {
    // `lease_client`, not `client`: a probe on the same connection as a
    // replication batch waits behind it, and a probe that waits has already
    // failed. See `PeerConnections`.
    let mut client = conns.lease_client(peer)?;
    let attempt = client.get_lease(req);
    match tokio::time::timeout(timeout, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    }
}

/// Probe `peer` for a lease renewal every heartbeat. Separate from, and
/// much cheaper than, `spawn_replication`'s `AppendEntries`, so a follower
/// busy appending a large batch does not stop the leader from learning
/// quickly whether it is still trusted. Every response is forwarded as
/// `Event::GetLeaseResponse`, like replication and voting.
///
/// Timing is read from `timing_rx` on every probe, so a heartbeat change
/// takes effect on the next one.
pub(super) fn spawn_lease_prober(
    cancel: &CancellationToken,
    peer: PeerInfo,
    leader_id: String,
    term: u64,
    conns: Arc<PeerConnections>,
    timing_rx: watch::Receiver<Timing>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        loop {
            let timing = *timing_rx.borrow();
            let req = GetLeaseRequest {
                leader_id: leader_id.clone(),
                term,
                lease_millis: timing.lease_ms(),
            };
            let expires_at = Instant::now() + timing.lease();
            let probe = get_lease_from_peer(&peer, req, &conns, timing.lightweight_rpc_timeout());
            let Some(probed) = until_cancelled(&cancel, probe).await else {
                return;
            };
            if let Some(resp) = probed
                && !send_event(
                    &cancel,
                    &event_tx,
                    Event::GetLeaseResponse {
                        from: peer.id.clone(),
                        resp,
                        expires_at,
                    },
                )
                .await
            {
                return;
            }
            if until_cancelled(&cancel, tokio::time::sleep(timing.heartbeat()))
                .await
                .is_none()
            {
                return;
            }
        }
    })
}
