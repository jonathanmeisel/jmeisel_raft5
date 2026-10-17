//! Asking peers whether they still recognise this leader.
//!
//! Deliberately separate from replication (see `super::replication`) and
//! much cheaper: a lease probe does no disk I/O on either side, so a
//! follower busy appending a large batch can still answer one promptly.
//! That is the whole point of the split -- it's what lets a leader learn
//! it has lost its read quorum without waiting on `AppendEntries`.

use super::*;

/// Sends one `GetLease` to `peer`, same shape as the other peer RPCs,
/// bounded by the tight `timeout` (`Timing::lightweight_rpc_timeout`:
/// this one does no disk I/O, so it should always be fast -- that's the
/// whole point of keeping it separate from `AppendEntries`). `None` on
/// any failure -- the caller (a lease-probing task, running continuously)
/// just retries on its next probe.
async fn get_lease_from_peer(
    peer: &PeerInfo,
    req: GetLeaseRequest,
    conns: &PeerConnections,
    timeout: Duration,
) -> Option<GetLeaseResponse> {
    // `lease_client`, not `client`: a probe on the same connection as a
    // replication batch waits behind it, and a probe that waits is a
    // probe that has already failed. See `PeerConnections`.
    let mut client = conns.lease_client(peer)?;
    let attempt = client.get_lease(req);
    match tokio::time::timeout(timeout, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    }
}

/// Repeatedly probes `peer` for a lease renewal every heartbeat --
/// independent of, and much cheaper than, `spawn_replication`'s
/// `AppendEntries`, so a follower busy appending a large batch doesn't
/// prevent the leader from promptly learning whether it's still trusted.
/// Every response is forwarded as `Event::GetLeaseResponse`, same pattern
/// as replication and voting.
///
/// Timing is re-read from `timing_rx` on every probe, so a heartbeat
/// change takes effect on the next one.
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
