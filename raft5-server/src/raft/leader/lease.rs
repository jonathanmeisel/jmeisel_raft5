//! Asking peers whether they still recognise this leader.
//!
//! Deliberately separate from replication (see `super::replication`) and
//! much cheaper: a lease probe does no disk I/O on either side, so a
//! follower busy appending a large batch can still answer one promptly.
//! That is the whole point of the split -- it's what lets a leader learn
//! it has lost its read quorum without waiting on `AppendEntries`.

use super::*;

/// Sends one `GetLease` to `peer`, same shape as the other peer RPCs,
/// bounded by the tight `LIGHTWEIGHT_RPC_TIMEOUT` (this one does no disk
/// I/O, so it should always be fast -- that's the whole point of keeping
/// it separate from `AppendEntries`). `None` on any failure -- the caller
/// (a lease-probing task, running continuously) just retries on its next
/// probe.
async fn get_lease_from_peer(
    peer: &PeerInfo,
    req: GetLeaseRequest,
    conns: &PeerConnections,
) -> Option<GetLeaseResponse> {
    let mut client = conns.client(peer)?;
    let attempt = client.get_lease(req);
    match tokio::time::timeout(LIGHTWEIGHT_RPC_TIMEOUT, attempt).await {
        Ok(Ok(resp)) => Some(resp.into_inner()),
        _ => None,
    }
}

/// Repeatedly probes `peer` for a lease renewal at `HEARTBEAT` --
/// independent of, and much cheaper than, `spawn_replication`'s
/// `AppendEntries`, so a follower busy appending a large batch doesn't
/// prevent the leader from promptly learning whether it's still trusted.
/// Every response is forwarded as `Event::GetLeaseResponse`, same pattern
/// as replication and voting.
pub(super) fn spawn_lease_prober(
    cancel: &CancellationToken,
    peer: PeerInfo,
    leader_id: String,
    term: u64,
    conns: Arc<PeerConnections>,
    event_tx: mpsc::Sender<Event>,
) -> Task {
    Task::spawn(cancel, move |cancel| async move {
        loop {
            let req = GetLeaseRequest {
                leader_id: leader_id.clone(),
                term,
            };
            let Some(probed) =
                until_cancelled(&cancel, get_lease_from_peer(&peer, req, &conns)).await
            else {
                return;
            };
            if let Some(resp) = probed
                && !send_event(
                    &cancel,
                    &event_tx,
                    Event::GetLeaseResponse {
                        from: peer.id.clone(),
                        resp,
                    },
                )
                .await
            {
                return;
            }
            if until_cancelled(&cancel, tokio::time::sleep(HEARTBEAT))
                .await
                .is_none()
            {
                return;
            }
        }
    })
}
