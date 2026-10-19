use super::*;

fn locks() -> Locks {
    let locks = Locks::new();
    locks.observe_term(Some(1));
    locks
}

fn txn(locks: &Locks) -> TxnId {
    locks
        .create(1, Duration::from_secs(1), fresh_id())
        .expect("a fresh transaction should open")
}

/// One key's `read_many`, values and all -- what a single-key
/// transactional read asks of the table.
fn read(locks: &Locks, t: TxnId, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, Refused> {
    locks
        .read_many(t, &[key.to_vec()], true)
        .map(|mut buffered| buffered.remove(0))
}

fn conflict<T: std::fmt::Debug>(r: Result<T, Refused>) -> Vec<u8> {
    match r {
        Err(Refused::Conflict { key }) => key,
        other => panic!("expected a lock conflict, got {other:?}"),
    }
}

/// Reading is shared: any number of transactions can hold the same key,
/// which is what lets them all proceed as long as none of them writes.
#[test]
fn many_transactions_can_read_the_same_key() {
    let locks = locks();
    let (a, b, c) = (txn(&locks), txn(&locks), txn(&locks));

    for t in [a, b, c] {
        assert_eq!(read(&locks, t, b"k"), Ok(None));
    }
    assert!(locks.is_locked(b"k"));
}

/// A transaction that is the *only* reader of a key may take it
/// exclusively -- read-then-write, the commonest thing a transaction
/// does.
#[test]
fn the_sole_reader_of_a_key_can_upgrade_to_a_write() {
    let locks = locks();
    let t = txn(&locks);

    read(&locks, t, b"k").unwrap();
    assert_eq!(locks.buffer_write(t, b"k", Some(b"v".to_vec())), Ok(()));
    // And the write is visible to the transaction that made it.
    assert_eq!(read(&locks, t, b"k"), Ok(Some(Some(b"v".to_vec()))));
}

/// The regression for encoding shared holders as a set rather than a
/// count: reading twice must not make a transaction look like two
/// readers, or it could never upgrade a key it alone had read.
#[test]
fn reading_the_same_key_twice_still_allows_an_upgrade() {
    let locks = locks();
    let t = txn(&locks);

    read(&locks, t, b"k").unwrap();
    read(&locks, t, b"k").unwrap();
    assert_eq!(
        locks.buffer_write(t, b"k", Some(b"v".to_vec())),
        Ok(()),
        "a transaction that read a key twice is still its only reader"
    );
}

/// Two readers, and the younger one wants to write: it is in the older
/// one's way, not the other way round, so it is refused and the older
/// one keeps what it had.
#[test]
fn a_younger_transaction_cannot_take_a_key_an_older_one_is_reading() {
    let locks = locks();
    let older = txn(&locks);
    let younger = txn(&locks);

    read(&locks, older, b"k").unwrap();
    read(&locks, younger, b"k").unwrap();

    assert_eq!(
        conflict(locks.buffer_write(younger, b"k", Some(b"v".to_vec()))),
        b"k".to_vec()
    );
    assert_eq!(locks.state_of(older), Some(TxnState::Open));
    assert_eq!(locks.state_of(younger), Some(TxnState::Open));
}

/// Wound-wait, the reason fail-fast alone isn't enough: an older
/// transaction takes the key rather than backing off, and the younger one
/// finds out why on its next request instead of being told it never
/// existed.
#[test]
fn an_older_transaction_wounds_a_younger_holder_and_takes_the_key() {
    let locks = locks();
    let older = txn(&locks);
    let younger = txn(&locks);

    locks.buffer_write(younger, b"k", Some(b"theirs".to_vec())).unwrap();
    assert_eq!(
        locks.buffer_write(older, b"k", Some(b"mine".to_vec())),
        Ok(()),
        "an older transaction takes the lock rather than failing"
    );

    assert_eq!(
        locks.state_of(younger),
        Some(TxnState::Aborted(AbortReason::Wounded))
    );
    assert_eq!(
        read(&locks, younger, b"k"),
        Err(Refused::Aborted(AbortReason::Wounded)),
        "the wounded transaction learns why, not just that it failed"
    );
    // And it gave up everything, not only the contested key.
    assert_eq!(
        locks.buffer_write(younger, b"other", Some(b"v".to_vec())),
        Err(Refused::Aborted(AbortReason::Wounded))
    );
}

/// The oldest transaction is never refused by another transaction,
/// however many of them are in its way. That is the progress property
/// wound-wait buys, so it is worth stating as a test rather than only in
/// a comment.
#[test]
fn the_oldest_transaction_wounds_everyone_in_its_way() {
    let locks = locks();
    let oldest = txn(&locks);
    let others: Vec<_> = (0..4).map(|_| txn(&locks)).collect();

    for (i, t) in others.iter().enumerate() {
        locks.buffer_write(*t, format!("k{i}").as_bytes(), Some(b"v".to_vec())).unwrap();
    }
    for i in 0..others.len() {
        assert_eq!(
            locks.buffer_write(oldest, format!("k{i}").as_bytes(), Some(b"mine".to_vec())),
            Ok(())
        );
    }
    for t in &others {
        assert_eq!(
            locks.state_of(*t),
            Some(TxnState::Aborted(AbortReason::Wounded))
        );
    }
}

/// A committing transaction's writes may already be in the log, and an
/// appended entry cannot be recalled -- so even an older transaction has
/// to wait its turn rather than wounding it.
#[test]
fn a_committing_transaction_is_not_wounded_by_an_older_one() {
    let locks = locks();
    let older = txn(&locks);
    let committing = txn(&locks);

    locks.buffer_write(committing, b"k", Some(b"v".to_vec())).unwrap();
    let (_writes, _guard) = locks.begin_commit(committing, 1).unwrap();

    assert_eq!(
        conflict(locks.buffer_write(older, b"k", Some(b"mine".to_vec()))),
        b"k".to_vec(),
        "an older transaction must not wound one that is already committing"
    );
}

/// An expired transaction holds nothing, and finds out the moment it asks
/// for anything. Released on the *conflict* path rather than by a
/// sweeper, so the key comes free as soon as somebody wants it.
#[test]
fn an_expired_transaction_releases_its_locks_to_the_next_request() {
    let locks = locks();
    let doomed = locks.create(1, Duration::ZERO, fresh_id()).unwrap();
    locks.buffer_write(doomed, b"k", Some(b"v".to_vec())).unwrap();
    assert!(locks.is_locked(b"k"));

    // Younger, so it could never *wound* the holder -- only the holder's
    // deadline can free this key.
    let later = txn(&locks);
    assert_eq!(locks.buffer_write(later, b"k", Some(b"v".to_vec())), Ok(()));

    assert_eq!(
        locks.state_of(doomed),
        Some(TxnState::Aborted(AbortReason::Expired))
    );
    assert_eq!(
        locks.begin_commit(doomed, 1).map(|_| ()),
        Err(Refused::Aborted(AbortReason::Expired)),
        "an expired transaction must not be able to commit"
    );
}

/// The sweeper is the backstop for a transaction nobody collides with,
/// and it reclaims the tombstone only after a grace period -- long enough
/// that its owner has had a chance to be told why it died.
#[test]
fn the_sweeper_reclaims_an_expired_transaction_nobody_touched() {
    let locks = locks();
    let doomed = locks.create(1, Duration::ZERO, fresh_id()).unwrap();
    locks.buffer_write(doomed, b"k", Some(b"v".to_vec())).unwrap();

    let now = Instant::now();
    locks.sweep(now, Duration::from_secs(30));
    assert!(!locks.is_locked(b"k"), "the sweeper should free an expired transaction's locks");
    assert_eq!(
        locks.state_of(doomed),
        Some(TxnState::Aborted(AbortReason::Expired)),
        "within the grace period the tombstone survives, so the owner can still be told why"
    );

    locks.sweep(now + Duration::from_secs(31), Duration::from_secs(30));
    assert_eq!(locks.state_of(doomed), None);
}

/// A transaction belongs to the leader that created it. A new term means
/// its reads were validated against state that no longer applies and its
/// commit can never happen, so it goes, and its locks go with it.
#[test]
fn a_change_of_leader_term_discards_every_transaction() {
    let locks = locks();
    let t = txn(&locks);
    locks.buffer_write(t, b"k", Some(b"v".to_vec())).unwrap();

    locks.observe_term(Some(2));

    assert_eq!(locks.state_of(t), None);
    assert!(!locks.is_locked(b"k"));

    let fresh = txn(&locks);
    assert_eq!(
        locks.buffer_write(fresh, b"k", Some(b"v".to_vec())),
        Ok(()),
        "the key is free for the new term"
    );
}

/// Committing hands back exactly what was buffered, in key order, and
/// dropping the guard gives the locks up however the commit turned out.
#[test]
fn committing_yields_the_buffered_writes_and_the_guard_releases_them() {
    let locks = locks();
    let t = txn(&locks);
    locks.buffer_write(t, b"b", Some(b"2".to_vec())).unwrap();
    locks.buffer_write(t, b"a", Some(b"1".to_vec())).unwrap();
    locks.buffer_write(t, b"gone", None).unwrap();
    read(&locks, t, b"witness").unwrap();

    let (writes, guard) = locks.begin_commit(t, 1).unwrap();
    assert_eq!(
        writes,
        vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"2".to_vec())),
            (b"gone".to_vec(), None),
        ]
    );
    assert!(locks.is_locked(b"a") && locks.is_locked(b"witness"));

    drop(guard);
    for key in [&b"a"[..], b"b", b"gone", b"witness"] {
        assert!(!locks.is_locked(key), "{key:?} should have been released");
    }
    assert_eq!(locks.state_of(t), None);
}

/// Caps are refusals, not clamps: a caller told "yes" to a lifetime this
/// server won't honour would act on a lease it doesn't have.
#[test]
fn an_over_long_lifetime_is_refused_rather_than_shortened() {
    let locks = locks();
    assert!(matches!(
        locks.create(1, MAX_TRANSACTION_TTL + Duration::from_secs(1), fresh_id()),
        Err(Refused::TooMuch(_))
    ));
    assert_eq!(locks.open_count(), 0);
}

/// A transaction too large to replicate would never commit -- and, being
/// already in the log and re-proposed forever, would block everything
/// after it. The only safe place to refuse it is before it reaches the
/// log at all.
#[test]
fn a_transaction_that_buffers_too_much_is_refused_before_it_can_commit() {
    let locks = locks();

    // One oversized value.
    let big = txn(&locks);
    assert!(matches!(
        locks.buffer_write(big, b"k", Some(vec![0u8; MAX_TRANSACTION_BYTES + 1])),
        Err(Refused::TooMuch(_))
    ));

    // ...and the same ceiling reached a megabyte at a time, which is
    // the shape a real transaction actually has.
    let many = txn(&locks);
    let chunk = vec![0u8; 1024 * 1024];
    let refused = (0..)
        .find_map(|i| locks.buffer_write(many, format!("k{i}").as_bytes(), Some(chunk.clone())).err());
    assert!(matches!(refused, Some(Refused::TooMuch(_))));
}

/// There is no separate limit on key *count*, so a great many tiny keys
/// have to be stopped by the byte cap alone -- which they only are
/// because `entry_size` charges each key an overhead on top of its
/// bytes. Without that, empty keys would be free and unbounded.
#[test]
fn a_transaction_of_countless_tiny_keys_is_still_bounded() {
    let locks = locks();
    let t = txn(&locks);

    let refused =
        (0..1_000_000).find_map(|i| locks.buffer_write(t, format!("k{i}").as_bytes(), None).err());
    assert!(
        matches!(refused, Some(Refused::TooMuch(_))),
        "keys with no value at all must still count against the budget"
    );
}

/// The per-transaction cap bounds one client; this bounds the node.
/// Without it, `MAX_OPEN_TRANSACTIONS` transactions at the per-
/// transaction ceiling would be gigabytes, all held on the leader before
/// anything has been proposed.
#[test]
fn the_node_wide_budget_bounds_what_every_transaction_holds_together() {
    let locks = locks();
    let chunk = vec![0u8; 1024 * 1024];

    // Fill the node budget across many transactions, each well under
    // its own limit.
    let mut refused = None;
    'outer: for _ in 0..(MAX_BUFFERED_BYTES_NODE_WIDE / MAX_TRANSACTION_BYTES + 2) {
        let t = txn(&locks);
        for i in 0..(MAX_TRANSACTION_BYTES / chunk.len()) {
            if let Err(e) = locks.buffer_write(t, format!("{t:?}-{i}").as_bytes(), Some(chunk.clone()))
            {
                refused = Some(e);
                break 'outer;
            }
        }
    }
    assert!(
        matches!(refused, Some(Refused::TooMuch(_))),
        "the node-wide budget should have stopped this before memory did"
    );
}

/// A transaction that goes away gives its budget back, or the node-wide
/// limit would be a one-way ratchet that eventually refuses everything.
#[test]
fn a_finished_transaction_returns_its_budget_to_the_node() {
    let locks = locks();
    let chunk = vec![0u8; 1024 * 1024];

    // Do the same work repeatedly, far more times than the node-wide
    // budget would allow if it were never released.
    for round in 0..(MAX_BUFFERED_BYTES_NODE_WIDE / MAX_TRANSACTION_BYTES + 4) {
        let t = txn(&locks);
        for i in 0..8 {
            locks
                .buffer_write(t, format!("r{round}-{i}").as_bytes(), Some(chunk.clone()))
                .expect("each round's budget should have been returned by the last");
        }
        locks.discard(t);
    }

    // The same holds for a transaction that was aborted rather than
    // dropped: it will never be written, so it stops counting at once.
    let doomed = txn(&locks);
    locks.buffer_write(doomed, b"k", Some(chunk.clone())).unwrap();
    let older = locks.create(1, Duration::from_secs(1), fresh_id()).unwrap();
    // Not actually older -- take the key by expiry instead.
    locks.discard(older);
    locks.sweep(Instant::now() + Duration::from_secs(60), Duration::ZERO);
    assert_eq!(locks.buffered_total(), 0);
}

/// Rewriting a key a transaction has already buffered replaces the value
/// rather than counting against the caps twice.
#[test]
fn rewriting_a_buffered_key_replaces_it_rather_than_accumulating() {
    let locks = locks();
    let t = txn(&locks);

    for _ in 0..100 {
        locks
            .buffer_write(t, b"k", Some(vec![0u8; MAX_TRANSACTION_BYTES / 2]))
            .expect("rewriting one key should not accumulate");
    }
    let (writes, _guard) = locks.begin_commit(t, 1).unwrap();
    assert_eq!(writes.len(), 1);
}

/// Dropping is idempotent and silent about an id it doesn't know:
/// commit-then-drop is the ordinary client path, and the drop shouldn't
/// have to know whether it raced its own commit.
#[test]
fn discarding_an_unknown_transaction_is_not_an_error() {
    let locks = locks();
    let t = txn(&locks);
    locks.discard(t);
    locks.discard(t);
    locks.discard(fresh_id());
}

// --- bulk reads and the buffer view, for key scans --------------------

/// Every key a batch names ends up in the read set and held, so a
/// many-key read's keys are as protected as a single one's.
#[test]
fn read_many_locks_every_key_it_is_given() {
    let locks = locks();
    let scanner = txn(&locks);

    assert_eq!(
        locks.read_many(scanner, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()], true),
        Ok(vec![None, None, None])
    );
    for key in [&b"a"[..], b"b", b"c"] {
        assert!(locks.is_locked(key));
    }

    // Held against a writer that can't displace this one: younger, so
    // it is refused rather than wounding its way in.
    let younger = txn(&locks);
    assert_eq!(
        conflict(locks.buffer_write(younger, b"b", Some(b"v".to_vec()))),
        b"b".to_vec()
    );
}

/// A refusal part-way leaves what it already took. Those locks are in
/// the read set now and are given up when the transaction ends -- and
/// handing one back early could not undo a wounding it took to get.
#[test]
fn read_many_keeps_the_locks_it_took_before_being_refused() {
    let locks = locks();
    let older = txn(&locks);
    let younger = txn(&locks);
    locks.buffer_write(older, b"b", Some(b"v".to_vec())).unwrap();

    assert_eq!(
        conflict(locks.read_many(younger, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()], true)),
        b"b".to_vec()
    );
    assert!(locks.is_locked(b"a"), "the key it got before the refusal is still held");
    assert!(!locks.is_locked(b"c"), "and it never reached the one after");
}

/// A key this transaction has already written is skipped: it holds the
/// stronger exclusive lock, and taking a shared one on top would be a
/// silent downgrade.
#[test]
fn read_many_leaves_a_key_this_transaction_already_wrote_alone() {
    let locks = locks();
    let t = txn(&locks);
    locks.buffer_write(t, b"k", Some(b"mine".to_vec())).unwrap();

    assert_eq!(
        locks.read_many(t, &[b"k".to_vec()], true),
        Ok(vec![Some(Some(b"mine".to_vec()))])
    );

    // Still exclusive: another transaction cannot read it.
    let other = txn(&locks);
    assert_eq!(conflict(read(&locks, other, b"k")), b"k".to_vec());
}

/// Positional, with buffered values only when asked for: a read that
/// wants versions alone gets no clone of a buffered value it would only
/// throw away -- and still takes every lock.
#[test]
fn read_many_answers_by_position_and_clones_only_when_asked() {
    let locks = locks();
    let t = txn(&locks);
    locks.buffer_write(t, b"w", Some(b"mine".to_vec())).unwrap();
    locks.buffer_write(t, b"d", None).unwrap();
    let keys = [b"a".to_vec(), b"w".to_vec(), b"d".to_vec(), b"a".to_vec()];

    assert_eq!(
        locks.read_many(t, &keys, true),
        Ok(vec![None, Some(Some(b"mine".to_vec())), Some(None), None])
    );
    assert_eq!(locks.read_many(t, &keys, false), Ok(vec![None; 4]));
    assert!(locks.is_locked(b"a"));
}

/// What a scan needs to fold in its own uncommitted changes: the
/// buffered keys from a point on, in order, each marked as a write or a
/// delete.
#[test]
fn buffered_from_reports_the_buffered_keys_in_order() {
    let locks = locks();
    let t = txn(&locks);
    locks.buffer_write(t, b"c", Some(b"3".to_vec())).unwrap();
    locks.buffer_write(t, b"a", Some(b"1".to_vec())).unwrap();
    locks.buffer_write(t, b"b", None).unwrap();

    assert_eq!(
        locks.buffered_from(t, b""),
        Ok(vec![
            (b"a".to_vec(), false),
            (b"b".to_vec(), true),
            (b"c".to_vec(), false),
        ])
    );
    assert_eq!(
        locks.buffered_from(t, b"b"),
        Ok(vec![(b"b".to_vec(), true), (b"c".to_vec(), false)]),
        "a scan starting mid-keyspace is not told about what is behind it"
    );
}
