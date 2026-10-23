//! The permission part of the client service: the RPCs that manage
//! groups and ACLs, and the checks the data path makes against them.
//!
//! `crate::perms` holds the rules and the records. This module holds the
//! policy around them: who may change what, the size caps, and how a node
//! keeps a usable copy of all permissions without reading the store for
//! every key.
//!
//! # The cache, and why it is safe
//!
//! [`KvService::permissions`] returns an immutable snapshot of every group
//! and ACL, rebuilt only when it changed. The change check is one seek:
//! every transaction that writes a group or an ACL also writes
//! [`crate::keyspace::EPOCH_KEY`], and that key's row seq is the epoch.
//! Reading it never touches a value, so it is a point lookup on a key that
//! is always in the block cache.
//!
//! The snapshot is built from locally applied state, which makes it
//! correct on a follower. A stale read is answered from local state, so
//! authorizing it from local state uses the same moment. A linearizable
//! read is answered by a caught-up leader, whose local state includes
//! everything committed. There is no leader round trip here, on purpose:
//! the node serving most reads has no leader to ask.
//!
//! # Administrators
//!
//! `is_admin` means write everywhere, and no ACL can remove it. An
//! administrator can rewrite any ACL anyway, so an ACL that excluded one
//! would add no safety, and would let one call lock the cluster out of its
//! own keyspace.
//!
//! The bypass is checked after the ACLs, not before, so only a request an
//! ACL would refuse pays for the account read. On the common path (a key
//! nothing governs), nothing is read.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tonic::Status;

use super::{AuthFailure, KvService, caller, storage_err};
use crate::auth::{AuthContext, UserRecord};
use crate::keyspace;
use crate::pb::raft5::client::{
    Acl as PbAcl, AclEntry as PbAclEntry, AclList, CreateGroupRequest, DeleteAclRequest,
    DeleteGroupRequest, GetAclOk, GetAclRequest, GetGroupRequest, Group as PbGroup, GroupList,
    Principal as PbPrincipal, Right as PbRight, SetAclRequest, UpdateGroupRequest, auth_error,
    principal,
};
use crate::perms::{AclRecord, Decision, GroupRecord, Permissions, Principal, Right};
use crate::storage::RaftLog;
use crate::txn::TxnId;

/// The most groups a cluster may hold.
///
/// Bounds the listing, like `MAX_LIVE_INVITES` (`ListGroups` reads every
/// record in one unpaged call), and bounds the snapshot that every node
/// keeps in memory.
pub(super) const MAX_GROUPS: usize = 1024;

/// The most groups one account may create.
///
/// Creating a group is not an administrator's privilege, so this stops one
/// account from taking the whole namespace. A quota, not a security
/// boundary: it is counted from the snapshot, not inside the transaction,
/// so two creations at once can put an account one over. Administrators
/// are not counted.
pub(super) const MAX_GROUPS_PER_CREATOR: usize = 32;

/// The most principals one group may contain, and the most that may
/// administer it. Members are bounded because every snapshot rebuild reads
/// the whole record; admins because nobody really reads a list that long.
pub(super) const MAX_GROUP_MEMBERS: usize = 256;
pub(super) const MAX_GROUP_ADMINS: usize = 32;

/// The most ACLs a cluster may hold, and the most grants one ACL may
/// carry. Both bound the snapshot. The first also bounds the worst case of
/// a longest-prefix lookup, which is one probe per distinct prefix length.
pub(super) const MAX_ACLS: usize = 1024;
pub(super) const MAX_ACL_ENTRIES: usize = 64;

/// How many keys one `GetKeys` examines before it stops trying to fill
/// its batch and returns a cursor.
///
/// Without it, a caller whose access is one narrow prefix of a large store
/// would make one call walk everything before that prefix: keys it may not
/// read are skipped like a transaction's buffered deletes, and a skipped
/// key does not count toward the batch. Large enough that a normal walk
/// never reaches it.
pub(super) const SCAN_ROW_BUDGET: usize = 8192;

/// How many times a snapshot rebuild restarts because the epoch moved
/// while it read.
///
/// Each attempt is two listings and two seeks. Running out means
/// permissions are being rewritten nonstop, which is not worth more
/// retries.
const PERMISSION_LOAD_ATTEMPTS: usize = 3;

/// The value written to the epoch key. Nobody reads it (the row's seq is
/// the whole signal), so it is as short as a value can be and still be a
/// write, not a delete.
const EPOCH_MARK: &[u8] = b"1";

/// This node's current view of every group and ACL.
///
/// One per node, shared by every clone tonic makes of the service. A
/// synchronous mutex never held across `.await`, as in `crate::txn::Locks`:
/// it only guards a pointer swap.
pub(super) type PermsCache = Arc<Mutex<Arc<Permissions>>>;

pub(super) fn new_cache() -> PermsCache {
    Arc::new(Mutex::new(Arc::new(Permissions::empty())))
}

/// What a scan uses to hide keys. The closure on the blocking pool holds
/// it.
///
/// Built once per call, not per key, and only when there is something to
/// hide: see [`KvService::scan_filter`].
#[derive(Clone)]
pub(super) struct ScanFilter {
    caller: String,
    perms: Arc<Permissions>,
}

impl ScanFilter {
    /// Whether this key must be left out of the batch.
    pub(super) fn hides(&self, key: &[u8]) -> bool {
        !self.perms.decide(&self.caller, key).permits(Right::Read)
    }
}

/// Read all permissions at one point in time.
///
/// The two listings are two iterators, so two moments. The epoch is read
/// before and after to detect a change in between. This matters only here:
/// a snapshot built across a change could hold half of it, which is
/// permission state nobody could reason about.
fn load_permissions(log: &RaftLog) -> Result<Permissions, Status> {
    for _ in 0..PERMISSION_LOAD_ATTEMPTS {
        let before = read_epoch(log)?;
        let groups = log
            .list_versioned_records_under(keyspace::GROUP_PREFIX)
            .map_err(storage_err)?;
        let acls = log
            .list_versioned_records_under(keyspace::ACL_PREFIX)
            .map_err(storage_err)?;
        if read_epoch(log)? == before {
            return Permissions::build(before, groups, acls);
        }
    }
    Err(Status::unavailable(
        "permissions are being changed faster than this node can read them",
    ))
}

fn read_epoch(log: &RaftLog) -> Result<u64, Status> {
    Ok(log
        .get_version(keyspace::EPOCH_KEY)
        .map_err(storage_err)?
        .unwrap_or(0))
}

/// A `Principal` from the wire.
fn principal_from_pb(pb: &PbPrincipal) -> Result<Principal, Status> {
    let principal = match &pb.kind {
        Some(principal::Kind::User(name)) => Principal::User(name.clone()),
        Some(principal::Kind::Group(name)) => Principal::Group(name.clone()),
        None => {
            return Err(Status::invalid_argument(
                "a principal must name either a user or a group",
            ));
        }
    };
    principal.validate()?;
    Ok(principal)
}

/// A list of principals, with duplicates refused: a principal named twice
/// in one request means the caller contradicted itself, and guessing which
/// half it meant is worse than saying so.
fn principals_from_pb(pb: &[PbPrincipal]) -> Result<Vec<Principal>, Status> {
    let mut out = Vec::with_capacity(pb.len());
    for one in pb {
        let principal = principal_from_pb(one)?;
        if out.contains(&principal) {
            return Err(Status::invalid_argument(format!(
                "{principal} is named twice in the same request"
            )));
        }
        out.push(principal);
    }
    Ok(out)
}

fn principal_to_pb(principal: &Principal) -> PbPrincipal {
    PbPrincipal {
        kind: Some(match principal {
            Principal::User(name) => principal::Kind::User(name.clone()),
            Principal::Group(name) => principal::Kind::Group(name.clone()),
        }),
    }
}

fn right_from_pb(right: i32) -> Result<Right, Status> {
    match PbRight::try_from(right) {
        Ok(PbRight::Read) => Ok(Right::Read),
        Ok(PbRight::Write) => Ok(Right::Write),
        // Including `RIGHT_UNSPECIFIED`, the value proto3 gives an unset field:
        // a grant with no right must be an error, not the lowest-numbered right.
        _ => Err(Status::invalid_argument(
            "an ACL entry must say `read` or `write`",
        )),
    }
}

fn right_to_pb(right: Right) -> i32 {
    match right {
        Right::Read => PbRight::Read as i32,
        Right::Write => PbRight::Write as i32,
    }
}

fn entries_from_pb(pb: &[PbAclEntry]) -> Result<Vec<(Principal, Right)>, Status> {
    let mut out: Vec<(Principal, Right)> = Vec::with_capacity(pb.len());
    for entry in pb {
        let principal = entry
            .principal
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("an ACL entry must name a principal"))?;
        let principal = principal_from_pb(principal)?;
        if out.iter().any(|(existing, _)| *existing == principal) {
            return Err(Status::invalid_argument(format!(
                "{principal} is granted twice in the same ACL"
            )));
        }
        out.push((principal, right_from_pb(entry.right)?));
    }
    Ok(out)
}

fn acl_to_pb(prefix: &[u8], acl: &AclRecord, seq: u64) -> PbAcl {
    PbAcl {
        prefix: prefix.to_vec(),
        entries: acl
            .entries
            .iter()
            .map(|(principal, right)| PbAclEntry {
                principal: Some(principal_to_pb(principal)),
                right: right_to_pb(*right),
            })
            .collect(),
        seq,
    }
}

fn group_to_pb(name: &str, group: &GroupRecord, seq: u64) -> PbGroup {
    PbGroup {
        name: name.to_string(),
        created_by: group.created_by.clone(),
        admins: group.admins.iter().map(principal_to_pb).collect(),
        members: group.members.iter().map(principal_to_pb).collect(),
        seq,
    }
}

fn not_permitted(detail: impl Into<String>) -> AuthFailure {
    AuthFailure::refused(auth_error::Kind::NotPermitted, detail)
}

fn no_such_group(name: &str) -> AuthFailure {
    AuthFailure::refused(
        auth_error::Kind::NoSuchGroup,
        format!("there is no group called {name}"),
    )
}

/// Apply an `UpdateGroup`'s additions, then its removals.
///
/// That order is the contract: a principal in both lists ends up removed,
/// so "exactly these" is one call.
fn apply_changes(list: &mut Vec<Principal>, add: &[Principal], remove: &[Principal]) {
    for principal in add {
        if !list.contains(principal) {
            list.push(principal.clone());
        }
    }
    list.retain(|principal| !remove.contains(principal));
}

impl KvService {
    // -- the snapshot -------------------------------------------------

    /// This node's permissions, as of everything it has applied.
    ///
    /// One seek on the common path: the epoch has not changed, and the cached
    /// snapshot is returned as is.
    pub(super) async fn permissions(&self) -> Result<Arc<Permissions>, Status> {
        // In its own block: this is a synchronous mutex, and such a lock must
        // never be held across `.await`. A temporary would be dropped before the
        // next line anyway, but that is not something to rely on.
        let cached = {
            let cache = self.perms.lock().expect("permission cache poisoned");
            Arc::clone(&cache)
        };
        let epoch = self.store.run(read_epoch).await?;
        if epoch == cached.epoch() {
            return Ok(cached);
        }

        let rebuilt = Arc::new(self.store.run(load_permissions).await?);
        {
            // Never install a view older than the current one. Two requests can find
            // the cache stale at once, and the one that finishes second may not be
            // the one that read later.
            let mut cache = self.perms.lock().expect("permission cache poisoned");
            if cache.epoch() < rebuilt.epoch() {
                *cache = Arc::clone(&rebuilt);
            }
        }
        Ok(rebuilt)
    }

    // -- the checks ---------------------------------------------------

    /// Check that the sender of this request may do `wanted` to `key`.
    ///
    /// The open cluster is decided here, once, as in `require_admin`: a node
    /// with no secret has no accounts, so no ACL can be about anyone.
    pub(super) async fn require_access(
        &self,
        extensions: &tonic::Extensions,
        key: &[u8],
        wanted: Right,
    ) -> Result<(), AuthFailure> {
        self.require_access_all(extensions, &[key], wanted).await
    }

    /// `require_access` for all of `keys`. Refuses if any key is refused, and
    /// names the first such key by position.
    ///
    /// The permissions are loaded once and the caller resolved once, however
    /// many keys there are. The admin check (a store read) runs at most once,
    /// and only if some key needs it.
    pub(super) async fn require_access_all(
        &self,
        extensions: &tonic::Extensions,
        keys: &[impl AsRef<[u8]>],
        wanted: Right,
    ) -> Result<(), AuthFailure> {
        if self.auth.is_none() {
            return Ok(());
        }
        let perms = self.permissions().await?;
        // A cluster with no ACLs pays one seek and nothing else, not even the
        // cost of finding out who is calling.
        if !perms.has_acls() {
            return Ok(());
        }
        let who = caller(extensions)?;
        let Some(denied) = keys
            .iter()
            .position(|key| !perms.decide(&who, key.as_ref()).permits(wanted))
        else {
            return Ok(());
        };
        if self.is_admin_locally(&who).await? {
            return Ok(());
        }
        Err(not_permitted(if keys.len() == 1 {
            format!("this account may not {wanted} that key")
        } else {
            format!("this account may not {wanted} keys[{denied}]")
        }))
    }

    /// What a scan should hide, or `None` if nothing.
    ///
    /// Decided before the walk, not during it, because the walk runs on the
    /// blocking pool and cannot read the store. That is why the administrator
    /// bypass is checked eagerly here, while `require_access` checks it
    /// lazily: there is no later chance.
    pub(super) async fn scan_filter(
        &self,
        extensions: &tonic::Extensions,
    ) -> Result<Option<ScanFilter>, Status> {
        if self.auth.is_none() {
            return Ok(None);
        }
        let perms = self.permissions().await?;
        if !perms.has_acls() {
            return Ok(None);
        }
        // Not `caller`, which reports through the in-band channel: a scan is
        // never refused because of an ACL, so this fails only if the auth layer
        // did not run, which is a configuration error, not an answer.
        let Some(context) = extensions.get::<AuthContext>() else {
            return Err(Status::unauthenticated(
                "this request carried no verified identity",
            ));
        };
        let who = context.username.clone();
        if self.is_admin_locally(&who).await? {
            return Ok(None);
        }
        Ok(Some(ScanFilter { caller: who, perms }))
    }

    /// Whether this account is an administrator, by what this node has
    /// applied.
    ///
    /// A local read, unlike the linearizable read in `require_admin`, on
    /// purpose: this is the data path, which a follower serves with no leader
    /// to ask. It is also the state the read itself is answered from, so a
    /// stale read is authorized as of the moment it describes.
    async fn is_admin_locally(&self, username: &str) -> Result<bool, Status> {
        let key = keyspace::user_key(username);
        let found = self
            .store
            .run(move |log| log.get_latest(&key))
            .await
            .map_err(storage_err)?;
        match found {
            Some((_, bytes)) => Ok(UserRecord::from_bytes(&bytes)?.is_admin),
            None => Ok(false),
        }
    }

    /// Whether this request comes from an administrator, read as
    /// `require_admin` reads it.
    ///
    /// For the management RPCs. They run on the leader and already pay a raft
    /// round trip, so they can afford the authoritative answer that the data
    /// path cannot.
    async fn is_cluster_admin(&self, extensions: &tonic::Extensions) -> Result<bool, AuthFailure> {
        match self.require_admin(extensions).await {
            Ok(()) => Ok(true),
            Err(AuthFailure::Refused(auth_error::Kind::NotAdmin, _)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// The "auth enabled, or refuse" check that every account RPC starts with.
    fn require_auth_enabled(&self) -> Result<(), AuthFailure> {
        if self.auth.is_none() {
            return Err(AuthFailure::disabled());
        }
        Ok(())
    }

    // -- writing -------------------------------------------------------

    /// The permission snapshot, pinned for the rest of this transaction.
    ///
    /// Every permission change writes the epoch key, so reading it here (which
    /// takes a read lock on it) stops a change from landing between a decision
    /// and the write that the decision allows. Without it there is a harmful
    /// window: an account whose write grant is being revoked could use its last
    /// moment to write an ACL that grants the access back, and that ACL would
    /// outlive the revocation.
    ///
    /// It also serializes permission changes against each other, because they
    /// all contend for this one key. That is the right trade-off: they are
    /// rare, and two that race resolve like any two transactions.
    ///
    /// Once the lock is held, the epoch comparison should never fail (nothing
    /// can commit a change meanwhile), so a mismatch means a change landed
    /// between the lock and the read. The caller may retry; this does not hide
    /// it.
    async fn locked_permissions(&self, id: TxnId) -> Result<Arc<Permissions>, AuthFailure> {
        let locked = self.transactional_read(id, keyspace::EPOCH_KEY).await?.seq;
        let perms = self.permissions().await?;
        if perms.epoch() != locked {
            return Err(Status::unavailable(
                "a permission change landed while this one was being decided; try again",
            )
            .into());
        }
        Ok(perms)
    }

    /// Commit a permission change: the record and the epoch bump, in one
    /// transaction.
    ///
    /// The two belong together. A record without the bump is one no node would
    /// notice, and a bump without the record is a cache rebuild for nothing.
    /// They land at one seq, so a node that sees the bump also sees the change.
    async fn commit_permission_change(
        &self,
        id: TxnId,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) -> Result<u64, AuthFailure> {
        self.locks.buffer_write(id, key, value)?;
        self.locks
            .buffer_write(id, keyspace::EPOCH_KEY, Some(EPOCH_MARK.to_vec()))?;
        Ok(self.commit(id).await?)
    }

    /// Whether putting `members` into the group `name` would make the group
    /// reachable from itself.
    ///
    /// Reads every group it walks inside the transaction, so each stays
    /// read-locked until the transaction ends. That makes the answer hold: two
    /// administrators that each add one half of a cycle contend on the groups
    /// between them, and one transaction aborts, instead of both seeing a graph
    /// with no loop.
    ///
    /// A group that does not exist is walked over, not refused: naming one is
    /// allowed, and a name that resolves to nothing contains nothing.
    async fn would_create_a_cycle(
        &self,
        id: TxnId,
        name: &str,
        members: &[Principal],
    ) -> Result<bool, AuthFailure> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = members
            .iter()
            .filter_map(|principal| match principal {
                Principal::Group(nested) => Some(nested.clone()),
                Principal::User(_) => None,
            })
            .collect();

        while let Some(nested) = frontier.pop() {
            if nested == name {
                return Ok(true);
            }
            if !seen.insert(nested.clone()) {
                continue;
            }
            if seen.len() > MAX_GROUPS {
                return Err(Status::resource_exhausted(
                    "this group nests more deeply than the cluster can check",
                )
                .into());
            }
            let key = keyspace::group_key(&nested);
            let Some(bytes) = self.transactional_read(id, &key).await?.value else {
                continue;
            };
            for principal in GroupRecord::from_bytes(&bytes)?.members {
                if let Principal::Group(deeper) = principal {
                    frontier.push(deeper);
                }
            }
        }
        Ok(false)
    }

    /// The group `name`, read inside the transaction so it stays held until
    /// the transaction ends.
    async fn locked_group(&self, id: TxnId, name: &str) -> Result<GroupRecord, AuthFailure> {
        let key = keyspace::group_key(name);
        match self.transactional_read(id, &key).await?.value {
            Some(bytes) => Ok(GroupRecord::from_bytes(&bytes)?),
            None => Err(no_such_group(name)),
        }
    }

    /// Check that the caller may change this group: one of its own
    /// administrators, or a cluster administrator.
    async fn require_group_admin(
        &self,
        extensions: &tonic::Extensions,
        id: TxnId,
        who: &str,
        name: &str,
        group: &GroupRecord,
    ) -> Result<(), AuthFailure> {
        // `locked_permissions`, not `permissions`: `locked_group` already holds
        // `group.admins`, but expanding a nested group among them does not, and a
        // group's membership decides what its members can reach.
        let perms = self.locked_permissions(id).await?;
        if perms.is_any(who, &group.admins) {
            return Ok(());
        }
        if self.is_cluster_admin(extensions).await? {
            return Ok(());
        }
        Err(not_permitted(format!(
            "this account does not administer the group {name}"
        )))
    }

    // -- groups --------------------------------------------------------

    pub(super) async fn create_group_inner(
        &self,
        extensions: &tonic::Extensions,
        req: CreateGroupRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_group_name(&req.name)?;
        let members = principals_from_pb(&req.members)?;
        let mut admins = principals_from_pb(&req.admins)?;
        let who = caller(extensions)?;

        // The creator always administers what they create, whatever the request
        // says, so a group never starts with no administrator (which would leave
        // only a cluster administrator able to change it).
        let creator = Principal::User(who.clone());
        if !admins.contains(&creator) {
            admins.push(creator);
        }
        check_group_size(&admins, &members)?;

        let perms = self.permissions().await?;
        if perms.group_count() >= MAX_GROUPS {
            return Err(Status::resource_exhausted(format!(
                "this cluster already holds {MAX_GROUPS} groups"
            ))
            .into());
        }
        if perms.groups_created_by(&who) >= MAX_GROUPS_PER_CREATOR
            && !self.is_cluster_admin(extensions).await?
        {
            return Err(Status::resource_exhausted(format!(
                "an account may create at most {MAX_GROUPS_PER_CREATOR} groups"
            ))
            .into());
        }

        let record = GroupRecord::new(who, admins, members.clone());
        let key = keyspace::group_key(&req.name);
        self.admin_txn(async |id| {
            if self.transactional_read(id, &key).await?.value.is_some() {
                return Err(AuthFailure::refused(
                    auth_error::Kind::AlreadyExists,
                    "that group already exists",
                ));
            }
            if self.would_create_a_cycle(id, &req.name, &members).await? {
                return Err(
                    Status::invalid_argument("that would put the group inside itself").into(),
                );
            }
            self.commit_permission_change(id, &key, Some(record.to_bytes()?))
                .await
        })
        .await
    }

    pub(super) async fn delete_group_inner(
        &self,
        extensions: &tonic::Extensions,
        req: DeleteGroupRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_group_name(&req.name)?;
        let who = caller(extensions)?;
        let key = keyspace::group_key(&req.name);

        self.admin_txn(async |id| {
            let group = self.locked_group(id, &req.name).await?;
            self.require_group_admin(extensions, id, &who, &req.name, &group)
                .await?;
            // ACLs that name it are left alone: a grant to a group that no longer
            // exists matches nobody, which is how a dangling reference should fail.
            // Rewriting every ACL that names it would be far more work for the same
            // result.
            self.commit_permission_change(id, &key, None).await
        })
        .await
    }

    pub(super) async fn update_group_inner(
        &self,
        extensions: &tonic::Extensions,
        req: UpdateGroupRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_group_name(&req.name)?;
        let add_members = principals_from_pb(&req.add_members)?;
        let remove_members = principals_from_pb(&req.remove_members)?;
        let add_admins = principals_from_pb(&req.add_admins)?;
        let remove_admins = principals_from_pb(&req.remove_admins)?;
        let who = caller(extensions)?;
        let key = keyspace::group_key(&req.name);

        self.admin_txn(async |id| {
            let mut group = self.locked_group(id, &req.name).await?;
            self.require_group_admin(extensions, id, &who, &req.name, &group)
                .await?;

            apply_changes(&mut group.members, &add_members, &remove_members);
            apply_changes(&mut group.admins, &add_admins, &remove_admins);
            check_group_size(&group.admins, &group.members)?;

            // A group with no administrators could never change again except by a
            // cluster administrator, so the call that would create one is refused,
            // instead of someone finding out later.
            if group.admins.is_empty() {
                return Err(Status::invalid_argument(
                    "a group must always have at least one administrator",
                )
                .into());
            }
            if !add_members.is_empty()
                && self
                    .would_create_a_cycle(id, &req.name, &group.members)
                    .await?
            {
                return Err(
                    Status::invalid_argument("that would put the group inside itself").into(),
                );
            }

            self.commit_permission_change(id, &key, Some(group.to_bytes()?))
                .await
        })
        .await
    }

    pub(super) async fn get_group_inner(
        &self,
        extensions: &tonic::Extensions,
        req: GetGroupRequest,
    ) -> Result<PbGroup, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_group_name(&req.name)?;
        caller(extensions)?;
        let perms = self.permissions().await?;
        match perms.group(&req.name) {
            Some(group) => Ok(group_to_pb(&req.name, group, perms.group_seq(&req.name))),
            None => Err(no_such_group(&req.name)),
        }
    }

    pub(super) async fn list_groups_inner(
        &self,
        extensions: &tonic::Extensions,
    ) -> Result<GroupList, AuthFailure> {
        self.require_auth_enabled()?;
        caller(extensions)?;
        let perms = self.permissions().await?;
        let mut groups: Vec<PbGroup> = perms
            .groups()
            .map(|(name, group)| group_to_pb(name, group, perms.group_seq(name)))
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(GroupList { groups })
    }

    // -- ACLs ----------------------------------------------------------

    pub(super) async fn set_acl_inner(
        &self,
        extensions: &tonic::Extensions,
        req: SetAclRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_acl_prefix(&req.prefix)?;
        let entries = entries_from_pb(&req.entries)?;
        if entries.len() > MAX_ACL_ENTRIES {
            return Err(Status::invalid_argument(format!(
                "an ACL may hold at most {MAX_ACL_ENTRIES} grants"
            ))
            .into());
        }
        // An ACL with no entries would deny everyone, including its writer, and
        // the caller more likely meant to remove it. `DeleteAcl` does that, and
        // says so.
        if entries.is_empty() {
            return Err(Status::invalid_argument(
                "an ACL with no grants denies everyone; use DeleteAcl to remove one",
            )
            .into());
        }

        let who = caller(extensions)?;
        let record = AclRecord::new(entries);
        let key = keyspace::acl_key(&req.prefix);

        self.admin_txn(async |id| {
            // Inside the transaction, against a pinned snapshot: authority over an
            // ACL is itself decided by the ACLs, and deciding it from a view that can
            // still move is how a revoked grant writes itself back in.
            let perms = self.locked_permissions(id).await?;
            let authority = perms.decide(&who, &req.prefix);
            self.require_acl_authority(extensions, &perms, &req.prefix, authority)
                .await?;

            if perms.acl_at(&req.prefix).is_none() && perms.acl_count() >= MAX_ACLS {
                return Err(Status::resource_exhausted(format!(
                    "this cluster already holds {MAX_ACLS} ACLs"
                ))
                .into());
            }

            self.commit_permission_change(id, &key, Some(record.to_bytes()?))
                .await
        })
        .await
    }

    pub(super) async fn delete_acl_inner(
        &self,
        extensions: &tonic::Extensions,
        req: DeleteAclRequest,
    ) -> Result<u64, AuthFailure> {
        self.require_auth_enabled()?;
        keyspace::validate_acl_prefix(&req.prefix)?;
        let who = caller(extensions)?;
        let key = keyspace::acl_key(&req.prefix);

        self.admin_txn(async |id| {
            let perms = self.locked_permissions(id).await?;

            // What would govern this prefix once its own ACL is gone: the longest ACL
            // on a strictly shorter prefix. If there is none, the removal opens the
            // subtree to everyone. That is the default, and only an administrator may
            // grant the default, for the reason given in `Decision::Default`.
            let falls_back_to = match req.prefix.len() {
                0 => None,
                len => perms.governing_within(&req.prefix, len - 1),
            };
            let authority = match falls_back_to {
                Some(_) => perms.decide(&who, &req.prefix),
                None => Decision::Default,
            };
            self.require_acl_authority(extensions, &perms, &req.prefix, authority)
                .await?;

            if self.transactional_read(id, &key).await?.value.is_none() {
                return Err(AuthFailure::refused(
                    auth_error::Kind::NoSuchAcl,
                    "there is no ACL on that prefix",
                ));
            }
            self.commit_permission_change(id, &key, None).await
        })
        .await
    }

    /// The rule for every change to an ACL.
    ///
    /// `authority` is what decides the prefix; for a removal, what would
    /// decide it afterwards. `Granted(Write)` is authority. `Default` is
    /// explicitly not, which stops any account from locking down a keyspace
    /// nobody has protected yet, or unprotecting one someone has. A cluster
    /// administrator may always: they could rewrite the ACL that governs the
    /// governing ACL, so refusing them would only add indirection.
    async fn require_acl_authority(
        &self,
        extensions: &tonic::Extensions,
        perms: &Permissions,
        prefix: &[u8],
        authority: Decision,
    ) -> Result<(), AuthFailure> {
        if authority.permits_acl_change() || self.is_cluster_admin(extensions).await? {
            return Ok(());
        }
        Err(not_permitted(match authority {
            Decision::Default => {
                "no ACL governs that prefix, so only an administrator may change one there"
                    .to_string()
            }
            _ => {
                let governed_by =
                    perms
                        .governing(prefix)
                        .map_or_else(String::new, |(prefix, _)| {
                            format!(
                                " (governed by the ACL on {})",
                                String::from_utf8_lossy(prefix)
                            )
                        });
                format!("this account does not hold write on that prefix{governed_by}")
            }
        }))
    }

    pub(super) async fn get_acl_inner(
        &self,
        extensions: &tonic::Extensions,
        req: GetAclRequest,
    ) -> Result<GetAclOk, AuthFailure> {
        self.require_auth_enabled()?;
        let who = caller(extensions)?;
        let perms = self.permissions().await?;
        let Some((prefix, acl)) = perms.governing(&req.key) else {
            return Ok(GetAclOk {
                acl: None,
                effective: None,
            });
        };
        let effective = match perms.decide_within(&who, acl) {
            Decision::Granted(right) => Some(right_to_pb(right)),
            // Denied by the ACL. `Default` cannot occur here, because an ACL was
            // found. An administrator sees what the ACL says about them, not their
            // bypass, because the question is about the ACL.
            _ => None,
        };
        Ok(GetAclOk {
            acl: Some(acl_to_pb(prefix, acl, perms.acl_seq(prefix))),
            effective,
        })
    }

    pub(super) async fn list_acls_inner(
        &self,
        extensions: &tonic::Extensions,
    ) -> Result<AclList, AuthFailure> {
        self.require_auth_enabled()?;
        caller(extensions)?;
        let perms = self.permissions().await?;
        let mut acls: Vec<PbAcl> = perms
            .acls()
            .map(|(prefix, acl)| acl_to_pb(prefix, acl, perms.acl_seq(prefix)))
            .collect();
        // Shortest first where one is a prefix of another: the order they take
        // effect, least specific to most.
        acls.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        Ok(AclList { acls })
    }
}

/// The two size caps, checked together because every path that changes one
/// of these lists can change the other.
fn check_group_size(admins: &[Principal], members: &[Principal]) -> Result<(), Status> {
    if members.len() > MAX_GROUP_MEMBERS {
        return Err(Status::invalid_argument(format!(
            "a group may hold at most {MAX_GROUP_MEMBERS} members"
        )));
    }
    if admins.len() > MAX_GROUP_ADMINS {
        return Err(Status::invalid_argument(format!(
            "a group may have at most {MAX_GROUP_ADMINS} administrators"
        )));
    }
    Ok(())
}
