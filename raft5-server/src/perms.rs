//! Who may read and write which keys.
//!
//! `auth` answers *who is calling*; `require_admin` answers the one
//! question about them the cluster used to be able to ask. This module
//! is the rest of it: named collections of principals (groups), and
//! grants of `read` or `write` on a key prefix to those principals
//! (ACLs).
//!
//! # The rules
//!
//! A key is governed by the ACL on the **longest key prefix that has
//! one**. Not the longest that mentions the caller -- the longest that
//! exists. An ACL on `app/` that names only `alice` denies `bob`
//! `app/config` outright; it does not fall back to whatever `app` or the
//! empty prefix might have said. That is what makes an ACL a statement
//! about a subtree rather than one more voice in a vote, and it is the
//! only rule under which "revoke bob's access to this subtree" is a
//! thing one can actually do.
//!
//! `write` subsumes `read`, and adds creating, deleting, and authority
//! over the ACL of that prefix and of every longer one.
//!
//! A key that no ACL governs is **open**, exactly as every key was
//! before this module existed. ACLs restrict; they do not enable. A
//! cluster with none behaves as it always did, and one that wants the
//! opposite posture writes an ACL on the empty prefix -- which governs
//! every key there is. The distinction between "open because nothing
//! decided" and "granted because something did" is carried in
//! [`Decision`] rather than flattened, because the two are not the same
//! authority: see `Decision::Default`.
//!
//! # Why this is a snapshot
//!
//! These checks sit on the data path -- every `Read`, every `Write`,
//! every key a scan considers. The account check they sit beside
//! (`require_admin`) reads the store on every call, which it can afford
//! because it guards a handful of rare RPCs that already pay a raft
//! round trip. This cannot: a linearizable read per key scanned would be
//! absurd, and on a follower answering a stale read there is no leader
//! to do one against.
//!
//! So the whole of it -- every group, every ACL -- is loaded into one
//! immutable [`Permissions`] and kept until it changes. It is small by
//! construction (the caps in `grpc` see to that), it is derived entirely
//! from applied state, and a node answers from the same state it serves
//! reads from, which is exactly the consistency each kind of read is
//! already promising.
//!
//! Knowing *when* it changed is the part that has to be cheap, and that
//! is [`crate::keyspace::EPOCH_KEY`]: one key, written by every
//! transaction that touches a group or an ACL, whose row seq is the
//! epoch. Asking for it is a seek that never reads a value.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tonic::Status;

/// The version stamped into every record this build writes. Separate
/// from `auth`'s constant of the same name: these records version
/// independently of account records, and tying them together would mean
/// a change to one forcing a rewrite of the other.
const RECORD_VERSION: u8 = 1;

/// How far a walk up the group graph will go.
///
/// A bound rather than a cycle check, because this is the *read* side:
/// cycles are refused when an edge is added, and a walk that meets one
/// anyway must still terminate rather than trust that they were. Deep
/// enough that no real nesting reaches it.
pub const MAX_GROUP_DEPTH: usize = 16;

/// Something an ACL or a group can name.
///
/// Variants are appended, never reordered: postcard encodes an enum as
/// its positional discriminant, so their order is the on-disk format.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Principal {
    User(String),
    Group(String),
}

impl Principal {
    /// Checks that this names something that could exist -- that is, a
    /// name the key builder for its kind is sound for.
    ///
    /// Deliberately not a check that it *does* exist. An ACL naming a
    /// group that has not been created yet, or a user since deleted, is
    /// a grant that matches nobody, which is the safe way for a dangling
    /// reference to fail. Requiring existence would instead mean every
    /// account deletion had to rewrite every ACL that mentioned it.
    pub fn validate(&self) -> Result<(), Status> {
        match self {
            Self::User(name) => crate::keyspace::validate_username(name),
            Self::Group(name) => crate::keyspace::validate_group_name(name),
        }
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User(name) => write!(f, "user:{name}"),
            Self::Group(name) => write!(f, "group:{name}"),
        }
    }
}

/// What a principal may do with the keys an ACL governs.
///
/// Ordered, and the order is the point: `Write` satisfies a requirement
/// for `Read`, so every check in this crate is `right >= wanted` and
/// there is no second place where "write implies read" has to be
/// remembered. Variants are appended, never reordered -- see
/// [`Principal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Right {
    /// Read the keys under this prefix, and nothing else.
    Read,
    /// Read, write, create and delete them -- and set the ACL of this
    /// prefix and of any longer one.
    Write,
}

impl std::fmt::Display for Right {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
        })
    }
}

/// One group, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
    /// First, for the reason `auth::UserRecord::version` is first.
    pub version: u8,
    /// Who created it. Kept so a listing can say where a group came
    /// from, and so the per-creator quota has something to count.
    pub created_by: String,
    /// Who may change it: add and remove members, add and remove other
    /// admins, and delete it. A group, so that "the on-call team
    /// administers its own membership" is expressible.
    ///
    /// Separate from `members`, and not implicitly part of it.
    /// Administering a group is deciding who is in it; being in it is
    /// holding whatever it has been granted. Folding the two together
    /// would mean every group's administrator silently held everything
    /// that group was ever granted -- including grants written long
    /// after they were made one, by someone who never considered them.
    /// An administrator who wants the group's access adds themselves to
    /// it, which they may, and which is then visible in the record.
    pub admins: Vec<Principal>,
    /// Who is in it. A group here is a nested group: everything in it is
    /// in this one too, transitively.
    pub members: Vec<Principal>,
}

impl GroupRecord {
    pub fn new(created_by: String, admins: Vec<Principal>, members: Vec<Principal>) -> Self {
        Self {
            version: RECORD_VERSION,
            created_by,
            admins,
            members,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Status> {
        postcard::to_allocvec(self)
            .map_err(|e| Status::internal(format!("could not encode a group: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let record: Self = postcard::from_bytes(bytes)
            .map_err(|_| Status::internal("a stored group could not be decoded"))?;
        if record.version != RECORD_VERSION {
            return Err(Status::internal(format!(
                "a stored group is version {}, and this node understands {RECORD_VERSION}",
                record.version
            )));
        }
        Ok(record)
    }
}

/// One access control list, as stored: everything granted on the key
/// prefix its own key encodes.
///
/// The prefix is not a field. It is in the key -- `keyspace::acl_key`
/// builds it and `acl_prefix_from_key_suffix` reads it back -- for the
/// same reason a checkpoint's seq is not in its record: a record that
/// carried it could disagree with where it was stored, and there would
/// be no way to say which was right.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRecord {
    /// First, for the reason `auth::UserRecord::version` is first.
    pub version: u8,
    /// Who is granted what. A principal appearing more than once gets
    /// the highest right it was given, which is what `right_for` takes;
    /// duplicates are refused when one is set, so that is a statement
    /// about decoding an old record rather than about the API.
    pub entries: Vec<(Principal, Right)>,
}

impl AclRecord {
    pub fn new(entries: Vec<(Principal, Right)>) -> Self {
        Self {
            version: RECORD_VERSION,
            entries,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Status> {
        postcard::to_allocvec(self)
            .map_err(|e| Status::internal(format!("could not encode an ACL: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Status> {
        let record: Self = postcard::from_bytes(bytes)
            .map_err(|_| Status::internal("a stored ACL could not be decoded"))?;
        if record.version != RECORD_VERSION {
            return Err(Status::internal(format!(
                "a stored ACL is version {}, and this node understands {RECORD_VERSION}",
                record.version
            )));
        }
        Ok(record)
    }
}

/// What the ACLs say about one caller and one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// No ACL governs this key. Allowed -- and allowed for everyone, so
    /// this is not a grant anybody holds. It confers no authority over
    /// the ACLs themselves: the rule that an ACL may only be written
    /// where one already decides, or by an administrator, is what stops
    /// any account locking down a keyspace nobody had got round to
    /// protecting (or unprotecting one somebody had).
    Default,
    /// An ACL governs this key and grants the caller this much.
    Granted(Right),
    /// An ACL governs this key and grants the caller nothing.
    Denied,
}

impl Decision {
    /// Whether this permits `wanted`.
    ///
    /// `Default` permits both rights, because a key nothing governs is
    /// as open as it was before this module existed.
    pub fn permits(self, wanted: Right) -> bool {
        match self {
            Self::Default => true,
            Self::Granted(held) => held >= wanted,
            Self::Denied => false,
        }
    }

    /// Whether this is authority over an ACL, which `Default` is not.
    pub fn permits_acl_change(self) -> bool {
        matches!(self, Self::Granted(Right::Write))
    }
}

/// Every group and every ACL in the cluster, as of one permission epoch.
///
/// Immutable once built -- the memo below is the single exception, and
/// it only ever caches a function of what is already here. Shared behind
/// an `Arc` so that a request, or a scan's skip closure on the blocking
/// pool, holds one point in time for as long as it needs it, whatever
/// replaces it meanwhile.
#[derive(Debug)]
pub struct Permissions {
    /// The seq of [`crate::keyspace::EPOCH_KEY`] when this was built.
    /// `0` means no permission record has ever been written, which is
    /// the state every existing cluster upgrades into.
    epoch: u64,
    /// Keyed by the key prefix each governs.
    acls: HashMap<Vec<u8>, AclRecord>,
    /// The seq each of those was last written at. Kept beside the
    /// records rather than in them, for the reason the record itself
    /// does not carry its prefix: a seq is a fact about where a record
    /// is stored, and a copy inside it could disagree.
    acl_seqs: HashMap<Vec<u8>, u64>,
    /// The distinct lengths of those prefixes, descending. Longest-match
    /// walks this rather than the keyspace, so a lookup costs one hash
    /// probe per length that actually exists -- typically two or three,
    /// however deep the key.
    acl_lengths: Vec<usize>,
    groups: HashMap<String, GroupRecord>,
    /// The seq each group was last written at. See
    /// [`Permissions::acl_seqs`].
    group_seqs: HashMap<String, u64>,
    /// The membership edges, reversed: each principal to the groups
    /// that directly contain it.
    ///
    /// A group record lists who is *in* it, which is the wrong
    /// direction for the only question anybody asks -- "which groups is
    /// this caller in?" -- and answering it forwards means sweeping
    /// every group on every miss. Built once here, it makes that a walk
    /// over the edges that actually exist.
    ///
    /// Users and groups share the map because a `Principal` is the
    /// thing an edge points at, and keeping them apart would mean two
    /// lookups at every step of a walk that does not care which it has.
    parents: HashMap<Principal, Vec<String>>,
    /// Per-user transitive group membership, computed on first use.
    ///
    /// The walk is cheap but not free, and a busy caller does it on
    /// every request; the answer cannot change without the epoch
    /// changing, which is what makes caching it here sound. A plain
    /// `Mutex<HashMap>` and no eviction: it holds one entry per caller
    /// seen since the last permission change, bounded by the accounts
    /// that exist, and is dropped whole when the epoch moves.
    closures: Mutex<HashMap<String, Arc<HashSet<String>>>>,
}

impl Permissions {
    /// Builds a snapshot from the records a listing of each range
    /// returned, decoding as it goes.
    ///
    /// A record that will not decode fails the build rather than being
    /// skipped. A skipped group is one whose members silently lose
    /// access and a skipped ACL is one whose subtree silently opens; of
    /// the two ways to be wrong about a permission, refusing to serve is
    /// the one that can be noticed and fixed.
    pub fn build(
        epoch: u64,
        groups: Vec<(Vec<u8>, u64, Vec<u8>)>,
        acls: Vec<(Vec<u8>, u64, Vec<u8>)>,
    ) -> Result<Self, Status> {
        let mut decoded_groups = HashMap::with_capacity(groups.len());
        let mut group_seqs = HashMap::with_capacity(groups.len());
        for (name, seq, bytes) in groups {
            let name = String::from_utf8(name)
                .map_err(|_| Status::internal("a stored group's name is not valid UTF-8"))?;
            decoded_groups.insert(name.clone(), GroupRecord::from_bytes(&bytes)?);
            group_seqs.insert(name, seq);
        }

        let mut decoded_acls: HashMap<Vec<u8>, AclRecord> = HashMap::with_capacity(acls.len());
        let mut acl_seqs = HashMap::with_capacity(acls.len());
        for (suffix, seq, bytes) in acls {
            let prefix = crate::keyspace::acl_prefix_from_key_suffix(&suffix)
                .ok_or_else(|| Status::internal("a stored ACL's key is not one this node wrote"))?;
            decoded_acls.insert(prefix.clone(), AclRecord::from_bytes(&bytes)?);
            acl_seqs.insert(prefix, seq);
        }

        let mut acl_lengths: Vec<usize> = decoded_acls.keys().map(Vec::len).collect();
        acl_lengths.sort_unstable_by(|a, b| b.cmp(a));
        acl_lengths.dedup();

        let mut parents: HashMap<Principal, Vec<String>> = HashMap::new();
        for (name, group) in &decoded_groups {
            for member in &group.members {
                parents
                    .entry(member.clone())
                    .or_default()
                    .push(name.clone());
            }
        }

        Ok(Self {
            epoch,
            acls: decoded_acls,
            acl_seqs,
            acl_lengths,
            groups: decoded_groups,
            group_seqs,
            parents,
            closures: Mutex::new(HashMap::new()),
        })
    }

    /// An empty snapshot, for a cluster that has never written a
    /// permission record and for a node that has not looked yet.
    pub fn empty() -> Self {
        Self {
            epoch: 0,
            acls: HashMap::new(),
            acl_seqs: HashMap::new(),
            acl_lengths: Vec::new(),
            groups: HashMap::new(),
            group_seqs: HashMap::new(),
            parents: HashMap::new(),
            closures: Mutex::new(HashMap::new()),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether any ACL exists at all.
    ///
    /// The cheap way to ask whether this cluster uses ACLs, which is
    /// what lets a scan skip the whole apparatus -- including the
    /// account read that decides an administrator's bypass -- on a
    /// cluster that does not.
    pub fn has_acls(&self) -> bool {
        !self.acls.is_empty()
    }

    pub fn group(&self, name: &str) -> Option<&GroupRecord> {
        self.groups.get(name)
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn acl_count(&self) -> usize {
        self.acls.len()
    }

    /// How many groups `creator` has created, for the per-creator quota.
    pub fn groups_created_by(&self, creator: &str) -> usize {
        self.groups
            .values()
            .filter(|group| group.created_by == creator)
            .count()
    }

    /// Every group, by name, in no particular order.
    pub fn groups(&self) -> impl Iterator<Item = (&str, &GroupRecord)> {
        self.groups.iter().map(|(name, group)| (name.as_str(), group))
    }

    /// Every ACL, by the key prefix it governs, in no particular order.
    pub fn acls(&self) -> impl Iterator<Item = (&[u8], &AclRecord)> {
        self.acls.iter().map(|(prefix, acl)| (prefix.as_slice(), acl))
    }

    /// The seq a group was last written at.
    pub fn group_seq(&self, name: &str) -> u64 {
        self.group_seqs.get(name).copied().unwrap_or(0)
    }

    /// The seq an ACL was last written at.
    pub fn acl_seq(&self, prefix: &[u8]) -> u64 {
        self.acl_seqs.get(prefix).copied().unwrap_or(0)
    }

    /// The ACL governing `key`: the one on the longest prefix of `key`
    /// that has one, with the prefix it is on.
    pub fn governing(&self, key: &[u8]) -> Option<(&[u8], &AclRecord)> {
        self.governing_within(key, key.len())
    }

    /// The ACL governing `key` among prefixes of at most `max_len`
    /// bytes.
    ///
    /// The bound is what answers "if the ACL on this prefix were
    /// removed, what would govern it then?" -- pass one less than the
    /// prefix's own length and the answer is whatever it would fall back
    /// to, which is the question `DeleteAcl` has to ask before allowing
    /// a removal.
    ///
    /// One hash probe per distinct ACL prefix length in range, longest
    /// first, so the first hit is the answer.
    pub fn governing_within(&self, key: &[u8], max_len: usize) -> Option<(&[u8], &AclRecord)> {
        let max_len = max_len.min(key.len());
        for &len in &self.acl_lengths {
            if len > max_len {
                continue;
            }
            let prefix = &key[..len];
            if let Some((prefix, acl)) = self.acls.get_key_value(prefix) {
                return Some((prefix.as_slice(), acl));
            }
        }
        None
    }

    /// The ACL written for exactly this prefix, if there is one.
    pub fn acl_at(&self, prefix: &[u8]) -> Option<&AclRecord> {
        self.acls.get(prefix)
    }

    /// What the ACLs say about `caller` and `key`.
    pub fn decide(&self, caller: &str, key: &[u8]) -> Decision {
        let Some((_, acl)) = self.governing(key) else {
            return Decision::Default;
        };
        self.decide_within(caller, acl)
    }

    /// What `acl` alone says about `caller` -- the second half of
    /// [`decide`], split out for the ACL-management checks, which have
    /// already chosen which ACL decides.
    ///
    /// [`decide`]: Permissions::decide
    pub fn decide_within(&self, caller: &str, acl: &AclRecord) -> Decision {
        let mut best: Option<Right> = None;
        // The membership closure is computed at most once, and only if
        // some entry actually names a group.
        let mut closure: Option<Arc<HashSet<String>>> = None;
        for (principal, right) in &acl.entries {
            // Nothing beats `Write`, so stop once it is held rather than
            // resolving group membership for the remaining entries.
            if best == Some(Right::Write) {
                break;
            }
            let matches = match principal {
                Principal::User(name) => name == caller,
                Principal::Group(name) => closure
                    .get_or_insert_with(|| self.groups_of(caller))
                    .contains(name),
            };
            if matches {
                best = Some(best.map_or(*right, |held: Right| held.max(*right)));
            }
        }
        match best {
            Some(right) => Decision::Granted(right),
            None => Decision::Denied,
        }
    }

    /// Whether `caller` is `principal`, or is in it transitively.
    pub fn is(&self, caller: &str, principal: &Principal) -> bool {
        match principal {
            Principal::User(name) => name == caller,
            Principal::Group(name) => self.groups_of(caller).contains(name),
        }
    }

    /// Whether `caller` matches any of `principals`, which is how "may
    /// this caller administer this group" is asked.
    pub fn is_any(&self, caller: &str, principals: &[Principal]) -> bool {
        // One closure for the whole list rather than one per principal.
        let mut closure: Option<Arc<HashSet<String>>> = None;
        principals.iter().any(|principal| match principal {
            Principal::User(name) => name == caller,
            Principal::Group(name) => closure
                .get_or_insert_with(|| self.groups_of(caller))
                .contains(name),
        })
    }

    /// Every group `caller` is in, directly or through another group.
    ///
    /// Memoised: see [`Permissions::closures`].
    pub fn groups_of(&self, caller: &str) -> Arc<HashSet<String>> {
        if let Ok(memo) = self.closures.lock()
            && let Some(closure) = memo.get(caller)
        {
            return Arc::clone(closure);
        }
        let closure = Arc::new(self.compute_groups_of(caller));
        if let Ok(mut memo) = self.closures.lock() {
            memo.insert(caller.to_string(), Arc::clone(&closure));
        }
        closure
    }

    /// The walk itself: breadth-first up [`Permissions::parents`] from
    /// the caller, one level of nesting per round, stopping at
    /// [`MAX_GROUP_DEPTH`].
    ///
    /// The `inside` set is what makes a cycle terminate rather than
    /// something the depth bound has to catch -- a group already reached
    /// is not expanded twice. The bound is there for the other case, a
    /// legitimately absurd nesting depth, and to put a ceiling on the
    /// work one request can ask for.
    fn compute_groups_of(&self, caller: &str) -> HashSet<String> {
        let mut inside: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = self
            .parents
            .get(&Principal::User(caller.to_string()))
            .cloned()
            .unwrap_or_default();

        for _ in 0..MAX_GROUP_DEPTH {
            if frontier.is_empty() {
                break;
            }
            let mut next = Vec::new();
            for name in frontier {
                if !inside.insert(name.clone()) {
                    continue;
                }
                if let Some(parents) = self.parents.get(&Principal::Group(name)) {
                    next.extend(parents.iter().cloned());
                }
            }
            frontier = next;
        }
        inside
    }

    /// Whether adding `child` to `parent` would make `parent` reachable
    /// from itself.
    ///
    /// Asked of the snapshot for the error message a caller gets; the
    /// authoritative check happens inside the write transaction, against
    /// records it holds read locks on. Both exist because they answer
    /// different questions: this one is fast and may be stale, that one
    /// is slow and cannot be.
    pub fn would_create_a_cycle(&self, parent: &str, child: &str) -> bool {
        if parent == child {
            return true;
        }
        // Everything reachable from `child`, downwards through nested
        // groups. If `parent` is in there, the new edge closes a loop.
        let mut seen: HashSet<&str> = HashSet::new();
        let mut frontier = vec![child];
        while let Some(name) = frontier.pop() {
            if !seen.insert(name) {
                continue;
            }
            if name == parent {
                return true;
            }
            let Some(group) = self.groups.get(name) else {
                continue;
            };
            for principal in &group.members {
                if let Principal::Group(nested) = principal {
                    frontier.push(nested.as_str());
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests;
