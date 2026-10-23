//! Groups (roles included) and access control lists.
//!
//! Every call here that changes something writes one shared key, the
//! permission epoch. So two at once resolve like any two conflicting
//! transactions, and one may return [`Error::Aborted`]. This is on
//! purpose: the cluster decides each change against a view that cannot
//! move under it, so (for example) a write grant being revoked cannot be
//! used in its last moment to write an ACL that grants it back. A replay
//! of an aborted call is always safe: it wrote nothing.
//!
//! Reads contend for nothing.

use raft5_shared::raft5::client::{
    Acl as PbAcl, AclEntry as PbAclEntry, CreateGroupRequest, CreateGroupResponse, DeleteAclRequest,
    DeleteAclResponse, DeleteGroupRequest, DeleteGroupResponse, GetAclRequest, GetAclResponse,
    GetGroupRequest, GetGroupResponse, Group as PbGroup, ListAclsRequest, ListAclsResponse,
    ListGroupsRequest, ListGroupsResponse, Principal as PbPrincipal, Right as PbRight,
    SetAclRequest, SetAclResponse, UpdateGroupRequest, UpdateGroupResponse, create_group_response,
    delete_acl_response, delete_group_response, get_acl_response, get_group_response,
    list_acls_response, list_groups_response, principal, set_acl_response, update_group_response,
};

use crate::{Client, Error, Reply};
use crate::routing::Kv;

/// Someone an ACL or a group can name.
///
/// Users and groups are separate namespaces: `Principal::user("ops")` and
/// `Principal::group("ops")` are two different principals, and neither
/// resolves to the other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Principal {
    User(String),
    Group(String),
}

impl Principal {
    pub fn user(name: impl Into<String>) -> Self {
        Self::User(name.into())
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self::Group(name.into())
    }

    fn to_pb(&self) -> PbPrincipal {
        PbPrincipal {
            kind: Some(match self {
                Self::User(name) => principal::Kind::User(name.clone()),
                Self::Group(name) => principal::Kind::Group(name.clone()),
            }),
        }
    }

    /// A principal from the wire. A kind this client does not know is left
    /// out of its list instead of failing the call: an ACL from a newer
    /// cluster is still mostly useful.
    fn from_pb(pb: &PbPrincipal) -> Option<Self> {
        match &pb.kind {
            Some(principal::Kind::User(name)) => Some(Self::User(name.clone())),
            Some(principal::Kind::Group(name)) => Some(Self::Group(name.clone())),
            None => None,
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

/// What an ACL grants. `Write` includes everything `Read` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Right {
    Read,
    Write,
}

impl Right {
    fn to_pb(self) -> i32 {
        match self {
            Self::Read => PbRight::Read as i32,
            Self::Write => PbRight::Write as i32,
        }
    }

    fn from_pb(right: i32) -> Option<Self> {
        match PbRight::try_from(right) {
            Ok(PbRight::Read) => Some(Self::Read),
            Ok(PbRight::Write) => Some(Self::Write),
            _ => None,
        }
    }
}

impl std::fmt::Display for Right {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
        })
    }
}

/// One access control list: the key prefix it governs and what it grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acl {
    /// The prefix. An empty prefix governs every key.
    pub prefix: Vec<u8>,
    pub entries: Vec<(Principal, Right)>,
    /// The seq the record was last written at.
    pub seq: u64,
}

impl Acl {
    fn from_pb(pb: PbAcl) -> Self {
        Self {
            prefix: pb.prefix,
            entries: pb
                .entries
                .iter()
                .filter_map(|entry| {
                    let principal = Principal::from_pb(entry.principal.as_ref()?)?;
                    Some((principal, Right::from_pb(entry.right)?))
                })
                .collect(),
            seq: pb.seq,
        }
    }
}

/// What governs one key, and what it grants the account that asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclFor {
    /// The ACL that governs the key, or `None` if none does. Then the key is
    /// open to every authenticated account.
    pub acl: Option<Acl>,
    /// What that ACL grants this account. `None` if it grants nothing, and
    /// when nothing governs the key.
    ///
    /// An administrator sees what the ACL says about them, not their bypass:
    /// the question is about the ACL.
    pub effective: Option<Right>,
}

/// One group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub name: String,
    /// The account that created it. It need not still administer the group.
    pub created_by: String,
    /// Who may change it.
    pub admins: Vec<Principal>,
    /// Who is in it. A group in this list is nested: all its members are
    /// members of this group too, transitively.
    pub members: Vec<Principal>,
    /// The seq the record was last written at.
    pub seq: u64,
}

impl Group {
    fn from_pb(pb: PbGroup) -> Self {
        Self {
            name: pb.name,
            created_by: pb.created_by,
            admins: pb.admins.iter().filter_map(Principal::from_pb).collect(),
            members: pb.members.iter().filter_map(Principal::from_pb).collect(),
            seq: pb.seq,
        }
    }
}

fn entries_to_pb(entries: &[(Principal, Right)]) -> Vec<PbAclEntry> {
    entries
        .iter()
        .map(|(principal, right)| PbAclEntry {
            principal: Some(principal.to_pb()),
            right: right.to_pb(),
        })
        .collect()
}

impl Client {
    /// Create a group. Any authenticated account may, up to a per-account
    /// quota. This client's account becomes its only administrator, whatever
    /// `admins` says.
    ///
    /// A group is a name for a set of principals and is useful only when an
    /// ACL names it, so creating one gives its creator nothing new.
    pub async fn create_group(
        &self,
        name: &str,
        members: &[Principal],
        admins: &[Principal],
    ) -> Result<u64, Error> {
        let request = CreateGroupRequest {
            name: name.to_string(),
            members: members.iter().map(Principal::to_pb).collect(),
            admins: admins.iter().map(Principal::to_pb).collect(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_group(request).await.map(create_group_reply) }
        })
        .await
    }

    /// Delete a group. Its administrators, or a cluster administrator.
    ///
    /// ACLs that name it are left alone: a grant to a group that no longer
    /// exists matches nobody.
    pub async fn delete_group(&self, name: &str) -> Result<u64, Error> {
        let request = DeleteGroupRequest {
            name: name.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_group(request).await.map(delete_group_reply) }
        })
        .await
    }

    /// Add and remove members and administrators, in one transaction. The
    /// group's administrators, or a cluster administrator.
    ///
    /// Additions apply before removals, so a principal in both lists ends up
    /// removed. That makes "exactly these" one call that cannot land half
    /// done.
    pub async fn update_group(
        &self,
        name: &str,
        add_members: &[Principal],
        remove_members: &[Principal],
        add_admins: &[Principal],
        remove_admins: &[Principal],
    ) -> Result<u64, Error> {
        let request = UpdateGroupRequest {
            name: name.to_string(),
            add_members: add_members.iter().map(Principal::to_pb).collect(),
            remove_members: remove_members.iter().map(Principal::to_pb).collect(),
            add_admins: add_admins.iter().map(Principal::to_pb).collect(),
            remove_admins: remove_admins.iter().map(Principal::to_pb).collect(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.update_group(request).await.map(update_group_reply) }
        })
        .await
    }

    /// One group, as stored: members and admins are not expanded, so a nested
    /// group appears as itself.
    pub async fn get_group(&self, name: &str) -> Result<Group, Error> {
        let request = GetGroupRequest {
            name: name.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_group(request).await.map(get_group_reply) }
        })
        .await
    }

    /// Every group, in name order.
    pub async fn list_groups(&self) -> Result<Vec<Group>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_groups(ListGroupsRequest {})
                .await
                .map(list_groups_reply)
        })
        .await
    }

    /// Write the ACL that governs a key prefix, replacing what it held. An
    /// empty prefix governs every key.
    ///
    /// Requires `write` on that prefix, which, by the longest-prefix rule,
    /// means write on it or on a shorter prefix that governs it. A cluster
    /// administrator may always.
    ///
    /// It does not accept the default: creating an ACL where no ACL decides is
    /// an administrator's act. A key nothing governs is open to everyone, and
    /// that gives permission to use the key, not authority over who else may.
    pub async fn set_acl(
        &self,
        prefix: &[u8],
        entries: &[(Principal, Right)],
    ) -> Result<u64, Error> {
        let request = SetAclRequest {
            prefix: prefix.to_vec(),
            entries: entries_to_pb(entries),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.set_acl(request).await.map(set_acl_reply) }
        })
        .await
    }

    /// Remove the ACL on a key prefix, so the next shorter prefix with an ACL
    /// governs instead.
    ///
    /// Same authority as writing it, with the same exception from the other
    /// side: a removal that leaves the prefix governed by nothing, and so open
    /// to everyone, is an administrator's act.
    pub async fn delete_acl(&self, prefix: &[u8]) -> Result<u64, Error> {
        let request = DeleteAclRequest {
            prefix: prefix.to_vec(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_acl(request).await.map(delete_acl_reply) }
        })
        .await
    }

    /// Which ACL governs `key`, and what it grants this client's account.
    ///
    /// The question a holder of [`Denial::NotPermitted`] has, answered in one
    /// round trip, without reimplementing group expansion on top of
    /// [`list_acls`](Client::list_acls).
    ///
    /// [`Denial::NotPermitted`]: crate::Denial::NotPermitted
    pub async fn get_acl(&self, key: &[u8]) -> Result<AclFor, Error> {
        let request = GetAclRequest { key: key.to_vec() };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.get_acl(request).await.map(get_acl_reply) }
        })
        .await
    }

    /// Every ACL, shortest prefix first: the order they take effect, least
    /// specific to most.
    pub async fn list_acls(&self) -> Result<Vec<Acl>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_acls(ListAclsRequest {}).await.map(list_acls_reply)
        })
        .await
    }
}

reply! {
    fn create_group_reply(CreateGroupResponse, create_group_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn delete_group_reply(DeleteGroupResponse, delete_group_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn update_group_reply(UpdateGroupResponse, update_group_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn get_group_reply(GetGroupResponse, get_group_response) -> Group {
        Ok(group) => Reply::Ok(Group::from_pb(group)),
    }

    fn list_groups_reply(ListGroupsResponse, list_groups_response) -> Vec<Group> {
        Ok(list) => Reply::Ok(list.groups.into_iter().map(Group::from_pb).collect()),
    }

    fn set_acl_reply(SetAclResponse, set_acl_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn delete_acl_reply(DeleteAclResponse, delete_acl_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn get_acl_reply(GetAclResponse, get_acl_response) -> AclFor {
        Ok(ok) => Reply::Ok(AclFor {
            acl: ok.acl.map(Acl::from_pb),
            effective: ok.effective.and_then(Right::from_pb),
        }),
    }

    fn list_acls_reply(ListAclsResponse, list_acls_response) -> Vec<Acl> {
        Ok(list) => Reply::Ok(list.acls.into_iter().map(Acl::from_pb).collect()),
    }
}
