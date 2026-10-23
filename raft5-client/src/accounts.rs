//! Logging in, accounts, and invitations.

use std::time::Duration;

use raft5_shared::raft5::client::{
    CreateInviteRequest, CreateInviteResponse, CreateUserRequest, CreateUserResponse,
    DeleteUserRequest, DeleteUserResponse, ListInvitesRequest, ListInvitesResponse, LoginRequest,
    LoginResponse, RedeemInviteRequest, RedeemInviteResponse, RevokeInviteRequest,
    RevokeInviteResponse, create_invite_response, create_user_response, delete_user_response,
    list_invites_response, login_response, redeem_invite_response, revoke_invite_response,
};

use crate::{Client, Error, Reply, empty_response};
use crate::routing::Kv;

/// One open invitation, as an administrator sees it.
///
/// All facts about an invitation except the secret, which exists only in
/// the reply to the [`create_invite`] that created it.
///
/// [`create_invite`]: Client::create_invite
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invite {
    /// The name to give [`revoke_invite`]: the hash of the token, not the
    /// token, so it can be written down and shared without being worth
    /// anything.
    ///
    /// [`revoke_invite`]: Client::revoke_invite
    pub id: String,
    /// The administrator who issued it.
    pub issued_by: String,
    /// Whether the account it creates will be an administrator. Set when it
    /// was issued; the recipient has no say.
    pub is_admin: bool,
    /// The only username it may create, if it was issued for one person.
    pub username: Option<String>,
    /// When it can no longer be redeemed, by the cluster's clock.
    pub expires_unix_ms: u64,
}

/// A new invitation: the secret to pass on, and its public record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewInvite {
    /// **The only time this is available.** The cluster keeps a hash and
    /// cannot produce it again. An issuer who loses it must withdraw the
    /// invitation and create another.
    pub token: String,
    pub invite: Invite,
    /// The seq the invitation was written at.
    pub seq: u64,
}

impl Client {
    /// Log in, and use the resulting token for everything this client does
    /// from now on, including connections it already holds and all of its
    /// clones.
    ///
    /// Against a cluster started without a signing secret, this fails with
    /// [`Denial::AuthDisabled`]: there is nothing to log in to.
    ///
    /// The credentials stay in memory, so a token that expires mid-session is
    /// replaced without the caller noticing. A caller that does not want to
    /// hold a password for the client's lifetime can use [`login_with_token`]
    /// instead, at the cost of seeing the expiry.
    ///
    /// [`login_with_token`]: Client::login_with_token
    /// [`Denial::AuthDisabled`]: crate::Denial::AuthDisabled
    pub async fn login(&self, username: &str, password: &str) -> Result<(), Error> {
        self.refresh_token(username, password).await?;
        self.router
            .set_credentials(Some((username.to_string(), password.to_string())));
        Ok(())
    }

    /// Use a token obtained elsewhere.
    ///
    /// Nothing is checked here: the first request that carries it finds out
    /// whether the cluster accepts it. No credentials are stored, so an expiry
    /// is reported as [`Error::Unauthenticated`], not silently replaced.
    pub fn login_with_token(&self, token: String) {
        self.router.set_token(Some(token));
        self.router.set_credentials(None);
    }

    /// Forget the token and any credentials held with it.
    pub fn logout(&self) {
        self.router.set_token(None);
        self.router.set_credentials(None);
    }

    /// Whether this client holds a token. Says nothing about whether the
    /// cluster still accepts it.
    pub fn is_logged_in(&self) -> bool {
        self.router.has_token()
    }

    /// Who this client logged in as, if it logged in.
    ///
    /// `None` for a client that was given a token: a token names its holder,
    /// but this client does not read the tokens it carries.
    pub fn username(&self) -> Option<String> {
        self.router.username()
    }

    /// Create an account. Administrators only.
    ///
    /// `is_admin` puts the new account in [`ADMINS_GROUP`] in the same
    /// transaction. Returns the seq the account was written at.
    ///
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        is_admin: bool,
    ) -> Result<u64, Error> {
        let request = CreateUserRequest {
            username: username.to_string(),
            password: password.to_string(),
            is_admin,
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_user(request).await.map(create_user_reply) }
        })
        .await
    }

    /// Remove an account. Administrators only.
    ///
    /// The account also leaves every group, so a later account with the same
    /// name inherits nothing. Refused for the last member of
    /// [`ADMINS_GROUP`].
    ///
    /// A token already issued to that account stays valid until it expires
    /// (nothing can withdraw one), but it gives no administrative rights once
    /// the account is gone.
    ///
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    pub async fn delete_user(&self, username: &str) -> Result<u64, Error> {
        let request = DeleteUserRequest {
            username: username.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.delete_user(request).await.map(delete_user_reply) }
        })
        .await
    }

    /// Create an invitation, so someone without an account can create one
    /// for themselves. Administrators only.
    ///
    /// Pass on the returned [`NewInvite::token`]. This is the only moment it
    /// exists: the cluster stores a hash and cannot produce it again.
    ///
    /// - `is_admin` puts the new account in [`ADMINS_GROUP`]: the
    ///   administrator decides here, not the recipient.
    /// - `username`, if given, pins the invitation to that name, so a
    ///   misdelivered invitation cannot become an account with another name.
    /// - `ttl` is refused if it is longer than the cluster allows, not
    ///   shortened. `Duration::ZERO` asks for the cluster's default.
    ///
    /// [`ADMINS_GROUP`]: crate::ADMINS_GROUP
    pub async fn create_invite(
        &self,
        is_admin: bool,
        ttl: Duration,
        username: Option<&str>,
    ) -> Result<NewInvite, Error> {
        let request = CreateInviteRequest {
            is_admin,
            ttl_ms: ttl.as_millis() as u64,
            username: username.map(str::to_string),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.create_invite(request).await.map(create_invite_reply) }
        })
        .await
    }

    /// Use an invitation to create an account, and choose its password.
    ///
    /// The one call on this client that needs no login: a caller with an
    /// invitation has no account yet. It does not log the client in either:
    /// the account exists afterwards, and [`login`](Client::login) gets a
    /// token for it.
    ///
    /// The invitation is used up in the same transaction that writes the
    /// account, so it works exactly once. Two failures leave it unused and are
    /// worth telling apart:
    /// - [`Denial::AlreadyExists`]: the username is taken; try another with
    ///   the same token.
    /// - [`Denial::BadInvite`]: the token itself is bad; get a new one from
    ///   the issuer.
    ///
    /// [`Denial::AlreadyExists`]: crate::Denial::AlreadyExists
    /// [`Denial::BadInvite`]: crate::Denial::BadInvite
    pub async fn redeem_invite(
        &self,
        token: &str,
        username: &str,
        password: &str,
    ) -> Result<u64, Error> {
        let request = RedeemInviteRequest {
            token: token.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        };
        // `call_inner(.., false)`: there are no credentials to fall back on. A
        // client redeeming an invitation has no account to log in to; it is
        // creating one.
        self.call_inner(
            |mut kv: Kv| {
                let request = request.clone();
                async move { kv.redeem_invite(request).await.map(redeem_invite_reply) }
            },
            false,
        )
        .await
    }

    /// Every open invitation. Administrators only.
    ///
    /// Never the secrets: those are gone. Lapsed invitations are not listed:
    /// nobody can use them, and the cluster sweeps them itself.
    pub async fn list_invites(&self) -> Result<Vec<Invite>, Error> {
        self.call(|mut kv: Kv| async move {
            kv.list_invites(ListInvitesRequest {})
                .await
                .map(list_invites_reply)
        })
        .await
    }

    /// Withdraw an open invitation, by its [`id`](Invite::id). Administrators
    /// only.
    ///
    /// The fix for one sent to the wrong person: the secret cannot be
    /// recalled, only made worthless. An invitation already redeemed gives
    /// [`Error::Rejected`], which is the answer you need: someone used it
    /// first.
    pub async fn revoke_invite(&self, invite_id: &str) -> Result<u64, Error> {
        let request = RevokeInviteRequest {
            invite_id: invite_id.to_string(),
        };
        self.call(|mut kv: Kv| {
            let request = request.clone();
            async move { kv.revoke_invite(request).await.map(revoke_invite_reply) }
        })
        .await
    }

    /// Get a token and start to use it, without touching the stored
    /// credentials.
    ///
    /// `call_inner(.., false)`: `Login` is the one call that must not answer
    /// an expired token by logging in again, because that is what it is
    /// doing.
    pub(crate) async fn refresh_token(&self, username: &str, password: &str) -> Result<(), Error> {
        let request = LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        };
        let routed = self
            .call_inner(
                |mut kv: Kv| {
                    let request = request.clone();
                    async move { kv.login(request).await.map(login_reply) }
                },
                false,
            )
            .await;

        let token = match routed {
            Ok(token) => token,
            // No node could be shown to be leading. For a cluster not yet
            // bootstrapped, that is normal, not a failure, and logging in is what an
            // operator must do there to get the token `Bootstrap` asks for. A node
            // with no configuration answers a login from its startup credentials
            // and needs no leader, so ask the nodes directly.
            Err(Error::Unavailable(why)) => match self.login_directly(&request).await {
                Ok(token) => token,
                // Report the first complaint, not the second: "no leader" is more
                // useful, and the direct attempt was a guess.
                Err(Error::Unavailable(_)) => return Err(Error::Unavailable(why)),
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        self.router.set_token(Some(token));
        Ok(())
    }

    /// One `Login` per known endpoint, in turn, with no leader routing.
    ///
    /// The fallback for a cluster with no leader. Sent to each node, not one,
    /// because this client's seeds may include nodes that do not hold the
    /// startup credentials.
    async fn login_directly(&self, request: &LoginRequest) -> Result<String, Error> {
        let mut last: Option<String> = None;
        for node in self.router.nodes() {
            let mut kv = node.connect();
            match kv.login(request.clone()).await {
                Ok(resp) => match login_reply(resp) {
                    Reply::Ok(token) => return Ok(token),
                    // A refusal on the merits is the cluster's answer, not this node's, so
                    // the next node would give the same refusal.
                    Reply::Denied { reason, detail } => {
                        return Err(Error::Denied { reason, detail });
                    }
                    _ => last = Some(format!("{} could not answer a login", node.key)),
                },
                Err(status) => last = Some(format!("{}: {status}", node.key)),
            }
        }
        Err(Error::Unavailable(last.unwrap_or_else(|| {
            "no cluster member answered a login".to_string()
        })))
    }
}

reply! {
    fn login_reply(LoginResponse, login_response) -> String {
        Ok(ok) => Reply::Ok(ok.token),
    }
}

/// An `Invite` from the wire.
fn invite_from_wire(invite: raft5_shared::raft5::client::Invite) -> Invite {
    Invite {
        id: invite.invite_id,
        issued_by: invite.issued_by,
        is_admin: invite.is_admin,
        username: invite.username,
        expires_unix_ms: invite.expires_unix_ms,
    }
}

reply! {
    fn create_invite_reply(CreateInviteResponse, create_invite_response) -> NewInvite {
        // A reply with the token but no record is treated like any other
        // incomplete reply. It is not filled with a default, because an
        // invitation shown with no expiry would read as one that never lapses.
        Ok(ok) => match ok.invite.map(invite_from_wire) {
            Some(invite) => Reply::Ok(NewInvite {
                token: ok.token,
                invite,
                seq: ok.seq,
            }),
            None => empty_response(),
        },
    }

    fn redeem_invite_reply(RedeemInviteResponse, redeem_invite_response) -> u64 {
        Ok(ok) => Reply::Ok(ok.seq),
    }

    fn list_invites_reply(ListInvitesResponse, list_invites_response) -> Vec<Invite> {
        Ok(list) => Reply::Ok(list.invites.into_iter().map(invite_from_wire).collect()),
    }

    fn revoke_invite_reply(RevokeInviteResponse, revoke_invite_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn create_user_reply(CreateUserResponse, create_user_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }

    fn delete_user_reply(DeleteUserResponse, delete_user_response) -> u64 {
        Seq(seq) => Reply::Ok(seq),
    }
}
