//! An interactive shell for a raft5 cluster.
//!
//! Connects once, using `--nodes` as seeds, and keeps that [`Client`]
//! alive for the whole session -- so, unlike a one-shot CLI, the second
//! command onward reuses whatever leader and membership the first one
//! already found.

use std::io::{self, IsTerminal, Write};

use clap::Parser;

use raft5_client::{Client, Error, Invite, Principal, Right};

/// Opens an interactive session against a raft5 cluster.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Comma-separated client API addresses of one or more cluster
    /// members to start from, e.g.
    /// `127.0.0.1:52005,127.0.0.1:52006`. A starting point rather than a
    /// fixed list -- see [`Client::new`]. An address with no scheme is
    /// taken to be `http://`.
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Log in as this user before the session starts. The password is
    /// prompted for; it is never taken as an argument, where it would
    /// end up in shell history and in the process list.
    ///
    /// Against a cluster started without a signing secret there is
    /// nothing to log in to, and this is not needed.
    #[arg(long)]
    user: Option<String>,
}

/// Asks for a password, without echoing it when there is an echo to
/// suppress.
///
/// `rpassword` reads from the terminal directly and fails outright if
/// there isn't one, so a shell being driven by a script -- which is how
/// these commands get tested -- needs the plain path instead. There is
/// nothing to hide in that case anyway: the password is arriving down a
/// pipe, not being typed where someone could read it.
fn prompt_password(username: &str) -> Result<String, String> {
    let prompt = format!("password for {username}: ");
    if io::stdin().is_terminal() {
        return rpassword::prompt_password(prompt)
            .map_err(|e| format!("could not read a password: {e}"));
    }
    read_line(&prompt)
}

async fn log_in(client: &Client, username: &str) -> Result<(), String> {
    let password = prompt_password(username)?;
    match client.login(username, &password).await {
        Ok(()) => {
            println!("logged in as {username}");
            Ok(())
        }
        Err(e) => Err(format!("could not log in: {e}")),
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();

    let seeds: Vec<String> = args
        .nodes
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            if s.contains("://") {
                s.to_string()
            } else {
                format!("http://{s}")
            }
        })
        .collect();

    let client = match Client::new(seeds) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("could not create client: {e}");
            std::process::exit(1);
        }
    };

    if let Some(username) = args.user.as_deref()
        && let Err(e) = log_in(&client, username).await
    {
        eprintln!("{e}");
        std::process::exit(1);
    }

    println!(
        "raft5 client shell -- {} seed(s), type `help` for commands",
        client.endpoints().len()
    );

    repl(&client).await;
}

/// Reads commands from stdin until EOF or `quit`, running each against
/// `client` in turn. One line is one command; nothing is pipelined, so a
/// command that blocks (a write waiting on an election, say) is seen to
/// do so before the next prompt appears -- which is the point of a shell
/// meant for a human to drive.
async fn repl(client: &Client) {
    let stdin = io::stdin();
    loop {
        print!("raft> ");
        if io::stdout().flush().is_err() {
            break;
        }

        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("could not read input: {e}");
                break;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let words = match shell_words::split(line) {
            Ok(words) => words,
            Err(e) => {
                eprintln!("unmatched quote: {e}");
                continue;
            }
        };

        match run(client, &words).await {
            Ok(true) => {}
            Ok(false) => break, // `quit` / `exit`
            Err(e) => eprintln!("{e}"),
        }
    }
}

/// Runs one already-tokenized command. Returns `Ok(false)` for `quit` /
/// `exit`, which is the only thing that ends the session.
///
/// Nothing is carried between commands. Transactions deliberately aren't
/// exposed here: one is held on the leader with a deadline measured in
/// seconds, and a human typing the next command would lose it more often
/// than not -- along with the locks it was holding on everyone else's
/// behalf. `write` and `delete` still commit through a transaction; the
/// client opens and closes it inside the one call.
async fn run(client: &Client, words: &[String]) -> Result<bool, String> {
    let Some((cmd, args)) = words.split_first() else {
        return Ok(true);
    };

    match cmd.as_str() {
        "help" | "?" => print_help(),
        "quit" | "exit" => return Ok(false),
        "endpoints" => {
            for endpoint in client.endpoints() {
                println!("{endpoint}");
            }
        }
        "write" => {
            let [key, value] = fixed_args(args, "write <key> <value>")?;
            print_seq(client.write(key.as_bytes(), Some(value.as_bytes())).await);
        }
        "delete" => {
            let [key] = fixed_args(args, "delete <key>")?;
            print_seq(client.write(key.as_bytes(), None).await);
        }
        "read" => {
            let [key] = fixed_args(args, "read <key>")?;
            print_read(client.read(key.as_bytes()).await);
        }
        "read_current" => {
            let [key] = fixed_args(args, "read_current <key>")?;
            print_read(client.read_linearizable(key.as_bytes()).await);
        }
        "version" => {
            let [key] = fixed_args(args, "version <key>")?;
            print_version(client.get_version(key.as_bytes()).await);
        }
        "version_current" => {
            let [key] = fixed_args(args, "version_current <key>")?;
            print_version(client.get_version_linearizable(key.as_bytes()).await);
        }
        "snapshot" => {
            // An id is optional: with none, the client stamps its own
            // clock, which is where a clock belongs.
            let id = match args {
                [] => None,
                [id] => Some(id.as_bytes()),
                _ => return Err("usage: snapshot [id]".to_string()),
            };
            match client.create_snapshot(id).await {
                Ok(s) => println!(
                    "created {} at seq={}",
                    String::from_utf8_lossy(&s.id),
                    s.seq
                ),
                Err(e) => println!("error: {e}"),
            }
        }
        "snapshots" => match client.list_snapshots().await {
            Ok(list) if list.is_empty() => println!("(no snapshots)"),
            Ok(list) => {
                for s in list {
                    println!("{} seq={}", String::from_utf8_lossy(&s.id), s.seq);
                }
            }
            Err(e) => println!("error: {e}"),
        },
        "drop_snapshot" => {
            let [id] = fixed_args(args, "drop_snapshot <id>")?;
            match client.remove_snapshot(id.as_bytes()).await {
                Ok(seq) => println!("ok, seq={seq}"),
                Err(e) => println!("error: {e}"),
            }
        }
        "read_at" => {
            let [id, key] = fixed_args(args, "read_at <snapshot> <key>")?;
            print_read(client.read_at(id.as_bytes(), key.as_bytes()).await);
        }
        "version_at" => {
            let [id, key] = fixed_args(args, "version_at <snapshot> <key>")?;
            print_version(client.get_version_at(id.as_bytes(), key.as_bytes()).await);
        }
        "keys_at" => {
            let (id, start) = match args {
                [id] => (id.as_str(), ""),
                [id, start] => (id.as_str(), start.as_str()),
                _ => return Err("usage: keys_at <snapshot> [start_key]".to_string()),
            };
            match client
                .key_iterator_at(id.as_bytes(), start.as_bytes())
                .collect()
                .await
            {
                Ok(keys) if keys.is_empty() => println!("(no keys)"),
                Ok(keys) => {
                    for key in keys {
                        println!("{}", String::from_utf8_lossy(&key));
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }
        "keys" => {
            let start = match args {
                [] => "",
                [start] => start.as_str(),
                _ => return Err("usage: keys [start_key]".to_string()),
            };
            match client.key_iterator(start.as_bytes()).collect().await {
                Ok(keys) if keys.is_empty() => println!("(no keys)"),
                Ok(keys) => {
                    for key in keys {
                        println!("{}", String::from_utf8_lossy(&key));
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }
        "add_server" => {
            let [client_address] = fixed_args(args, "add_server <client_address>")?;
            match client.add_server(client_address).await {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "remove_server" => {
            let [server_id] = fixed_args(args, "remove_server <server_id>")?;
            match client.remove_server(server_id).await {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "bootstrap" => {
            let [server_id] = fixed_args(args, "bootstrap <server_id>")?;
            match client.bootstrap(server_id).await {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "login" => {
            let username = match args {
                [username] => username.clone(),
                // Prompted rather than defaulted: whose session this is
                // is not something to guess at.
                [] => read_line("username: ")?,
                _ => return Err("usage: login [username]".to_string()),
            };
            // The password is never a positional argument: `shell_words`
            // would put it in this shell's history, where it would
            // outlive the session.
            if let Err(e) = log_in(client, &username).await {
                println!("{e}");
            }
        }
        "logout" => {
            client.logout();
            println!("logged out");
        }
        "whoami" => match (client.username(), client.is_logged_in()) {
            (Some(username), _) => println!("{username}"),
            (None, true) => println!("logged in with a token, which does not say who you are"),
            (None, false) => println!("not logged in"),
        },
        "create_user" => {
            let (username, is_admin) = match args {
                [username] => (username.as_str(), false),
                [username, flag] if flag == "--admin" => (username.as_str(), true),
                _ => return Err("usage: create_user <username> [--admin]".to_string()),
            };
            let password = prompt_password(username)?;
            match client.create_user(username, &password, is_admin).await {
                Ok(seq) => println!("ok, seq={seq}"),
                Err(e) => println!("error: {e}"),
            }
        }
        "delete_user" => {
            let [username] = fixed_args(args, "delete_user <username>")?;
            print_seq(client.delete_user(username).await);
        }
        "invite" => {
            let options = invite_options(args)?;
            match client
                .create_invite(options.is_admin, options.ttl, options.username.as_deref())
                .await
            {
                Ok(new) => print_new_invite(&new),
                Err(e) => println!("error: {e}"),
            }
        }
        "redeem" => {
            // The token is a positional argument and the password is
            // not, which looks inconsistent and is not. A password is
            // reusable and its owner's forever; an invitation is
            // single-use and stops being a secret the moment it is
            // spent, which is the same moment this command spends it.
            let (token, username) = match args {
                [token] => (token.clone(), read_line("username: ")?),
                [token, username] => (token.clone(), username.clone()),
                _ => return Err("usage: redeem <token> [username]".to_string()),
            };
            let password = prompt_password(&username)?;
            match client.redeem_invite(&token, &username, &password).await {
                Ok(seq) => println!("ok, seq={seq} -- now `login {username}`"),
                Err(e) => println!("error: {e}"),
            }
        }
        "invites" => match client.list_invites().await {
            Ok(invites) if invites.is_empty() => println!("(no outstanding invitations)"),
            Ok(invites) => {
                let now = unix_millis();
                for invite in &invites {
                    println!("{}", describe_invite(invite, now));
                }
            }
            Err(e) => println!("error: {e}"),
        },
        "revoke_invite" => {
            let [invite_id] = fixed_args(args, "revoke_invite <invite_id>")?;
            print_seq(client.revoke_invite(invite_id).await);
        }
        "groups" => match client.list_groups().await {
            Ok(groups) if groups.is_empty() => println!("(no groups)"),
            Ok(groups) => {
                for group in &groups {
                    println!("{}", describe_group(group));
                }
            }
            Err(e) => println!("error: {e}"),
        },
        "group" => {
            let [name] = fixed_args(args, "group <name>")?;
            match client.get_group(name).await {
                Ok(group) => println!("{}", describe_group(&group)),
                Err(e) => println!("error: {e}"),
            }
        }
        "group_create" => {
            let [name] = fixed_args(args, "group_create <name>")?;
            print_seq(client.create_group(name, &[], &[]).await);
        }
        "group_delete" => {
            let [name] = fixed_args(args, "group_delete <name>")?;
            print_seq(client.delete_group(name).await);
        }
        "group_add" | "group_remove" => {
            let usage = format!("{cmd} <group> <principal> [principal ...]");
            let Some((name, rest)) = args.split_first() else {
                return Err(format!("usage: {usage}"));
            };
            if rest.is_empty() {
                return Err(format!("usage: {usage}"));
            }
            // `--admin` says these principals are being added to (or
            // taken from) the group's administrators rather than its
            // members. The two lists are separate everywhere else, and
            // collapsing them here would be the one place they were not.
            let (as_admin, names) = match rest.split_first() {
                Some((flag, names)) if flag == "--admin" && !names.is_empty() => (true, names),
                _ => (false, rest),
            };
            let principals = names
                .iter()
                .map(|name| parse_principal(name))
                .collect::<Result<Vec<_>, _>>()?;
            let none: &[Principal] = &[];
            let result = if cmd == "group_add" {
                if as_admin {
                    client.update_group(name, none, none, &principals, none).await
                } else {
                    client.update_group(name, &principals, none, none, none).await
                }
            } else if as_admin {
                client.update_group(name, none, none, none, &principals).await
            } else {
                client.update_group(name, none, &principals, none, none).await
            };
            print_seq(result);
        }
        "acls" => match client.list_acls().await {
            Ok(acls) if acls.is_empty() => println!("(no ACLs -- every key is open)"),
            Ok(acls) => {
                for acl in &acls {
                    println!("{}", describe_acl(acl));
                }
            }
            Err(e) => println!("error: {e}"),
        },
        "acl" => {
            let [key] = fixed_args(args, "acl <key>")?;
            match client.get_acl(key.as_bytes()).await {
                Ok(raft5_client::AclFor { acl: None, .. }) => {
                    println!("(no ACL governs that key -- it is open to every account)");
                }
                Ok(raft5_client::AclFor {
                    acl: Some(acl),
                    effective,
                }) => {
                    println!("{}", describe_acl(&acl));
                    match effective {
                        Some(right) => println!("  you: {right}"),
                        None => println!("  you: (nothing)"),
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }
        "acl_set" => {
            // The prefix is first and may be empty, which is the one
            // that governs every key -- so it is a positional argument
            // that can legitimately be "".
            let Some((prefix, grants)) = args.split_first() else {
                return Err("usage: acl_set <prefix> <principal>=<right> ...".to_string());
            };
            if grants.is_empty() {
                return Err(
                    "usage: acl_set <prefix> <principal>=<right> ... (use acl_delete to remove one)"
                        .to_string(),
                );
            }
            let entries = grants
                .iter()
                .map(|grant| parse_grant(grant))
                .collect::<Result<Vec<_>, _>>()?;
            print_seq(client.set_acl(prefix.as_bytes(), &entries).await);
        }
        "acl_delete" => {
            let [prefix] = fixed_args(args, "acl_delete <prefix>")?;
            print_seq(client.delete_acl(prefix.as_bytes()).await);
        }
        other => println!("unknown command {other:?}; type `help` for a list"),
    }

    Ok(true)
}

/// What `invite` was asked for.
struct InviteOptions {
    is_admin: bool,
    username: Option<String>,
    /// `Duration::ZERO` asks the cluster for its own default rather than
    /// guessing at one here -- how long an invitation may last is the
    /// cluster's policy, and this shell should not have a second opinion
    /// about it.
    ttl: std::time::Duration,
}

/// `invite`'s flags, which are enough of them to want parsing rather
/// than matching on shapes the way the fixed-arity commands do.
fn invite_options(args: &[String]) -> Result<InviteOptions, String> {
    const USAGE: &str = "usage: invite [--admin] [--user <name>] [--hours <n>]";
    let mut options = InviteOptions {
        is_admin: false,
        username: None,
        ttl: std::time::Duration::ZERO,
    };
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--admin" => options.is_admin = true,
            "--user" => {
                options.username = Some(rest.next().ok_or_else(|| USAGE.to_string())?.clone());
            }
            "--hours" => {
                let hours: u64 = rest
                    .next()
                    .ok_or_else(|| USAGE.to_string())?
                    .parse()
                    .map_err(|_| "--hours takes a whole number of hours".to_string())?;
                options.ttl = std::time::Duration::from_secs(hours * 60 * 60);
            }
            _ => return Err(USAGE.to_string()),
        }
    }
    Ok(options)
}

/// Prints a freshly minted invitation, token first.
///
/// The warning is not decoration. This is the only time the cluster will
/// ever produce the token -- it keeps a hash -- so an issuer who scrolls
/// past it has to revoke the invitation and mint another.
fn print_new_invite(new: &raft5_client::NewInvite) {
    println!("token: {}", new.token);
    println!("  ^ copy this now; the cluster keeps only a hash and cannot show it again");
    println!("{}", describe_invite(&new.invite, unix_millis()));
}

fn describe_invite(invite: &Invite, now_unix_ms: u64) -> String {
    let who = match &invite.username {
        Some(username) => format!("for {username}"),
        None => "for anyone holding it".to_string(),
    };
    let rights = if invite.is_admin { "admin" } else { "ordinary" };
    // The id leads the line: it is what `revoke_invite` takes, and the
    // thing most likely to be copied out of a listing.
    format!(
        "{}  {who}, {rights}, from {}, {}",
        invite.id,
        invite.issued_by,
        remaining(invite.expires_unix_ms, now_unix_ms)
    )
}

/// How much longer an invitation has, by this machine's clock.
///
/// Relative rather than absolute because the cluster's clock is the one
/// that decides and this one is only approximating it -- "in about 3h"
/// is honest about that in a way a printed timestamp would not be.
fn remaining(expires_unix_ms: u64, now_unix_ms: u64) -> String {
    let left_ms = expires_unix_ms.saturating_sub(now_unix_ms);
    if left_ms == 0 {
        return "lapsed".to_string();
    }
    let minutes = left_ms / 60_000;
    match (minutes / 60, minutes % 60) {
        (0, m) => format!("expires in {m}m"),
        (h, m) => format!("expires in {h}h{m:02}m"),
    }
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A principal as this shell spells one: `user:alice` or `group:ops`.
///
/// The kind is never inferred from the name. Users and groups are
/// separate namespaces, so `ops` alone is genuinely ambiguous, and
/// guessing would make the one command where it mattered -- granting
/// something to the wrong `ops` -- the one that failed silently.
fn parse_principal(text: &str) -> Result<Principal, String> {
    match text.split_once(':') {
        Some(("user", name)) if !name.is_empty() => Ok(Principal::user(name)),
        Some(("group", name)) if !name.is_empty() => Ok(Principal::group(name)),
        _ => Err(format!(
            "{text:?} is not a principal; write user:<name> or group:<name>"
        )),
    }
}

/// One ACL grant, as `acl_set` takes them: `user:alice=write`.
fn parse_grant(text: &str) -> Result<(Principal, Right), String> {
    let Some((who, right)) = text.rsplit_once('=') else {
        return Err(format!(
            "{text:?} is not a grant; write <principal>=read or <principal>=write"
        ));
    };
    let right = match right {
        "read" => Right::Read,
        "write" => Right::Write,
        other => return Err(format!("{other:?} is not a right; write `read` or `write`")),
    };
    Ok((parse_principal(who)?, right))
}

fn join_principals(principals: &[Principal]) -> String {
    if principals.is_empty() {
        return "(none)".to_string();
    }
    principals
        .iter()
        .map(Principal::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn describe_group(group: &raft5_client::Group) -> String {
    format!(
        "{}  admins: {}  members: {}  (created by {}, seq={})",
        group.name,
        join_principals(&group.admins),
        join_principals(&group.members),
        group.created_by,
        group.seq,
    )
}

fn describe_acl(acl: &raft5_client::Acl) -> String {
    let prefix = if acl.prefix.is_empty() {
        "(every key)".to_string()
    } else {
        format!("{:?}", String::from_utf8_lossy(&acl.prefix))
    };
    let grants = acl
        .entries
        .iter()
        .map(|(principal, right)| format!("{principal}={right}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{prefix}  {grants}  (seq={})", acl.seq)
}

/// Reads one line from stdin, for the arguments a command would rather
/// ask for than have typed where they can be seen.
fn read_line(prompt: &str) -> Result<String, String> {
    print!("{prompt}");
    io::stdout().flush().map_err(|e| e.to_string())?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|e| format!("could not read input: {e}"))?;
    let line = line.trim().to_string();
    if line.is_empty() {
        return Err("nothing entered".to_string());
    }
    Ok(line)
}

/// Matches `args` against exactly `N` positional arguments, so each
/// command above can destructure straight into named bindings instead of
/// indexing.
fn fixed_args<'a, const N: usize>(args: &'a [String], usage: &str) -> Result<[&'a str; N], String> {
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    refs.try_into().map_err(|_| format!("usage: {usage}"))
}

fn print_seq(result: Result<u64, Error>) {
    match result {
        Ok(seq) => println!("ok, seq={seq}"),
        Err(e) => println!("error: {e}"),
    }
}

fn print_read(result: Result<raft5_client::ReadResult, Error>) {
    match result {
        Ok(raft5_client::ReadResult {
            seq,
            value: Some(value),
        }) => {
            println!("seq={seq} value={:?}", String::from_utf8_lossy(&value));
        }
        Ok(raft5_client::ReadResult { seq, value: None }) => {
            println!("seq={seq} (no value)");
        }
        Err(e) => println!("error: {e}"),
    }
}

/// Seq 0 is the "no current value" sentinel, so it is spelled out rather
/// than printed as a number that looks like a real version.
fn print_version(result: Result<u64, raft5_client::Error>) {
    match result {
        Ok(0) => println!("(no value)"),
        Ok(seq) => println!("seq={seq}"),
        Err(e) => println!("error: {e}"),
    }
}

fn print_help() {
    println!(
        "{}",
        [
            "write <key> <value>             write value to key, retrying until it lands",
            "delete <key>                     same as write, with no value",
            "read <key>                       read from whichever node is currently connected,",
            "                                  without confirming it still leads -- so it may",
            "                                  miss a write that just returned",
            "read_current <key>               read from the leader, confirming it still leads",
            "version <key>                    the seq a key was last written at, without its",
            "                                  value -- same cost whatever the value weighs;",
            "                                  stale like `read`",
            "version_current <key>            the same, confirmed by the leader",
            "keys [start_key]                list every key from <start_key> on, stale like `read`",
            "snapshot [id]                    pin the current sequence as a snapshot; the id",
            "                                  defaults to this machine's clock. admins only",
            "snapshots                        list every snapshot that exists",
            "drop_snapshot <id>               release a snapshot. admins only",
            "read_at <snapshot> <key>         read a key as it was when the snapshot was taken",
            "version_at <snapshot> <key>      the seq a key was at when the snapshot was taken",
            "keys_at <snapshot> [start_key]   list the keys that existed at the snapshot",
            "add_server <client_addr>         add a server as a learner, by its client address",
            "remove_server <id>               remove a server",
            "bootstrap <id>                   form a single-node cluster around <id>",
            "endpoints                        list the client API addresses this client knows of",
            "login [username]                 log in; the password is always prompted for",
            "logout                           forget the token, on every connection this client holds",
            "whoami                           show who this session is logged in as",
            "create_user <name> [--admin]     create an account; administrators only",
            "delete_user <name>               remove an account; administrators only",
            "invite [--admin] [--user <name>] mint a single-use invitation for someone else to",
            "       [--hours <n>]              make their own account with. administrators only;",
            "                                  the token it prints is shown once and never again",
            "redeem <token> [username]        spend an invitation on a new account. needs no",
            "                                  login -- it is what you run before you have one",
            "invites                          list the invitations still outstanding; admins only",
            "revoke_invite <id>               withdraw one, by the id `invites` shows",
            "groups                           list every group",
            "group <name>                     show one group's members and administrators",
            "group_create <name>              create a group; you become its administrator",
            "group_delete <name>              delete one; its administrators, or a cluster admin",
            "group_add <group> [--admin]      add principals to a group's members, or with",
            "          <principal> ...         --admin to its administrators. a principal is",
            "                                  written user:<name> or group:<name>",
            "group_remove <group> [--admin]   the same, taking them out again",
            "             <principal> ...",
            "acls                             list every ACL, least specific prefix first",
            "acl <key>                        which ACL governs a key, and what it grants you",
            "acl_set <prefix>                 replace the ACL on a key prefix. a grant is",
            "        <principal>=<right> ...   written user:alice=write or group:ops=read; an",
            "                                  empty prefix governs every key. needs write on",
            "                                  that prefix, or -- where no ACL governs it yet",
            "                                  -- an administrator",
            "acl_delete <prefix>              remove one, so a shorter prefix governs instead",
            "help                             show this message",
            "quit                             end the session",
        ]
        .join("\n")
    );
}
