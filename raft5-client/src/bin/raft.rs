//! An interactive shell for a raft5 cluster.
//!
//! Connects once, with `--nodes` as seeds, and keeps that [`Client`] for
//! the whole session. Unlike a one-shot CLI, the second command onward
//! reuses the leader and membership the first one found.

use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;

use clap::Parser;

use raft5_client::{Client, ClientTls, Consistency, Error, Invite, Membership, Principal, Right};

/// Open an interactive session against a raft5 cluster.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Comma-separated client API addresses of one or more cluster members to
    /// start from, for example `127.0.0.1:52005,127.0.0.1:52006`. A starting
    /// point, not a fixed list: see [`Client::new`]. An address with no scheme
    /// is taken as `http://`, or `https://` with `--ca-cert`.
    #[arg(long, value_delimiter = ',')]
    nodes: Vec<String>,

    /// Log in as this user before the session starts. The password is
    /// prompted for, never taken as an argument, where it would end up in
    /// shell history and the process list.
    ///
    /// A cluster started without a signing secret has nothing to log in to,
    /// so this is not needed there.
    #[arg(long)]
    user: Option<String>,

    /// The PEM certificate(s) to trust for the cluster's client API. Turns on
    /// TLS. Only these are trusted, not the system roots, so for a self-signed
    /// cluster this is that certificate (or its CA).
    #[arg(long)]
    ca_cert: Option<PathBuf>,

    /// The PEM certificate to present, for a cluster that requires mTLS.
    #[arg(long, requires_all = ["key", "ca_cert"])]
    cert: Option<PathBuf>,

    /// The PEM private key for `--cert`.
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,

    /// Check the servers' certificate against this name instead of the host
    /// in each address.
    #[arg(long, requires = "ca_cert")]
    tls_server_name: Option<String>,
}

/// The TLS configuration the flags describe, or `None` for plaintext.
fn tls(args: &Args) -> Result<Option<ClientTls>, String> {
    use raft5_shared::tls::{load_ca, load_identity};

    let Some(ca) = &args.ca_cert else {
        return Ok(None);
    };
    let mut tls = ClientTls::new(load_ca(ca)?);
    if let (Some(cert), Some(key)) = (&args.cert, &args.key) {
        tls = tls.identity(load_identity(cert, key)?);
    }
    if let Some(name) = &args.tls_server_name {
        tls = tls.server_name(name);
    }
    Ok(Some(tls))
}

/// Ask for a password, without echo when there is an echo to suppress.
///
/// `rpassword` reads from the terminal directly and fails if there is
/// none, so a shell driven by a script (which is how these commands are
/// tested) needs the plain path. There is nothing to hide then anyway: the
/// password arrives through a pipe, not typed where someone could see it.
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
    let tls = match tls(&args) {
        Ok(tls) => tls,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let scheme = if tls.is_some() { "https" } else { "http" };

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
                format!("{scheme}://{s}")
            }
        })
        .collect();

    let client = match tls {
        Some(tls) => Client::with_tls(seeds, tls),
        None => Client::new(seeds),
    };
    let client = match client {
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

/// Read commands from stdin until EOF or `quit`, and run each against
/// `client` in turn. One line is one command, with no pipelining, so a
/// command that blocks (a write waiting for an election, for example) is
/// seen to block before the next prompt appears. That is the point of a
/// shell meant for a human.
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

        let words = match split_line(line) {
            Ok(words) => words,
            Err(e) => {
                eprintln!("{e}");
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

/// Split a command line into words, each one bytes.
///
/// Not `shell_words`: POSIX quoting drops a backslash outside quotes and
/// turns `\\` into `\` inside double quotes, so `"k\xff"` as
/// [`quote_bytes`] prints it would not come back as the same key. Here a
/// backslash escape means the same thing everywhere, and covers everything
/// `quote_bytes` emits, so any key or value this shell prints can be pasted
/// back as an argument.
///
/// Whitespace separates words; `"..."` and `'...'` keep whitespace inside a
/// word, and `""` is a word of its own (the empty word, which `acl_set`
/// needs).
fn split_line(line: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut words = Vec::new();
    let mut word = Vec::new();
    // Whether a word is in progress. `word.is_empty()` cannot say this once
    // `""` counts as a word.
    let mut in_word = false;
    let mut quote = None;
    let mut chars = line.chars();

    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                unescape(&mut chars, &mut word)?;
                in_word = true;
            }
            '"' | '\'' if quote.is_none() => {
                quote = Some(c);
                in_word = true;
            }
            c if quote == Some(c) => quote = None,
            c if quote.is_none() && c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            c => {
                word.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                in_word = true;
            }
        }
    }
    if let Some(q) = quote {
        return Err(format!("unmatched {q}"));
    }
    if in_word {
        words.push(word);
    }
    Ok(words)
}

/// Decode the escape after a backslash, and add it to the end of `word`.
fn unescape(chars: &mut std::str::Chars, word: &mut Vec<u8>) -> Result<(), String> {
    let c = match chars.next().ok_or("trailing \\ escapes nothing")? {
        '\\' => '\\',
        '"' => '"',
        '\'' => '\'',
        ' ' => ' ',
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        '0' => '\0',
        'x' => {
            let hex: String = chars.take(2).collect();
            let byte = (hex.len() == 2)
                .then(|| u8::from_str_radix(&hex, 16).ok())
                .flatten()
                .ok_or_else(|| format!("\\x{hex} is not \\x followed by two hex digits"))?;
            word.push(byte);
            return Ok(());
        }
        'u' => {
            let bad = || "\\u takes a Unicode scalar value in braces, like \\u{7f}".to_string();
            if chars.next() != Some('{') {
                return Err(bad());
            }
            let rest = chars.as_str();
            let (hex, after) = rest.split_once('}').ok_or_else(bad)?;
            *chars = after.chars();
            u32::from_str_radix(hex, 16)
                .ok()
                .and_then(char::from_u32)
                .ok_or_else(bad)?
        }
        other => return Err(format!("unknown escape \\{other}")),
    };
    word.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
    Ok(())
}

/// Bytes as this shell prints them: quoted, with valid UTF-8 shown as text
/// and every other byte escaped as `\xNN`. [`split_line`] reads the result
/// back as the same bytes.
fn quote_bytes(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    for chunk in bytes.utf8_chunks() {
        out.extend(chunk.valid().escape_debug());
        for byte in chunk.invalid() {
            out.push_str(&format!("\\x{byte:02x}"));
        }
    }
    out.push('"');
    out
}

/// An argument that must be text (a username, a server id), not the
/// arbitrary bytes a key or value can be.
fn text(arg: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(arg).map_err(|_| format!("{} is not UTF-8", quote_bytes(arg)))
}

/// Run one tokenized command. Returns `Ok(false)` for `quit` or `exit`,
/// the only things that end the session.
///
/// Nothing carries over between commands. Transactions are not exposed
/// here, on purpose: one lives on the leader with a deadline of seconds,
/// and a human typing the next command would usually lose it, along with
/// the locks it held against everyone else. `write` and `delete` still
/// commit through a transaction, which the client opens and closes inside
/// the one call.
async fn run(client: &Client, words: &[Vec<u8>]) -> Result<bool, String> {
    let Some((cmd, args)) = words.split_first() else {
        return Ok(true);
    };
    let cmd = text(cmd)?;

    match cmd {
        "help" | "?" => print_help(),
        "quit" | "exit" => return Ok(false),
        "endpoints" => {
            for endpoint in client.endpoints() {
                println!("{endpoint}");
            }
        }
        "write" => {
            let [key, value] = fixed_args(args, "write <key> <value>")?;
            print_seq(client.write(key, Some(value)).await);
        }
        "delete" => {
            let [key] = fixed_args(args, "delete <key>")?;
            print_seq(client.write(key, None).await);
        }
        "read" | "read_current" | "version" | "version_current" => {
            if args.is_empty() {
                return Err(format!("usage: {cmd} <key>..."));
            }
            let consistency = if cmd.ends_with("_current") {
                Consistency::Linearizable
            } else {
                Consistency::Stale
            };
            print_reads(client, args, consistency, cmd.starts_with("version")).await;
        }
        "snapshot" => {
            // An id is optional: with none, the client uses its own clock, which is
            // where a clock belongs.
            let id = match args {
                [] => None,
                [id] => Some(id.as_slice()),
                _ => return Err("usage: snapshot [id]".to_string()),
            };
            match client.create_snapshot(id).await {
                Ok(s) => println!("created {} at seq={}", quote_bytes(&s.id), s.seq),
                Err(e) => println!("error: {e}"),
            }
        }
        "snapshots" => match client.list_snapshots().await {
            Ok(list) if list.is_empty() => println!("(no snapshots)"),
            Ok(list) => {
                for s in list {
                    println!("{} seq={}", quote_bytes(&s.id), s.seq);
                }
            }
            Err(e) => println!("error: {e}"),
        },
        "drop_snapshot" => {
            let [id] = fixed_args(args, "drop_snapshot <id>")?;
            match client.remove_snapshot(id).await {
                Ok(seq) => println!("ok, seq={seq}"),
                Err(e) => println!("error: {e}"),
            }
        }
        "read_at" | "version_at" => {
            let Some((id, keys)) = args.split_first().filter(|(_, keys)| !keys.is_empty()) else {
                return Err(format!("usage: {cmd} <snapshot> <key>..."));
            };
            let versions_only = cmd.starts_with("version");
            print_reads(client, keys, Consistency::Snapshot(id), versions_only).await;
        }
        "keys" | "keys_current" | "keys_at" => {
            let (consistency, rest) = match (cmd, args) {
                ("keys_at", [id, rest @ ..]) => (Consistency::Snapshot(id.as_slice()), rest),
                ("keys_at", []) => {
                    return Err("usage: keys_at <snapshot> [start_key]".to_string());
                }
                ("keys_current", rest) => (Consistency::Linearizable, rest),
                (_, rest) => (Consistency::Stale, rest),
            };
            let start = match rest {
                [] => &[][..],
                [start] => start.as_slice(),
                _ if cmd == "keys_at" => {
                    return Err("usage: keys_at <snapshot> [start_key]".to_string());
                }
                _ => return Err(format!("usage: {cmd} [start_key]")),
            };
            match client.key_iterator(start, consistency).collect().await {
                Ok(keys) if keys.is_empty() => println!("(no keys)"),
                Ok(keys) => {
                    for key in keys {
                        println!("{}", quote_bytes(&key));
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }
        "add_server" => {
            let [peer_address] = fixed_text_args(args, "add_server <peer_address>")?;
            match client.add_server(peer_address).await {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "remove_server" => {
            let [server_id] = fixed_text_args(args, "remove_server <server_id>")?;
            match client.remove_server(server_id).await {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "bootstrap" => {
            const USAGE: &str =
                "usage: bootstrap <server_id> [heartbeat_ms] [--restore-from <backup_file>]";
            // `--restore-from <file>` may come anywhere after the id.
            let mut rest: Vec<&Vec<u8>> = args.iter().collect();
            let mut restore_from = None;
            if let Some(i) = rest.iter().position(|a| a.as_slice() == b"--restore-from") {
                if i + 1 >= rest.len() {
                    return Err(USAGE.to_string());
                }
                restore_from = Some(text(rest[i + 1])?.to_string());
                rest.drain(i..=i + 1);
            }
            let (server_id, heartbeat_ms) = match rest.as_slice() {
                [server_id] => (text(server_id)?, None),
                [server_id, ms] => (text(server_id)?, Some(parse_millis(ms, USAGE)?)),
                _ => return Err(USAGE.to_string()),
            };
            let result = match restore_from {
                None => client.bootstrap(server_id, heartbeat_ms).await,
                Some(path) => match tokio::fs::File::open(&path).await {
                    Ok(file) => client.bootstrap_from_backup(server_id, heartbeat_ms, file).await,
                    Err(e) => {
                        println!("error: could not open {path}: {e}");
                        return Ok(true);
                    }
                },
            };
            match result {
                Ok(()) => println!("ok"),
                Err(e) => println!("error: {e}"),
            }
        }
        "backup" => {
            let (path, snapshot_id) = match args {
                [path] => (text(path)?, None),
                [path, id] => (text(path)?, Some(id.as_slice())),
                _ => return Err("usage: backup <file> [snapshot_id]".to_string()),
            };
            match write_backup(client, path, snapshot_id).await {
                Ok(bytes) => println!("ok, {bytes} bytes written to {path}"),
                Err(e) => println!("error: {e}"),
            }
        }
        "members" => {
            if !args.is_empty() {
                return Err("usage: members".to_string());
            }
            match client.members().await {
                Ok(membership) if membership.members.is_empty() => {
                    println!("(not part of a cluster yet)")
                }
                Ok(membership) => print_members(&membership),
                Err(e) => println!("error: {e}"),
            }
        }
        "heartbeat" => match args {
            [] => match client.heartbeat_ms().await {
                Ok(0) => println!("(not part of a cluster yet)"),
                Ok(ms) => println!("{ms}ms"),
                Err(e) => println!("error: {e}"),
            },
            [ms] => {
                let ms = parse_millis(ms, "usage: heartbeat [ms]")?;
                match client.set_heartbeat(ms).await {
                    Ok(()) => println!("ok"),
                    Err(e) => println!("error: {e}"),
                }
            }
            _ => return Err("usage: heartbeat [ms]".to_string()),
        },
        "login" => {
            let username = match args {
                [username] => text(username)?.to_string(),
                // Prompted, not defaulted: whose session this is must not be guessed.
                [] => read_line("username: ")?,
                _ => return Err("usage: login [username]".to_string()),
            };
            // The password is never a positional argument: it would end up in this
            // shell's history, which outlives the session.
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
                [username] => (text(username)?, false),
                [username, flag] if flag == b"--admin" => (text(username)?, true),
                _ => return Err("usage: create_user <username> [--admin]".to_string()),
            };
            let password = prompt_password(username)?;
            match client.create_user(username, &password, is_admin).await {
                Ok(seq) => println!("ok, seq={seq}"),
                Err(e) => println!("error: {e}"),
            }
        }
        "delete_user" => {
            let [username] = fixed_text_args(args, "delete_user <username>")?;
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
            // The token is a positional argument and the password is not. That looks
            // inconsistent but is not: a password is reusable and belongs to its owner
            // forever, while an invitation is single-use and stops being a secret the
            // moment it is used, which is the moment this command uses it.
            let (token, username) = match args {
                [token] => (text(token)?.to_string(), read_line("username: ")?),
                [token, username] => (text(token)?.to_string(), text(username)?.to_string()),
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
            let [invite_id] = fixed_text_args(args, "revoke_invite <invite_id>")?;
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
            let [name] = fixed_text_args(args, "group <name>")?;
            match client.get_group(name).await {
                Ok(group) => println!("{}", describe_group(&group)),
                Err(e) => println!("error: {e}"),
            }
        }
        "group_create" => {
            let [name] = fixed_text_args(args, "group_create <name>")?;
            print_seq(client.create_group(name, &[], &[]).await);
        }
        "group_delete" => {
            let [name] = fixed_text_args(args, "group_delete <name>")?;
            print_seq(client.delete_group(name).await);
        }
        "group_add" | "group_remove" => {
            let usage = format!("{cmd} <group> <principal> [principal ...]");
            let Some((name, rest)) = args.split_first() else {
                return Err(format!("usage: {usage}"));
            };
            let name = text(name)?;
            if rest.is_empty() {
                return Err(format!("usage: {usage}"));
            }
            // `--admin` says these principals are added to (or removed from) the
            // group's administrators, not its members. The two lists are separate
            // everywhere else, and must not be merged in this one place.
            let (as_admin, names) = match rest.split_first() {
                Some((flag, names)) if flag == b"--admin" && !names.is_empty() => (true, names),
                _ => (false, rest),
            };
            let principals = names
                .iter()
                .map(|name| parse_principal(text(name)?))
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
            match client.get_acl(key).await {
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
            // The prefix comes first and may be empty (the empty prefix governs every
            // key), so it is a positional argument that can validly be "".
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
                .map(|grant| parse_grant(text(grant)?))
                .collect::<Result<Vec<_>, _>>()?;
            print_seq(client.set_acl(prefix, &entries).await);
        }
        "acl_delete" => {
            let [prefix] = fixed_args(args, "acl_delete <prefix>")?;
            print_seq(client.delete_acl(prefix).await);
        }
        other => println!("unknown command {other:?}; type `help` for a list"),
    }

    Ok(true)
}

/// What `invite` was asked for.
struct InviteOptions {
    is_admin: bool,
    username: Option<String>,
    /// `Duration::ZERO` asks for the cluster's own default instead of guessing
    /// one here: how long an invitation may last is cluster policy, and this
    /// shell should not have its own opinion.
    ttl: std::time::Duration,
}

/// Parse `invite`'s flags. There are enough of them to need parsing,
/// unlike the fixed-arity commands, which match on shapes.
fn invite_options(args: &[Vec<u8>]) -> Result<InviteOptions, String> {
    const USAGE: &str = "usage: invite [--admin] [--user <name>] [--hours <n>]";
    let mut options = InviteOptions {
        is_admin: false,
        username: None,
        ttl: std::time::Duration::ZERO,
    };
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        match text(arg)? {
            "--admin" => options.is_admin = true,
            "--user" => {
                options.username =
                    Some(text(rest.next().ok_or_else(|| USAGE.to_string())?)?.to_string());
            }
            "--hours" => {
                let hours: u64 = text(rest.next().ok_or_else(|| USAGE.to_string())?)?
                    .parse()
                    .map_err(|_| "--hours takes a whole number of hours".to_string())?;
                options.ttl = std::time::Duration::from_secs(hours * 60 * 60);
            }
            _ => return Err(USAGE.to_string()),
        }
    }
    Ok(options)
}

/// Print a new invitation, token first.
///
/// The warning matters: this is the only time the cluster ever produces
/// the token (it keeps a hash), so an issuer who scrolls past it must
/// revoke the invitation and create another.
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
    // The id comes first on the line: `revoke_invite` takes it, and it is the
    // part most likely to be copied from a listing.
    format!(
        "{}  {who}, {rights}, from {}, {}",
        invite.id,
        invite.issued_by,
        remaining(invite.expires_unix_ms, now_unix_ms)
    )
}

/// How much time an invitation has left, by this machine's clock.
///
/// Relative, not absolute, because the cluster's clock decides and this one
/// only approximates it. "in about 3h" is honest about that in a way a
/// printed timestamp would not be.
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

/// A principal as this shell writes one: `user:alice` or `group:ops`.
///
/// The kind is never inferred from the name. Users and groups are separate
/// namespaces, so `ops` alone is ambiguous, and a guess would make the one
/// command where it matters (granting something to the wrong `ops`) fail
/// silently.
fn parse_principal(text: &str) -> Result<Principal, String> {
    match text.split_once(':') {
        Some(("user", name)) if !name.is_empty() => Ok(Principal::user(name)),
        Some(("group", name)) if !name.is_empty() => Ok(Principal::group(name)),
        _ => Err(format!(
            "{text:?} is not a principal; write user:<name> or group:<name>"
        )),
    }
}

/// One ACL grant, as `acl_set` takes it: `user:alice=write`.
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
        quote_bytes(&acl.prefix)
    };
    let grants = acl
        .entries
        .iter()
        .map(|(principal, right)| format!("{principal}={right}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{prefix}  {grants}  (seq={})", acl.seq)
}

/// Read one line from stdin, for arguments a command prefers to prompt
/// for instead of having them typed where others can see them.
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

/// Match `args` against exactly `N` positional arguments, so each command
/// above can destructure into named bindings instead of indexing.
fn fixed_args<'a, const N: usize>(
    args: &'a [Vec<u8>],
    usage: &str,
) -> Result<[&'a [u8]; N], String> {
    let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    refs.try_into().map_err(|_| format!("usage: {usage}"))
}

/// [`fixed_args`], for commands whose arguments are all [`text`].
fn fixed_text_args<'a, const N: usize>(
    args: &'a [Vec<u8>],
    usage: &str,
) -> Result<[&'a str; N], String> {
    let mut out = [""; N];
    for (out, arg) in out.iter_mut().zip(fixed_args::<N>(args, usage)?) {
        *out = text(arg)?;
    }
    Ok(out)
}

/// A millisecond count, for the commands that take one.
fn parse_millis(arg: &[u8], usage: &str) -> Result<u64, String> {
    text(arg)?.parse().map_err(|_| usage.to_string())
}

fn print_seq(result: Result<u64, Error>) {
    match result {
        Ok(seq) => println!("ok, seq={seq}"),
        Err(e) => println!("error: {e}"),
    }
}

/// Write a backup to `path`. It goes to a temporary file next to `path`
/// first and is renamed only when complete, so a failed backup never leaves
/// a partial file under the name a restore would use.
async fn write_backup(client: &Client, path: &str, snapshot_id: Option<&[u8]>) -> Result<u64, String> {
    let partial = format!("{path}.partial");
    let mut file = tokio::fs::File::create(&partial)
        .await
        .map_err(|e| format!("could not create {partial}: {e}"))?;
    let written = client.backup(snapshot_id, &mut file).await;
    drop(file);
    match written {
        Ok(info) => {
            tokio::fs::rename(&partial, path)
                .await
                .map_err(|e| format!("could not rename {partial} to {path}: {e}"))?;
            Ok(info.bytes)
        }
        Err(e) => {
            let _ = tokio::fs::remove_file(&partial).await;
            Err(e.to_string())
        }
    }
}

/// Read `keys` and print them, one line each, under the `as_of` they were
/// read at.
///
/// If the cluster cuts a response short, the rest is reissued, and each
/// batch prints its own `as_of`: two batches of a stale or current read are
/// two moments, and one heading for both would hide that.
async fn print_reads(
    client: &Client,
    keys: &[Vec<u8>],
    consistency: Consistency<'_>,
    versions_only: bool,
) {
    let mut rest = keys;
    while !rest.is_empty() {
        if rest.len() < keys.len() {
            println!("(cut short by the response size limit; reading the other {})", rest.len());
        }
        let answered = if versions_only {
            client.read_versions(rest, consistency).await.map(|read| {
                println!("as_of={}", read.as_of);
                for v in &read.entries {
                    match v.seq {
                        // Seq 0 means "no current value", so it is spelled out instead of printed
                        // as a number that looks like a real version.
                        0 => println!("{} (no value)", quote_bytes(&v.key)),
                        seq => println!("{} seq={seq}", quote_bytes(&v.key)),
                    }
                }
                read.entries.len()
            })
        } else {
            client.read(rest, consistency).await.map(|read| {
                println!("as_of={}", read.as_of);
                for e in &read.entries {
                    match &e.value {
                        Some(value) => println!(
                            "{} seq={} value={}",
                            quote_bytes(&e.key),
                            e.seq,
                            quote_bytes(value)
                        ),
                        None => println!("{} seq={} (no value)", quote_bytes(&e.key), e.seq),
                    }
                }
                read.entries.len()
            })
        };
        match answered {
            Ok(n) => rest = &rest[n..],
            Err(e) => {
                println!("error: {e}");
                return;
            }
        }
    }
}

/// One line per member, with ids padded to line up:
/// `n1  voter    leader  peer=... client=...`.
fn print_members(membership: &Membership) {
    let width = membership
        .members
        .iter()
        .map(|m| m.server_id.len())
        .max()
        .unwrap_or(0);
    for m in &membership.members {
        let role = if m.voting { "voter" } else { "learner" };
        let leads = if membership.leader.as_deref() == Some(m.server_id.as_str()) {
            "leader"
        } else {
            ""
        };
        println!(
            "{:width$}  {role:7}  {leads:6}  peer={} client={}",
            m.server_id, m.peer_address, m.client_address
        );
    }
}

fn print_help() {
    println!(
        "{}",
        [
            "write <key> <value>             write value to key, retrying until it lands",
            "delete <key>                     same as write, with no value",
            "read <key>...                    read keys, all as of one moment, from whichever",
            "                                  node is currently connected, without confirming",
            "                                  it still leads -- so it may miss a write that",
            "                                  just returned; as_of says how far it had applied",
            "read_current <key>...            read from the leader, confirming it still leads",
            "version <key>...                 the seqs keys were last written at, without their",
            "                                  values -- same cost whatever the values weigh;",
            "                                  stale like `read`",
            "version_current <key>...         the same, confirmed by the leader",
            "keys [start_key]                 list every key from <start_key> on, stale like `read`",
            "keys_current [start_key]         the same, each batch current like `read_current`",
            "snapshot [id]                    pin the current sequence as a snapshot; the id",
            "                                  defaults to this machine's clock. admins and",
            "                                  backup-operators only",
            "snapshots                        list every snapshot that exists",
            "drop_snapshot <id>               release a snapshot. admins and backup-operators",
            "read_at <snapshot> <key>...      read keys as they were when the snapshot was taken",
            "version_at <snapshot> <key>...   the seqs keys were at when the snapshot was taken",
            "keys_at <snapshot> [start_key]   list the keys that existed at the snapshot",
            "add_server <peer_addr>           add a server as a learner, by its peer address",
            "remove_server <id>               remove a server",
            "bootstrap <id> [heartbeat_ms]    form a single-node cluster around <id>; the",
            "                                  heartbeat defaults to 50ms",
            "  [--restore-from <file>]         ...holding the contents of a backup. accounts",
            "                                  come from the backup, passwords included; the",
            "                                  token secret does not (the node keeps its own)",
            "backup <file> [snapshot]         write a backup of every key, account, group and",
            "                                  ACL, as of now or a snapshot. admins and",
            "                                  backup-operators only",
            "members                          list the cluster's members: each one's id, whether",
            "                                  it votes yet or is a learner still catching up,",
            "                                  which leads, and its peer and client addresses",
            "heartbeat [ms]                   show the cluster's heartbeat interval, or set it.",
            "                                  raise it for nodes far apart: it should well",
            "                                  exceed the round trip between any two. admins only",
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
            "                                  two groups are built in: admins (the cluster's",
            "                                  administrators) and backup-operators (may back",
            "                                  up and manage snapshots, so may read every key",
            "                                  and password hash). grant either with group_add",
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
            "",
            "keys, values, prefixes and snapshot ids are bytes. quote an argument with \"...\"",
            "or '...' to keep spaces in it, and write any byte as \\xNN; \\n \\t \\r \\0 \\\\",
            "\\\" \\' and \\u{...} also work, quoted or not. they are printed the same way, so",
            "anything printed can be pasted back in as an argument",
        ]
        .join("\n")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<Vec<u8>> {
        split_line(line).unwrap()
    }

    #[test]
    fn every_byte_round_trips() {
        for byte in 0..=u8::MAX {
            let quoted = quote_bytes(&[byte]);
            assert_eq!(words(&quoted), vec![vec![byte]], "{quoted}");
        }
    }

    #[test]
    fn mixed_bytes_round_trip() {
        for bytes in [&b"caf\xc3\xa9\xff\x00\"\\ '\n#"[..], b"", b"\xc3", b"a b"] {
            assert_eq!(words(&quote_bytes(bytes)), vec![bytes.to_vec()]);
        }
    }

    #[test]
    fn valid_utf8_prints_as_text() {
        assert_eq!(quote_bytes("café".as_bytes()), "\"café\"");
        assert_eq!(quote_bytes(b"k\xff"), r#""k\xff""#);
        assert_eq!(quote_bytes(b"v\x00\xfe"), r#""v\0\xfe""#);
    }

    #[test]
    fn splits_words() {
        assert_eq!(
            words("  write a   b "),
            vec![b"write".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            words(r#"acl_set "" x"#),
            vec![b"acl_set".to_vec(), vec![], b"x".to_vec()]
        );
        assert_eq!(words(r#"a"b c"d"#), vec![b"ab cd".to_vec()]);
        assert_eq!(words(r"a\xffb"), vec![b"a\xffb".to_vec()]);
        assert_eq!(words(r"'it\'s'"), vec![b"it's".to_vec()]);
        assert_eq!(words(r#"'say "hi"'"#), vec![br#"say "hi""#.to_vec()]);
        assert_eq!(words(r"a\ b"), vec![b"a b".to_vec()]);
        assert_eq!(words(r"\u{e9}#"), vec!["é#".as_bytes().to_vec()]);
        assert!(words("").is_empty());
    }

    #[test]
    fn rejects_malformed_lines() {
        for line in [
            r#"read "abc"#,
            "read 'abc",
            r"read \q",
            r"read \xZ1",
            r"read \x1",
            r"read \u{d800}",
            r"read \u{41",
            r"read \u41",
            "read abc\\",
        ] {
            assert!(split_line(line).is_err(), "{line}");
        }
    }
}
