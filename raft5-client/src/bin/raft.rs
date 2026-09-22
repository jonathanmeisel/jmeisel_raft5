//! An interactive shell for a raft5 cluster.
//!
//! Connects once, using `--nodes` as seeds, and keeps that [`Client`]
//! alive for the whole session -- so, unlike a one-shot CLI, the second
//! command onward reuses whatever leader and membership the first one
//! already found.

use std::io::{self, IsTerminal, Write};

use clap::Parser;

use raft5_client::{Client, Error};

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
        other => println!("unknown command {other:?}; type `help` for a list"),
    }

    Ok(true)
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
            "add_server <client_addr>         add a server as a learner, by its client address",
            "remove_server <id>               remove a server",
            "bootstrap <id>                   form a single-node cluster around <id>",
            "endpoints                        list the client API addresses this client knows of",
            "login [username]                 log in; the password is always prompted for",
            "logout                           forget the token, on every connection this client holds",
            "whoami                           show who this session is logged in as",
            "create_user <name> [--admin]     create an account; administrators only",
            "delete_user <name>               remove an account; administrators only",
            "help                             show this message",
            "quit                             end the session",
        ]
        .join("\n")
    );
}
