//! An interactive shell for a raft5 cluster.
//!
//! Connects once, using `--nodes` as seeds, and keeps that [`Client`]
//! alive for the whole session -- so, unlike a one-shot CLI, the second
//! command onward reuses whatever leader and membership the first one
//! already found.

use std::io::{self, Write};

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
            let (key, value, prev_seq) = write_args(args)?;
            let result = match prev_seq {
                Some(prev_seq) => {
                    client
                        .write_if(key.as_bytes(), Some(value.as_bytes()), prev_seq)
                        .await
                }
                None => client.write(key.as_bytes(), Some(value.as_bytes())).await,
            };
            print_seq(result);
        }
        "delete" => {
            let key = delete_args(args)?;
            let result = client.write(key.as_bytes(), None).await;
            print_seq(result);
        }
        "read" => {
            let [key] = fixed_args(args, "read <key>")?;
            print_read(client.read(key.as_bytes()).await);
        }
        "read_stale" => {
            let [key] = fixed_args(args, "read_stale <key>")?;
            print_read(client.read_stale(key.as_bytes()).await);
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
        other => println!("unknown command {other:?}; type `help` for a list"),
    }

    Ok(true)
}

/// `write <key> <value> [prev_seq]`.
fn write_args(args: &[String]) -> Result<(&str, &str, Option<u64>), String> {
    match args {
        [key, value] => Ok((key, value, None)),
        [key, value, prev_seq] => Ok((key, value, Some(parse_seq(prev_seq)?))),
        _ => Err("usage: write <key> <value> [prev_seq]".to_string()),
    }
}

/// `delete <key> [prev_seq]`.
fn delete_args(args: &[String]) -> Result<&str, String> {
    match args {
        [key] => Ok(key),
        _ => Err("usage: delete <key>".to_string()),
    }
}

fn parse_seq(s: &str) -> Result<u64, String> {
    s.parse()
        .map_err(|_| format!("{s:?} is not a valid sequence number"))
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

fn print_help() {
    println!(
        "{}",
        [
            "write <key> <value> [prev_seq]   write value to key, retrying until it lands;",
            "                                  with prev_seq, fails instead if the key has",
            "                                  moved on from it",
            "delete <key>                     same as write, with no value",
            "update <key> <value>             read-modify-write: retries a bounded number of",
            "                                  times if another write races it",
            "read <key>                       linearizable read",
            "read_stale <key>                 read from whichever node is currently connected,",
            "                                  without confirming it still leads",
            "add_server <client_addr>         add a server as a learner, by its client address",
            "remove_server <id>               remove a server",
            "bootstrap <id>                   form a single-node cluster around <id>",
            "endpoints                        list the client API addresses this client knows of",
            "help                             show this message",
            "quit                             end the session",
        ]
        .join("\n")
    );
}
