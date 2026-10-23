//! Loading certificates, and choosing the URI scheme for a dialer that may
//! or may not use TLS: the parts of TLS setup the server and client share.
//!
//! **Why the scheme matters.** tonic decides whether to use TLS from the
//! URI scheme alone: `https` gets the configured TLS, and anything else is
//! dialed in plaintext, even with a TLS config set. Cluster addresses live
//! in the Raft log, and a cluster configured without TLS has `http://` in
//! every one. So the dialer's own configuration decides whether to use
//! TLS, and [`with_scheme`] makes the address agree. Otherwise, turning TLS
//! on would silently do nothing for the addresses that already exist.

use std::path::Path;

use tonic::transport::{Certificate, Identity};

/// A certificate chain and its private key, from two PEM files.
pub fn load_identity(cert: &Path, key: &Path) -> Result<Identity, String> {
    Ok(Identity::from_pem(read(cert)?, read(key)?))
}

/// The certificates to trust, from a PEM file. May hold several (a
/// bundle), and may be a self-signed leaf used as its own anchor.
pub fn load_ca(path: &Path) -> Result<Certificate, String> {
    Ok(Certificate::from_pem(read(path)?))
}

fn read(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("could not read {}: {e}", path.display()))
}

/// `address` with its scheme set to `https` if `tls`, `http` if not.
///
/// An address with no scheme gets one. An address with another scheme is
/// left alone, for `Endpoint` to refuse with its own error.
pub fn with_scheme(address: &str, tls: bool) -> String {
    let scheme = if tls { "https" } else { "http" };
    let rest = match address.split_once("://") {
        Some(("http" | "https", rest)) => rest,
        Some(_) => return address.to_string(),
        None => address,
    };
    format!("{scheme}://{rest}")
}

#[cfg(test)]
mod tests;
