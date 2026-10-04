//! Certificates for TLS tests, minted fresh by each test that needs
//! them -- nothing checked in to expire, and no test trusting another's.
//!
//! Compiled into every test binary that includes `common`, and into the
//! client's tests by path, most of which use only part of it or none.
#![allow(dead_code)]

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tonic::transport::{Certificate, Identity};

/// A certificate and its private key, as PEM.
pub struct Pem {
    pub cert: String,
    pub key: String,
}

impl Pem {
    pub fn identity(&self) -> Identity {
        Identity::from_pem(&self.cert, &self.key)
    }

    /// This certificate as a trust anchor -- what a bare self-signed
    /// certificate is trusted as.
    pub fn as_ca(&self) -> Certificate {
        Certificate::from_pem(&self.cert)
    }
}

/// A private, self-signed CA.
pub struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl TestCa {
    pub fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        let pem = params.self_signed(&key).unwrap().pem();
        Self {
            issuer: Issuer::new(params, key),
            pem,
        }
    }

    pub fn certificate(&self) -> Certificate {
        Certificate::from_pem(&self.pem)
    }

    /// A leaf this CA signs, good as both a server and a client
    /// certificate for `names` -- which is what a peer needs, being both.
    pub fn leaf(&self, names: &[&str]) -> Pem {
        let key = KeyPair::generate().unwrap();
        let cert = leaf_params(names).signed_by(&key, &self.issuer).unwrap();
        Pem {
            cert: cert.pem(),
            key: key.serialize_pem(),
        }
    }
}

/// A self-signed leaf, meant to be trusted as its own anchor: not
/// `CA:TRUE`, which webpki would refuse as a CA used as an end entity.
pub fn self_signed(names: &[&str]) -> Pem {
    let key = KeyPair::generate().unwrap();
    let cert = leaf_params(names).self_signed(&key).unwrap();
    Pem {
        cert: cert.pem(),
        key: key.serialize_pem(),
    }
}

fn leaf_params(names: &[&str]) -> CertificateParams {
    let mut params =
        CertificateParams::new(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    params.is_ca = IsCa::ExplicitNoCa;
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    params
}
