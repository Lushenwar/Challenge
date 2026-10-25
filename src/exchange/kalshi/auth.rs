//! Kalshi request signing: base64(sign(timestamp_ms + METHOD + path)), with whichever key
//! type the account registered: Ed25519 (RFC 8032) or RSA-PSS (SHA-256, MGF1-SHA-256,
//! salt = digest length).

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::pkcs8::DecodePrivateKey as _;
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use sha2::Sha256;

enum Key {
    Ed25519(Box<ed25519_dalek::SigningKey>),
    Rsa(Box<rsa::pss::SigningKey<Sha256>>),
}

pub struct Signer {
    key_id: String,
    key: Key,
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material; the id is truncated.
        let kind = match self.key {
            Key::Ed25519(_) => "ed25519",
            Key::Rsa(_) => "rsa",
        };
        write!(
            f,
            "Signer({kind}, {}…)",
            &self.key_id[..self.key_id.len().min(8)]
        )
    }
}

impl Signer {
    /// Parse a PEM private key: Ed25519 PKCS#8, RSA PKCS#8 (both `BEGIN PRIVATE KEY`) or
    /// RSA PKCS#1 (`BEGIN RSA PRIVATE KEY`). The type is found by parsing, not the header.
    pub fn new(key_id: String, pem: &str) -> anyhow::Result<Self> {
        let key = if let Ok(k) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            Key::Ed25519(Box::new(k))
        } else if let Ok(k) = RsaPrivateKey::from_pkcs8_pem(pem) {
            Key::Rsa(Box::new(rsa::pss::SigningKey::new(k)))
        } else if let Ok(k) = RsaPrivateKey::from_pkcs1_pem(pem) {
            Key::Rsa(Box::new(rsa::pss::SigningKey::new(k)))
        } else {
            anyhow::bail!("private key is neither Ed25519 PKCS#8 nor RSA PKCS#8/PKCS#1 PEM");
        };
        Ok(Self { key_id, key })
    }

    pub fn from_file(key_id: String, path: &Path) -> anyhow::Result<Self> {
        let pem = std::fs::read_to_string(path)
            .with_context(|| format!("reading private key {}", path.display()))?;
        Self::new(key_id, &pem)
    }

    /// Auth headers for `method path` (path without query string) at `ts_ms`.
    pub fn headers_at(&self, method: &str, path: &str, ts_ms: u64) -> [(&'static str, String); 3] {
        let ts = ts_ms.to_string();
        let msg = format!("{ts}{method}{path}");
        let sig = match &self.key {
            Key::Ed25519(k) => ed25519_dalek::Signer::sign(k.as_ref(), msg.as_bytes())
                .to_bytes()
                .to_vec(),
            Key::Rsa(k) => k
                .sign_with_rng(&mut rand::rngs::OsRng, msg.as_bytes())
                .to_vec(),
        };
        [
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
            ("KALSHI-ACCESS-SIGNATURE", STANDARD.encode(sig)),
        ]
    }

    pub fn headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before 1970")
            .as_millis() as u64;
        self.headers_at(method, path, ts_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier as _, pkcs8::EncodePrivateKey as _};
    use rsa::pkcs1::EncodeRsaPrivateKey as _;

    const MSG: &[u8] = b"1700000000000GET/trade-api/ws/v2";

    fn sign(pem: &str) -> Vec<u8> {
        let signer = Signer::new("id".into(), pem).unwrap();
        let [k, t, s] = signer.headers_at("GET", "/trade-api/ws/v2", 1_700_000_000_000);
        assert_eq!(k, ("KALSHI-ACCESS-KEY", "id".into()));
        assert_eq!(t, ("KALSHI-ACCESS-TIMESTAMP", "1700000000000".into()));
        STANDARD.decode(&s.1).unwrap()
    }

    #[test]
    fn ed25519_signs_timestamp_method_path() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(Default::default()).unwrap();
        let sig = Signature::from_slice(&sign(&pem)).unwrap();
        key.verifying_key().verify(MSG, &sig).unwrap();
    }

    #[test]
    fn rsa_pss_signs_from_pkcs8_and_pkcs1() {
        // 1024 bits keeps the test fast; Kalshi keys are 2048, the scheme is the same.
        let key = RsaPrivateKey::new(&mut rand::rngs::OsRng, 1024).unwrap();
        let verifier = rsa::pss::VerifyingKey::<Sha256>::new(key.to_public_key());
        let pkcs8 = key.to_pkcs8_pem(Default::default()).unwrap();
        let pkcs1 = key.to_pkcs1_pem(Default::default()).unwrap();
        for pem in [pkcs8.as_str(), pkcs1.as_str()] {
            let sig = rsa::pss::Signature::try_from(sign(pem).as_slice()).unwrap();
            verifier.verify(MSG, &sig).unwrap();
            // PSS is randomized: a wrong message must not verify.
            assert!(verifier.verify(b"other", &sig).is_err());
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(Signer::new("id".into(), "not a key").is_err());
        assert!(
            Signer::new(
                "id".into(),
                "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
            )
            .is_err()
        );
    }
}
