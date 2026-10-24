//! Kalshi request signing: base64(Ed25519(timestamp_ms + METHOD + path)).

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer as _, SigningKey, pkcs8::DecodePrivateKey};

// ponytail: Ed25519 only (the key type in use); add RSA-PSS if an RSA key ever shows up.
pub struct Signer {
    key_id: String,
    key: SigningKey,
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material; the id is truncated.
        write!(f, "Signer({}…)", &self.key_id[..self.key_id.len().min(8)])
    }
}

impl Signer {
    pub fn new(key_id: String, pem: &str) -> anyhow::Result<Self> {
        let key = SigningKey::from_pkcs8_pem(pem)
            .map_err(|e| anyhow::anyhow!("parsing Ed25519 PKCS#8 private key: {e}"))?;
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
        let sig = self.key.sign(format!("{ts}{method}{path}").as_bytes());
        [
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
            ("KALSHI-ACCESS-SIGNATURE", STANDARD.encode(sig.to_bytes())),
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
    use ed25519_dalek::{Signature, Verifier as _, pkcs8::EncodePrivateKey};

    #[test]
    fn signs_timestamp_method_path() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(Default::default()).unwrap();
        let signer = Signer::new("id".into(), &pem).unwrap();
        let [k, t, s] = signer.headers_at("GET", "/trade-api/ws/v2", 1_700_000_000_000);
        assert_eq!(k, ("KALSHI-ACCESS-KEY", "id".into()));
        assert_eq!(t, ("KALSHI-ACCESS-TIMESTAMP", "1700000000000".into()));
        let sig = Signature::from_slice(&STANDARD.decode(&s.1).unwrap()).unwrap();
        key.verifying_key()
            .verify(b"1700000000000GET/trade-api/ws/v2", &sig)
            .unwrap();
    }
}
