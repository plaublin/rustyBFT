// Everything related to crypto

//extern crate ed25519_dalek;

use ed25519_dalek::*;
use hmac::{Hmac, Mac};
use ring::digest;
use sha2::Sha256;

#[derive(Debug)]
pub struct Cryptografer {
    hmac: Vec<u8>,        // HMAC+SHA256 key
    key: Option<Keypair>, // ED25519 256 bits keys
}

impl Cryptografer {
    /// key is the concatenation (size == PUBLIC_KEY_LENGTH + SECRET_KEY_LENGTH) of the public
    /// [..PUBLIC_KEY_LENGTH] and secret keys [PUBLIC_KEY_LENGTH..]
    pub fn new(hmac: String, key: String) -> Self {
        let hmac = if hmac == "NONE" {
            vec![]
        } else {
            hmac.as_bytes().to_vec()
        };

        let key = if key == "NONE" {
            None
        } else {
            let mut bytes = [0u8; PUBLIC_KEY_LENGTH + SECRET_KEY_LENGTH];
            hex::decode_to_slice(key, &mut bytes as &mut [u8]).unwrap();
            let bytes = bytes.to_vec();
            if bytes.is_empty() {
                None
            } else {
                Some(Keypair {
                    public: PublicKey::from_bytes(&bytes[..PUBLIC_KEY_LENGTH]).unwrap(),
                    secret: SecretKey::from_bytes(&bytes[PUBLIC_KEY_LENGTH..]).unwrap(),
                })
            }
        };

        Cryptografer { hmac, key }
    }

    pub fn gen_hmac(&self, b: &[u8]) -> Vec<u8> {
        if self.hmac.is_empty() {
            Vec::new()
        } else {
            let mut mac =
                Hmac::<Sha256>::new_from_slice(&self.hmac).expect("HMAC can take key of any size");
            mac.update(b);
            let result = mac.finalize();
            result.into_bytes().to_vec()
        }
    }

    pub fn verify_hmac(&self, b: &[u8], h: &[u8]) -> bool {
        if self.hmac.is_empty() {
            true
        } else {
            self.gen_hmac(b) == h
        }
    }

    pub fn gen_sign(&self, b: &[u8]) -> Vec<u8> {
        match &self.key {
            Some(k) => k.sign(b).to_bytes().to_vec(),
            None => Vec::new(),
        }
    }

    pub fn verify_sign(&self, b: &[u8], s: &[u8]) -> bool {
        match &self.key {
            Some(k) => {
                let s = Signature::try_from(s).unwrap();
                k.verify(b, &s).is_ok()
            }
            None => true,
        }
    }

    pub fn digest_len() -> usize {
        digest::SHA256_OUTPUT_LEN
    }

    pub fn sign_len() -> usize {
        SIGNATURE_LENGTH
    }

    pub fn create_digest(b: &[u8]) -> Vec<u8> {
        digest::digest(&digest::SHA256, b).as_ref().to_vec()
    }
}

#[cfg(test)]
mod crypto_test {
    use super::*;
    use hex_literal::hex;

    const NONE_KEY: &str = "NONE";
    const HMAC_KEY: &str = "my secret and secure key";
    //                                              pub key ------------------------->|<-- priv key
    const KEY: &str = "42177206f5e9a64b12f44826bf917a65e958aaf2cd97464be33e8f7d86a65d722b794e21f3fd0ac8bdef1172f4f6cb1405043e469d33b812342a8a8f41b882c5";

    #[test]
    fn test_crypto_deactivated() {
        let crypto = Cryptografer::new(NONE_KEY.to_string(), NONE_KEY.to_string());
        assert!(crypto.hmac.is_empty() && crypto.key.is_none());

        let m = b"input message";
        assert_eq!(crypto.gen_hmac(m), vec![]);
    }

    #[test]
    fn test_valid_mac() {
        let crypto = Cryptografer::new(HMAC_KEY.to_string(), NONE_KEY.to_string());
        assert!(!crypto.hmac.is_empty());

        let m = b"input message";
        let h = crypto.gen_hmac(m);
        let expected = hex!("97d2a569059bbcd8ead4444ff99071f4c01d005bcefe0d3567e1be628e5fdcd9");
        assert_eq!(h[..], expected[..]);
        assert!(crypto.verify_hmac(m, &expected));
    }

    #[test]
    fn test_invalid_mac() {
        let crypto = Cryptografer::new(HMAC_KEY.to_string(), NONE_KEY.to_string());
        assert!(!crypto.hmac.is_empty());

        let m = b"input message";
        let expected = hex!("97d2a569059bbcd8ead7");
        assert!(!crypto.verify_hmac(m, &expected));
    }

    #[test]
    fn test_load_ed25519_key() {
        let secret_key: [u8; SECRET_KEY_LENGTH] = [
            43, 121, 78, 33, 243, 253, 10, 200, 189, 239, 17, 114, 244, 246, 203, 20, 5, 4, 62, 70,
            157, 51, 184, 18, 52, 42, 138, 143, 65, 184, 130, 197,
        ];
        let public_key: [u8; PUBLIC_KEY_LENGTH] = [
            66, 23, 114, 6, 245, 233, 166, 75, 18, 244, 72, 38, 191, 145, 122, 101, 233, 88, 170,
            242, 205, 151, 70, 75, 227, 62, 143, 125, 134, 166, 93, 114,
        ];

        let crypto = Cryptografer::new(NONE_KEY.to_string(), KEY.to_string());

        assert_eq!(crypto.key.as_ref().unwrap().public.to_bytes(), public_key);
        assert_eq!(crypto.key.as_ref().unwrap().secret.to_bytes(), secret_key);
    }

    #[test]
    fn test_valid_sign() {
        // some bytes, in a vector
        let crypto = Cryptografer::new(NONE_KEY.to_string(), KEY.to_string());
        assert!(crypto.key.is_some());

        let m = b"input message";
        let s = crypto.gen_sign(m);
        assert!(crypto.verify_sign(m, &s));
    }

    #[test]
    fn test_invalid_sign() {
        // some bytes, in a vector
        let crypto = Cryptografer::new(NONE_KEY.to_string(), KEY.to_string());
        assert!(crypto.key.is_some());

        let m = b"input message";
        let mut s = crypto.gen_sign(m);
        s[0] = !s[0];
        assert!(!crypto.verify_sign(m, &s));
    }
}
