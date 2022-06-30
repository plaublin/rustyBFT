use crate::configuration::Node;
use crate::message::MessageType;
use crate::message::RawMessage;
use ed25519_dalek::{Keypair, PublicKey, SecretKey, Signature, Signer};
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH};
use hmac::{Hmac, Mac};
use ring::digest;
use sha2::Sha256;

pub struct CryptoLayer {
    id: u32, // this node ID
    // In these 2 Vecs, vec[i] == the key of node i
    pub digest_keys: Vec<Vec<u8>>,
    pub signature_keys: Vec<Option<Keypair>>,
}

impl CryptoLayer {
    pub const fn digest_length() -> usize {
        digest::SHA256_OUTPUT_LEN
    }

    pub const fn signature_length() -> usize {
        ed25519_dalek::SIGNATURE_LENGTH
    }

    pub fn new(id: u32, config: &[Node]) -> Self {
        let mut digest_keys = Vec::new();
        let mut signature_keys = Vec::new();
        for (i, node) in config.iter().enumerate() {
            if i != node.id as usize {
                panic!(
                    "Node order is mixed up in the nodes configuration vector: {} != {}",
                    i, node.id
                );
            }

            /*
            println!(
                "Node {}, digest key {}, signature key {}",
                i, node.digest_key, node.signature_key
            );
            */

            digest_keys.push(if node.digest_key == "NONE" {
                vec![]
            } else {
                node.digest_key.as_bytes().to_vec()
            });

            signature_keys.push(if node.signature_key == "NONE" {
                None
            } else {
                let mut bytes = [0u8; PUBLIC_KEY_LENGTH + SECRET_KEY_LENGTH];
                hex::decode_to_slice(&node.signature_key, &mut bytes as &mut [u8]).unwrap();
                let bytes = bytes.to_vec();
                if bytes.is_empty() {
                    None
                } else {
                    Some(Keypair {
                        public: PublicKey::from_bytes(&bytes[..PUBLIC_KEY_LENGTH]).unwrap(),
                        secret: SecretKey::from_bytes(&bytes[PUBLIC_KEY_LENGTH..]).unwrap(),
                    })
                }
            });
        }

        Self {
            id,
            digest_keys,
            signature_keys,
        }
    }

    /// Authenticate message m that will be sent to i
    pub fn authenticate_message(&self, i: u32, m: &mut RawMessage) {
        // authenticate with the destination digest key
        let key = &self.digest_keys[i as usize];
        if key.is_empty() {
            // nothing to do: we do not authenticate messages
        } else {
            let _ = m.reset_header_digest();
            let mut mac =
                Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
            mac.update(&m.inner);
            let result = mac.finalize();
            let digest = result.into_bytes().to_vec();
            m.set_header_digest(digest.try_into().unwrap());
        }
    }

    pub fn message_authentication_is_valid(&self, m: &mut RawMessage) -> bool {
        //verify authentication with my own digest key
        let key = &self.digest_keys[self.id as usize];
        if key.is_empty() {
            // we do not authenticate messages, so the message is correct!
            true
        } else {
            let message_digest = m.reset_header_digest();
            let mut mac =
                Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
            mac.update(&m.inner);
            let result = mac.finalize();
            let digest = result.into_bytes().to_vec();
            m.set_header_digest(message_digest);
            digest == message_digest
        }
    }

    pub fn sign_request(&self, m: &mut RawMessage) {
        assert!(m.header().t == MessageType::to_u8(MessageType::Request));
        // use my own private key
        let key = &self.signature_keys[self.id as usize];
        match key {
            Some(k) => {
                let _ = m.reset_request_signature();
                let signature = k
                    .sign(&m.rawmessage_content()[..m.header().len])
                    .to_bytes()
                    .to_vec();
                m.set_request_signature(signature.try_into().unwrap());
            }
            None => (), // nothing to do: we do not sign requests
        }
    }

    pub fn request_signature_is_valid(&self, m: &mut RawMessage) -> bool {
        // use the public signature key of the sender
        let key = &self.signature_keys[m.header().sender as usize];
        match key {
            Some(k) => {
                let request_signature = m.reset_request_signature();
                let s = Signature::try_from(request_signature).unwrap();
                let ret = k.verify(&m.rawmessage_content()[..m.header().len], &s);
                m.set_request_signature(request_signature);
                ret.is_ok()
            }
            None => true, // we do not sign requests, so the request is correct!
        }
    }

    pub fn digest_request_batch(batch: &[u8]) -> [u8; CryptoLayer::digest_length()] {
        digest::digest(&digest::SHA256, batch)
            .as_ref()
            .try_into()
            .expect("Wrong size")
    }
}
