use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const KVS_VALUE_LEN: usize = 1000;

#[derive(Debug, Serialize, Deserialize)]
pub enum KVSRequest {
    Get,
    Set,
    Reply,
    Unknown,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KVSCommand {
    pub mode: KVSRequest,
    pub key: usize,
    #[serde(with = "serde_bytes")]
    pub value: Option<Vec<u8>>,
}

pub struct KVS {
    n_entries: usize,
    store: HashMap<usize, Vec<u8>>,
}

impl KVS {
    // create the KVS structure but doesn't populate it
    pub fn new(n_entries: usize) -> Self {
        println!("Creating a new KVS with {} entries", n_entries);
        KVS {
            n_entries,
            store: HashMap::new(),
        }
    }

    // for each key, insert something in the store
    pub fn populate(&mut self) {
        for i in 0..self.n_entries {
            let c = KVSCommand {
                mode: KVSRequest::Set,
                key: i,
                value: Some([0; KVS_VALUE_LEN].to_vec()),
            };
            self.execute_request(c);
        }
    }

    pub fn execute_request(&mut self, c: KVSCommand) -> KVSCommand {
        match c.mode {
            KVSRequest::Get => KVSCommand {
                mode: KVSRequest::Reply,
                key: c.key,
                value: Some(self.store[&c.key].clone()),
            },
            KVSRequest::Set => {
                if let Some(value) = c.value {
                    self.store.insert(c.key, value);
                } else {
                    self.store.remove(&c.key);
                }

                KVSCommand {
                    mode: KVSRequest::Reply,
                    key: c.key,
                    value: None,
                }
            }
            _ => panic!("Mode {:?} is not supported!", c.mode),
        }
    }

    pub fn execute_operation(&mut self, o: &[u8]) -> Vec<u8> {
        let req = parse_request(o);
        let rep = self.execute_request(req);
        create_request(rep)
    }
}

pub fn create_request(c: KVSCommand) -> Vec<u8> {
    rmp_serde::to_vec(&c).unwrap()
}

pub fn parse_request(req: &[u8]) -> KVSCommand {
    rmp_serde::from_slice::<KVSCommand>(req).unwrap()
}
