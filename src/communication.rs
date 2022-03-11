// Everything related to the network and communications

use memcmp::Memcmp;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::net::UdpSocket;

// Network stuff
pub struct Network {
    socket: UdpSocket, // my local socket
}

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum RawMessageType {
    Request,
    Reply,
    PrePrepare,
    Prepare,
    Commit,
}

#[derive(Debug)]
pub enum MessageWrapper {
    WrapRequest(Request),
    WrapReply(Reply),
    WrapPrePrepare(PrePrepare),
    WrapPrepare(Prepare),
    WrapCommit(Commit),
    Error(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RawMessage<'a> {
    pub t: RawMessageType,
    pub s: u32, // sender
    #[serde(with = "serde_bytes")]
    pub inner: &'a [u8],
    #[serde(with = "serde_bytes")]
    pub hmac: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Request {
    pub s: u64, // sequence number
    pub c: u32, // client id
    #[serde(with = "serde_bytes")]
    pub o: Vec<u8>, // operation payload
    #[serde(with = "serde_bytes")]
    pub sign: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Reply {
    pub v: u64, // view number
    pub s: u64, // request/reply sequence number
    pub r: u32, // replica id
    #[serde(with = "serde_bytes")]
    pub u: Vec<u8>, // result payload
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PrePrepare {
    pub v: u64, // view number
    pub n: u64, // PP sequence number
    #[serde(with = "serde_bytes")]
    pub r: Vec<u8>, // batch of requests
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Prepare {
    pub v: u64, // view number
    pub n: u64, // PP sequence number
    pub r: u32, // sender replica id
    #[serde(with = "serde_bytes")]
    pub d: Vec<u8>, // request digest
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Commit {
    pub v: u64, // view number
    pub n: u64, // PP sequence number
    pub r: u32, // sender replica id
}

impl PartialEq for Reply {
    fn eq(&self, r: &Self) -> bool {
        self.v == r.v && self.s == r.s
    }
}
impl PartialEq for Prepare {
    fn eq(&self, p: &Self) -> bool {
        self.v == p.v && self.n == p.n && Memcmp::memcmp(&self.d[..], &p.d[..])
    }
}

impl PartialEq for Commit {
    fn eq(&self, c: &Self) -> bool {
        self.v == c.v && self.n == c.n
    }
}

impl Network {
    pub const MAX_MESSAGE_LEN: usize = 65535;

    pub fn create_network(addr: String, nb: bool) -> Self {
        println!("Try to bind local socket at {}", addr);
        let socket = UdpSocket::bind(addr).expect("Cannot bind local socket");
        socket.set_nonblocking(nb).unwrap_or_else(|_| {
            panic!(
                "Unable to set socket {:?} in {}blocking mode",
                socket,
                if nb { "non-" } else { "" }
            );
        });
        Network { socket }
    }

    pub fn receive_raw_message<'a>(&self, buf: &'a mut [u8]) -> Option<RawMessage<'a>> {
        //let (amt, _src) = self.socket.recv_from(buf);
        let r = self.socket.recv_from(buf);
        if let Ok((amt, _src)) = r {
            let buf = &buf[..amt];
            /*
            println!(
                "Received {} bytes from {:?} on socket {:?}: {:?}",
                amt, _src, self.socket, buf
            );
            */

            Some(rmp_serde::from_slice::<RawMessage>(buf).unwrap())
        } else {
            None
        }
    }

    pub fn send_raw_message(&self, addr: String, m: &RawMessage) {
        // send the message
        let buf = rmp_serde::to_vec(&m).unwrap();
        let r = self.socket.send_to(&buf, addr);
        //println!("send result: {:?}", r);

        match r {
            Ok(s) => {
                // If the client couldn't send everything. This is not really an error because the
                // client will retransmit, but we should investigate, maybe there is a bigger problem
                // if it happens too frequently
                if s < buf.len() {
                    eprintln!("Short send: {} < {}", s, buf.len());
                }
            }
            Err(e) => panic!("{}", e),
        };
    }
}

#[cfg(test)]
mod communications_test {
    use super::*;

    #[test]
    fn test_local_comm() {
        let addr = "127.0.0.1:7777";
        let network = Network::create_network(addr.to_string(), false);

        // create a request
        let r1 = Request {
            c: 4,
            s: 27,
            o: vec![0; 10],
            sign: vec![],
        };
        let m1 = RawMessage {
            t: RawMessageType::Request,
            s: 42,
            inner: &rmp_serde::to_vec(&r1).unwrap(),
            hmac: vec![],
        };

        network.send_raw_message(addr.to_string(), &m1);

        let mut buf = [0; Network::MAX_MESSAGE_LEN];
        let m2 = network.receive_raw_message(&mut buf);
        assert!(m2.is_some());

        let m2 = m2.unwrap();
        assert!(m2.t == RawMessageType::Request);

        let r2 = rmp_serde::from_slice::<Request>(m2.inner).unwrap();
        assert!(r2.c == r1.c);
        assert!(r2.s == r1.s);
        assert!(r2.o == r1.o);
    }

    #[test]
    fn batching_serialization() {
        let r1 = Request {
            c: 4,
            s: 27,
            o: vec![0; 10],
            sign: vec![],
        };

        let batch1 = vec![r1];

        let buf = rmp_serde::to_vec(&batch1).unwrap();
        let batch2 = rmp_serde::from_slice::<Vec<Request>>(&buf).unwrap();

        assert!(batch1.len() == batch2.len());
    }
}
