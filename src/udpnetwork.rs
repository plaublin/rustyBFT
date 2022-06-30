use crate::configuration::{Node, READ_TIMEOUT_MS};
use crate::message::RawMessage;
use crate::network::*;
use std::net::UdpSocket;

// Network stuff
pub struct UDPNetwork {
    id: u32,           // my id
    socket: UdpSocket, // my local socket
    addr: Vec<String>, // addr[i] == addr:port of node i
}

impl NetworkLayer for UDPNetwork {
    fn new(id: u32, nonblocking: bool, config: &[Node]) -> Self
    where
        Self: Sized,
    {
        let mut addr = Vec::new();
        for (i, node) in config.iter().enumerate() {
            if i != node.id as usize {
                panic!(
                    "Node order is mixed up in the nodes configuration vector: {} != {}",
                    i, node.id
                );
            }

            addr.push(format!("{}:{}", node.ip, node.port));
        }

        if id as usize >= addr.len() {
            panic!("ID {} is invalid: there are only {} nodes", id, addr.len());
        }

        println!("Binding node {} to {}", id, addr[id as usize]);
        let socket = UdpSocket::bind(&addr[id as usize]).expect("Cannot bind local socket");
        let _ = socket.set_nonblocking(nonblocking);
        socket
            .set_read_timeout(Some(READ_TIMEOUT_MS))
            .expect("Cannot set read timeout");
        UDPNetwork { id, socket, addr }
    }

    fn send(&self, i: u32, m: &RawMessage) {
        assert!(i != self.id);

        let addr = &self.addr[i as usize];
        let buf = &m.inner[..];

        // send the message
        loop {
            let r = self.socket.send_to(buf, addr);
            match r {
                Ok(s) => {
                    // If the client couldn't send everything. This is not really an error because the
                    // client will retransmit, but we should investigate, maybe there is a bigger problem
                    // if it happens too frequently
                    if s < buf.len() {
                        eprintln!("Short send: {} < {}", s, buf.len());
                    }
                    break;
                }
                // With many clients and large messages the primary can fail to send with
                // an Err("Ressource not available"), so don't panic!
                // We should check what kind of error it is and panic for some of them but not all
                Err(e) => panic!("{}", e),
            };
        }
    }

    fn receive(&self) -> Result<RawMessage, std::io::Error> {
        let mut m = RawMessage::default();
        let buf = &mut m.inner[..];

        let r = self.socket.recv_from(buf);
        match r {
            Ok((amt, _src)) => {
                /*
                println!(
                    "Received {} bytes from {:?} on socket {:?}: {:?}",
                    amt,
                    _src,
                    self.socket,
                    &buf[..amt]
                );
                */
                m.trim(amt);
                Ok(m)
            }
            Err(e) => Err(e),
        }
    }
}
