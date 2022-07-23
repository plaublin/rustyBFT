use crate::configuration::Node;
use crate::message::RawMessage;

pub trait NetworkLayer {
    fn new(id: u32, nonblocking: bool, use_replica_port: bool, config: &[Node]) -> Self
    where
        Self: Sized;
    fn send(&self, i: u32, m: &RawMessage);
    fn receive(&self) -> Result<RawMessage, std::io::Error>;
}
