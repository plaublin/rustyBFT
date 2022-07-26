use std::fs::File;
use std::io::{self, BufRead};
use std::time;

/// Max UDP message length
pub const MAX_MESSAGE_LENGTH: usize = 65536;

/// Read timeout for non-blocking network I/O
pub const READ_TIMEOUT_MS: time::Duration = time::Duration::from_millis(100);

/// Timeout after which the client retransmits its request
pub const CLIENT_TIMEOUT_MS: time::Duration = time::Duration::from_millis(500);

/// Max number of pending consensus at the replicas
pub const MAX_PENDING_CONSENSUS: usize = 1;

/// True if run in hybrid mode, i.e., n = 2f+1 instead of n = 3f+1
pub const HYBRID_MODE: bool = false;

/// True if client requests are verified while the consensus is done
pub const SPECULATIVE_VERIFICATION: bool = false;

/// True if replica use UDPDK instead of the Linux sockets
#[cfg(feature = "udpdk")]
pub const USE_UDPDK: bool = false;

/// True => will print the messages received and sent
pub const DEBUG_PRINT_MESSAGES: bool = false;

#[macro_export]
macro_rules! dbg_println {
    ($($arg:tt)*) => {{
        if self::DEBUG_PRINT_MESSAGES {
        println!($($arg)*);
        }
    }};
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u32,
    pub ip: String,
    pub replica_port: u16,
    pub client_port: u16,
    pub digest_key: String,
    pub signature_key: String,
}

pub fn parse_configuration_file(config: &str) -> Vec<Node> {
    let mut nodes = Vec::new();
    let mut id = 0;
    let file = File::open(config).unwrap();
    for line in io::BufReader::new(file).lines().flatten() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let mut split = line.split_whitespace();
        let ip = split
            .next()
            .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
            .to_string();

        let replica_port = split
            .next()
            .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
            .parse::<u16>()
            .expect("Invalid replica port number");

        let client_port = split
            .next()
            .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
            .parse::<u16>()
            .expect("Invalid client port number");

        let digest_key = split
            .next()
            .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
            .to_string();

        let signature_key = split
            .next()
            .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
            .to_string();

        nodes.push(Node {
            id,
            ip,
            replica_port,
            client_port,
            digest_key,
            signature_key,
        });
        id += 1;
    }

    nodes
}
