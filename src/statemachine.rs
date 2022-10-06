use crate::configuration::*;
use crate::crypto::CryptoLayer;
use crate::dbg_println;
use crate::message::*;
use crate::network::NetworkLayer;
use crate::quorum::Quorum;
#[cfg(feature = "udpdk")]
use crate::udpdknetwork::UDPDKNetwork;
use crate::udpnetwork::UDPNetwork;
use crate::unixsocketnetwork::UnixSocketNetwork;
use crossbeam_channel::{Receiver, Sender};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::io::Write;
#[cfg(feature = "udpdk")]
use std::process;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::{thread, time};

pub struct Client {
    n: u32,
    f: u32,
    pub id: u32,       // this node's id; can be a client (id >= n) or a replica (id < n)
    seqnum: Cell<u64>, // request sequence number
    v: Cell<u64>,      // view number
    primary: u32,      // who is the current primary
    nodes: Vec<Node>,
    network: Box<dyn NetworkLayer>,
    crypto: CryptoLayer,
}

impl Client {
    pub fn new(config: &str, f: u32, id: u32) -> Self {
        let n = if HYBRID_MODE { 2 * f + 1 } else { 3 * f + 1 };
        assert!(id >= n, "Invalid client id {} >= {}", id, n);

        let nodes = parse_configuration_file(config);
        let crypto = CryptoLayer::new(id, &nodes);

        let network: Box<dyn NetworkLayer> = if USE_UNIX_SOCKETS {
            Box::new(UnixSocketNetwork::new(id, false, false, &nodes))
        } else {
            Box::new(UDPNetwork::new(id, false, false, &nodes))
        };

        Self {
            n,
            f,
            id,
            seqnum: Cell::new(0),
            v: Cell::new(0),
            primary: 0,
            nodes,
            network,
            crypto,
        }
    }

    pub fn my_address(&self) -> &Node {
        &self.nodes[self.id as usize]
    }

    pub fn create_request(&self, ro: bool, reqlen: usize) -> RawMessage {
        let mut request = RawMessage::new_request(self.id, ro, self.seqnum.get(), reqlen);
        self.crypto.sign_request(&mut request);
        self.crypto.authenticate_message(self.primary, &mut request);
        request
    }

    // Create a request that is not signed correctly
    pub fn create_malicious_request(&self, reqlen: usize) -> RawMessage {
        let mut request = RawMessage::new_request(self.id, false, self.seqnum.get(), reqlen);
        self.crypto.authenticate_message(self.primary, &mut request);
        request
    }

    pub fn send_request(&self, req: &RawMessage) {
        let r = req.message::<Request>();
        if r.ro {
            for i in 0..self.n {
                let mut my_req = req.clone();
                self.crypto.authenticate_message(i, &mut my_req);
                self.network.send(i, &my_req);
            }
        } else {
            self.network.send(self.primary, req);
        }
    }

    fn accept_reply(&self) -> Option<RawMessage> {
        let mut reply = loop {
            match self.network.receive() {
                Ok(r) => break r,
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return None;
                }
                Err(e) => panic!("{}", e),
            }
        };

        if reply.message_type() == MessageType::Reply
            && self.crypto.message_authentication_is_valid(&mut reply)
            && reply.message::<Reply>().seqnum == self.seqnum.get()
        {
            Some(reply)
        } else {
            None
        }
    }

    pub fn invoke(&self, req: &RawMessage) -> Rc<RawMessage> {
        dbg_println!(
            "Sending request (len {}) {:?}",
            req.message_len(),
            req.message::<Request>()
        );

        self.send_request(req);

        let mut start = time::Instant::now();

        let mut q = Quorum::new(self.n as usize, (self.f + 1) as usize);
        while !q.is_complete() {
            let ret = self.accept_reply();
            match ret {
                Some(reply) => {
                    let rep = reply.message::<Reply>();
                    dbg_println!("Received reply {:?}", rep);
                    q.add(rep.r, reply);
                    //println!("Quorum is now {:?}", q);
                }
                None => {
                    if start.elapsed() >= CLIENT_TIMEOUT_MS {
                        //println!("Retransmit request...");
                        self.send_request(req);
                        start = time::Instant::now();
                    }
                }
            }
        }

        let reply = q.value();
        let max_seqnum = reply.message::<Reply>().seqnum.max(self.seqnum.get());
        self.seqnum.replace(max_seqnum + 1); // move to next sequence number
        self.v.set(reply.message::<Reply>().v);

        q.value()
    }
}

#[derive(Debug)]
struct Consensus {
    batch: Vec<RawMessage>,
    pp: Option<RawMessage>,
    p: Quorum<RawMessage>, // 2f matching prepare
    c: Quorum<RawMessage>, // 2f+1 matching commit
    rep: Option<RawMessage>,
}

impl Consensus {
    fn prepare_quorum_size(f: u32) -> usize {
        if HYBRID_MODE {
            f as usize
        } else {
            (2 * f) as usize
        }
    }

    fn commit_quorum_size(f: u32) -> usize {
        if HYBRID_MODE {
            (f + 1) as usize
        } else {
            (2 * f + 1) as usize
        }
    }
}

#[derive(Clone, Copy)]
struct Statistics {
    min_batch_size: u64,
    max_batch_size: u64,
    total_batch_size: u64,
    nbatches: u64,
}

pub struct Replica {
    pub n: u32,
    pub f: u32,
    pub id: u32,       // this node's id; can be a client (id >= n) or a replica (id < n)
    seqnum: Cell<u64>, // PP sequence number
    next_pp_seqnum: Cell<u64>, // next PP sequence number
    v: u64,            // view number
    primary: u32,      // who is the current primary
    nodes: Vec<Node>,
    smr_to_crypto_sender: Sender<(u32, RawMessage)>,
    crypto_to_smr_receiver: Receiver<RawMessage>,
    batch_smr_to_crypto_sender: Sender<RawMessage>,
    batch_crypto_to_smr_receiver: Receiver<RawMessage>,
    speculative_smr_to_crypto_sender: Sender<RawMessage>,
    speculative_crypto_to_smr_receiver: Receiver<(u32, u64, bool)>,
    consensus: RefCell<BTreeMap<u64, Consensus>>, // the current consensus; BTree is ordered
    pending_req: RefCell<VecDeque<RawMessage>>,   // for batching
    // We assume there is at most 1 pending request per client
    speculatively_verified: RefCell<HashMap<u32, (u64, bool)>>, // speculative verification result.
    blacklist_lock: Arc<RwLock<HashSet<u32>>>,
    stats: Cell<Statistics>,
}

fn create_network_thread(
    id: u32,
    n: u32,
    nodes: Vec<Node>,
    net_to_crypto: Sender<RawMessage>,
    crypto_to_net: Receiver<(u32, RawMessage)>,
) {
    #[cfg(feature = "udpdk")]
    if USE_UDPDK {
        UDPDKNetwork::initialize();
    }

    let _ = thread::spawn(move || {
        fn get_network_layer(
            id: u32,
            nonblocking: bool,
            use_replica_port: bool,
            nodes: &[Node],
        ) -> Box<dyn NetworkLayer> {
            #[cfg(feature = "udpdk")]
            if USE_UDPDK {
                return Box::new(UDPDKNetwork::new(id, nonblocking, use_replica_port, nodes));
            }
            if USE_UNIX_SOCKETS {
                return Box::new(UnixSocketNetwork::new(
                    id,
                    nonblocking,
                    use_replica_port,
                    nodes,
                ));
            }
            return Box::new(UDPNetwork::new(id, nonblocking, use_replica_port, nodes));
        }

        println!("Starting the network thread");
        let replica_network = get_network_layer(id, true, true, &nodes);
        let client_network = get_network_layer(id, true, false, &nodes);

        loop {
            while !crypto_to_net.is_empty() {
                if let Ok((i, m)) = crypto_to_net.recv() {
                    //println!("net sends {:?} to {}", m, i);
                    if i < n {
                        replica_network.send(i, &m);
                    } else {
                        client_network.send(i, &m);
                    }
                }
            }

            // First receive messages from other replicas
            while let Ok(m) = replica_network.receive() {
                //println!("net has received {:?} and sends it to crypto", m);
                net_to_crypto.send(m).unwrap();
            }

            // and then receive messages from clients
            match client_network.receive() {
                Ok(m) => {
                    //println!("net has received {:?} and sends it to crypto", m);
                    net_to_crypto.send(m).unwrap();
                }
                Err(e) => {
                    if e.kind() != ErrorKind::WouldBlock {
                        println!("Error {}", e);
                    }
                }
            }
        }
    });
}

fn create_crypto_threads(
    id: u32,
    nodes: Vec<Node>,
    nthreads: usize,
    blacklist_lock: &Arc<RwLock<HashSet<u32>>>,
    (smr_to_crypto, crypto_to_smr): (Receiver<(u32, RawMessage)>, Sender<RawMessage>),
    (crypto_to_net, net_to_crypto): (Sender<(u32, RawMessage)>, Receiver<RawMessage>),
    (batch_smr_to_crypto, batch_crypto_to_smr): (Receiver<RawMessage>, Sender<RawMessage>),
    (speculative_smr_to_crypto_receiver, speculative_crypto_to_smr_sender): (
        Receiver<RawMessage>,
        Sender<(u32, u64, bool)>,
    ),
) {
    for t in 0..nthreads {
        let n = nodes.clone();
        let s2c = smr_to_crypto.clone();
        let c2s = crypto_to_smr.clone();
        let c2n = crypto_to_net.clone();
        let n2c = net_to_crypto.clone();
        let blist_lock = Arc::clone(blacklist_lock);

        // use to verify the batch of requests in the PP in parallel
        let batch_s2c = batch_smr_to_crypto.clone();
        let batch_c2s = batch_crypto_to_smr.clone();

        let speculative_s2c = speculative_smr_to_crypto_receiver.clone();
        let speculative_c2s = speculative_crypto_to_smr_sender.clone();

        let _ = thread::spawn(move || {
            println!("Starting crypto thread {}/{}", t, nthreads);
            let crypto = CryptoLayer::new(id, &n);

            loop {
                // receive from smr, authenticate, and send to net
                if let Ok((i, mut m)) = s2c.try_recv() {
                    //println!("{} will authenticate {:?}", t, m);
                    crypto.authenticate_message(i, &mut m);
                    //println!("{} sends {:?} to net for {}", t, m, i);
                    c2n.send((i, m)).unwrap();
                }

                // receive from net, authenticate, and send to smr
                if let Ok(mut m) = n2c.try_recv() {
                    //println!("{} will verify {:?}", t, m);

                    let mut blacklisted = false;
                    let mut valid = false;

                    // blacklist: if this is a request and the client is blacklisted then drop it
                    if m.message_type() == MessageType::Request {
                        let r = m.message::<Request>();
                        {
                            let blacklist = blist_lock.read().unwrap();
                            blacklisted = blacklist.contains(&r.c);
                        }
                    }

                    if !blacklisted {
                        valid = crypto.message_authentication_is_valid(&mut m);
                        if valid
                            && m.message_type() == MessageType::Request
                            && !SPECULATIVE_VERIFICATION
                        {
                            valid = crypto.request_signature_is_valid(&mut m);

                            // blacklist: here the primary receives a request from
                            //a client, so if the signature is invalid then blacklist this client
                            if !valid && BLACKLIST_NODES {
                                let c = m.message::<Request>().c;
                                {
                                    let mut blacklist = blist_lock.write().unwrap();
                                    blacklist.insert(c);
                                }
                                println!("Blacklist client {}", c);
                            }
                        }
                    } else {
                        //let r = m.message::<Request>();
                        //println!("Client {} is blacklisted", r.c);
                    }

                    if valid {
                        //println!("{} sends {:?} to smr", t, m);
                        c2s.send(m).unwrap();
                    } else {
                        //println!("{} received invalid request {:?} and dropping", t, m);
                    }
                }

                // receive a batch of requests to verify from smr
                // verify, and if valid send back to smr; otherwise send empty raw message
                if let Ok(mut m) = batch_s2c.try_recv() {
                    // we do not check the MAC because it's for the primary, not us another replica
                    assert!(m.message_type() == MessageType::Request);
                    if !crypto.request_signature_is_valid(&mut m) {
                        m = RawMessage::new(0);
                        //TODO blacklist: blacklist the current primary and trigger view change
                    }
                    batch_c2s.send(m).unwrap();
                }

                // speculative verification: receive a request, respond with (cid, rid, valid)
                //speculative_smr_to_crypto_sender: Sender<RawMessage>,
                //speculative_crypto_to_smr_receiver: Receiver<(u32, u64, bool)>,
                if let Ok(mut m) = speculative_s2c.try_recv() {
                    assert!(m.message_type() == MessageType::Request);
                    let r = m.message::<Request>();
                    let seqnum = r.seqnum;
                    let sender = r.c;

                    // blacklist: if the client is blacklisted then drop it
                    let blacklisted = {
                        let blacklist = blist_lock.read().unwrap();
                        blacklist.contains(&r.c)
                    };
                    let valid = if blacklisted {
                        //println!("Client {} is blacklisted", r.c);
                        false
                    } else {
                        let valid = crypto.request_signature_is_valid(&mut m);
                        if !valid && BLACKLIST_NODES {
                            {
                                let mut blacklist = blist_lock.write().unwrap();
                                blacklist.insert(sender);
                            }
                            println!("Blacklist client {}", sender);
                        }
                        valid
                    };

                    speculative_c2s.send((sender, seqnum, valid)).unwrap();
                }
            }
        });
    }
}

impl Replica {
    pub fn new(config: &str, f: u32, id: u32, crypto_threads: usize) -> Self {
        let n = if HYBRID_MODE { 2 * f + 1 } else { 3 * f + 1 };
        assert!(id < n, "Invalid replica ID {} < {}", id, n);
        assert!(crypto_threads > 0, "Need at least 1 crypto thread");

        let nodes = parse_configuration_file(config);

        let blacklist_lock = Arc::new(RwLock::new(HashSet::new()));

        let (smr_to_crypto_sender, smr_to_crypto_receiver) = crossbeam_channel::unbounded();
        let (crypto_to_smr_sender, crypto_to_smr_receiver) = crossbeam_channel::unbounded();
        let (crypto_to_net_sender, crypto_to_net_receiver) = crossbeam_channel::unbounded();
        let (net_to_crypto_sender, net_to_crypto_receiver) = crossbeam_channel::unbounded();

        let (batch_smr_to_crypto_sender, batch_smr_to_crypto_receiver) =
            crossbeam_channel::unbounded();
        let (batch_crypto_to_smr_sender, batch_crypto_to_smr_receiver) =
            crossbeam_channel::unbounded();

        let (speculative_smr_to_crypto_sender, speculative_smr_to_crypto_receiver) =
            crossbeam_channel::unbounded();
        let (speculative_crypto_to_smr_sender, speculative_crypto_to_smr_receiver) =
            crossbeam_channel::unbounded();

        create_network_thread(
            id,
            n,
            nodes.clone(),
            net_to_crypto_sender,
            crypto_to_net_receiver,
        );

        create_crypto_threads(
            id,
            nodes.clone(),
            crypto_threads,
            &blacklist_lock,
            (smr_to_crypto_receiver, crypto_to_smr_sender),
            (crypto_to_net_sender, net_to_crypto_receiver),
            (batch_smr_to_crypto_receiver, batch_crypto_to_smr_sender),
            (
                speculative_smr_to_crypto_receiver,
                speculative_crypto_to_smr_sender,
            ),
        );

        let stats = Statistics {
            min_batch_size: u64::MAX,
            max_batch_size: 0,
            total_batch_size: 0,
            nbatches: 0,
        };

        Self {
            n,
            f,
            id,
            seqnum: Cell::new(0),
            next_pp_seqnum: Cell::new(0),
            v: 0,
            primary: 0,
            nodes,
            smr_to_crypto_sender,
            crypto_to_smr_receiver,
            batch_smr_to_crypto_sender,
            batch_crypto_to_smr_receiver,
            speculative_smr_to_crypto_sender,
            speculative_crypto_to_smr_receiver,
            consensus: RefCell::new(BTreeMap::new()),
            pending_req: RefCell::new(VecDeque::new()),
            speculatively_verified: RefCell::new(HashMap::new()),
            blacklist_lock,
            stats: Cell::new(stats),
        }
    }

    pub fn is_primary(&self) -> bool {
        self.id == self.primary
    }

    pub fn my_address(&self) -> &Node {
        &self.nodes[self.id as usize]
    }

    pub fn run_replica(&self, f: &dyn Fn(Vec<u8>) -> Vec<u8>) -> ! {
        loop {
            while !self.crypto_to_smr_receiver.is_empty() {
                if let Ok(m) = self.crypto_to_smr_receiver.recv() {
                    //println!("\nReceived correctly authenticated message {:?}", m);
                    match m.message_type() {
                        MessageType::Request => self.handle_request(f, m),
                        MessageType::PrePrepare => self.handle_preprepare(m),
                        MessageType::Prepare => self.handle_prepare(m),
                        MessageType::Commit => self.handle_commit(m),
                        t => eprintln!(
                            "Replica {} has received message of unknown type {:?}",
                            self.id, t
                        ),
                    }
                }
            }

            self.receive_speculatively_verified_requests();

            self.execute_requests_and_reply(f);

            if self.f > 0 && self.is_primary() {
                self.create_and_send_pp();
            }
        }
    }

    fn send_message(&self, i: u32, m: RawMessage) {
        assert!(m.message_type() != MessageType::Request);

        if m.message_type() == MessageType::Reply {
            dbg_println!(
                "Replica {} is sending to client {}: {:?}",
                self.id,
                i,
                m.message::<Reply>()
            );
        }

        self.smr_to_crypto_sender.send((i, m)).unwrap();
    }

    fn send_message_to_all_replicas(&self, m: &RawMessage) {
        assert!(m.message_type() != MessageType::Request);

        match m.message_type() {
            MessageType::PrePrepare => {
                dbg_println!(
                    "Replica {} is sending to all: {:?}",
                    self.id,
                    m.message::<PrePrepare>()
                );
            }

            MessageType::Prepare => {
                dbg_println!(
                    "Replica {} is sending to all: {:?}",
                    self.id,
                    m.message::<Prepare>()
                );
            }
            MessageType::Commit => {
                dbg_println!(
                    "Replica {} is sending to all: {:?}",
                    self.id,
                    m.message::<Commit>(),
                );
            }
            _ => {
                dbg_println!("Replica {} is sending to all: {:?}", self.id, m,);
            }
        }

        for i in 0..self.n {
            if i != self.id {
                self.smr_to_crypto_sender.send((i, m.clone())).unwrap();
            }
        }
    }

    fn receive_speculatively_verified_requests(&self) {
        if !SPECULATIVE_VERIFICATION {
            return;
        }

        // While there are messages in the queue, receive them and insert into the hashmap of
        // validated requests. A message is (cid, rid, valid?).
        while !self.speculative_crypto_to_smr_receiver.is_empty() {
            if let Ok((cid, rid, valid)) = self.speculative_crypto_to_smr_receiver.recv() {
                self.speculatively_verified
                    .borrow_mut()
                    .insert(cid, (rid, valid));
            }
        }
    }

    fn handle_request(&self, f: &dyn Fn(Vec<u8>) -> Vec<u8>, request: RawMessage) {
        assert!(request.message_type() == MessageType::Request);

        let r = request.message::<Request>();
        dbg_println!("Replica {} has received request {:?}", self.id, r);

        // blacklist: if this client is blacklisted then drop the message early
        {
            let blacklist = self.blacklist_lock.read().unwrap();
            if blacklist.contains(&r.c) {
                return;
            }
        }

        // read-only requests: verify + reply to client immediately
        // no need for speculative execution as there is no consensus
        if r.ro {
            self.batch_smr_to_crypto_sender
                .send(request.clone())
                .unwrap();
            let m = self.batch_crypto_to_smr_receiver.recv().unwrap();
            if !m.inner.is_empty() {
                let reply = self.execute_single_request(f, &request);
                self.send_message(r.c, reply);
            }
            return;
        }

        // The client will retransmit to all and find the correct primary
        if !self.is_primary() {
            // maybe we have a reply? If so then retransmit it
            unimplemented!();
        }

        if SPECULATIVE_VERIFICATION {
            // Speculative verification, send request to queue for verification
            self.speculative_smr_to_crypto_sender
                .send(request.clone())
                .unwrap();
        }

        let mut pending = self.pending_req.borrow_mut();
        pending.push_back(request);
    }

    fn create_and_send_pp(&self) {
        if self.pending_req.borrow().is_empty() {
            return;
        }

        // if too many consensus in progress then forget about creating a new one for now
        if self.consensus.borrow().len() >= MAX_PENDING_CONSENSUS {
            return;
        }

        // TODO: retransmissions
        // We need to record the latest client sequence number received for each
        // client in a hashmap so that we don't accept an old request

        // batching: send a PP for multiple requests
        let mut batch = Vec::new();
        let max_batch_size = PrePrepare::max_payload_max();
        let mut current_batch_size = 0;
        let mut n_reqs = 0;

        /*
        println!(
            "{} pending req, max_batch_size = {}",
            self.pending_req.borrow().len(),
            max_batch_size
        );
        */

        while current_batch_size < max_batch_size && !self.pending_req.borrow().is_empty() {
            let sz = self.pending_req.borrow()[0].message_len();
            /*
            println!(
                "batch has {} reqs, current request of size {}, current_batch_size = {}",
                n_reqs, sz, current_batch_size
            );
            */
            if current_batch_size + sz > max_batch_size {
                break;
            } else {
                batch.push(self.pending_req.borrow_mut().pop_front().unwrap());
                current_batch_size += sz;
                n_reqs += 1;
            }
        }

        /*
        println!(
            "Creating PP of size {} and {} reqs",
            current_batch_size, n_reqs
        );
        */

        let mut stats = self.stats.get();
        stats.min_batch_size = if n_reqs < stats.min_batch_size {
            n_reqs
        } else {
            stats.min_batch_size
        };
        stats.max_batch_size = if n_reqs > stats.max_batch_size {
            n_reqs
        } else {
            stats.max_batch_size
        };
        stats.total_batch_size += n_reqs;
        stats.nbatches += 1;
        self.stats.set(stats);

        if stats.nbatches % 10000 == 0 {
            println!(
                "Stats: {} PP, batch size (min, max, avg) == {}, {}, {}",
                stats.nbatches,
                stats.min_batch_size,
                stats.max_batch_size,
                (stats.total_batch_size as f64) / (stats.nbatches as f64)
            );
        }

        let mut pp = RawMessage::new_preprepare(
            self.v,
            self.next_pp_seqnum.get(),
            self.id,
            current_batch_size,
        );

        // serialize the batch
        let payload = pp.message_payload_mut::<PrePrepare>().unwrap();
        let mut offset = 0;
        for req in &batch {
            let len = req.message_len();
            unsafe {
                let src = req.inner.as_ptr() as *const u8;
                let dst = payload.as_mut_ptr().add(offset);
                std::ptr::copy(src, dst, len);
            }

            //println!("reqlen = {}, offset now {}, req = {:?}", len, offset, req);
            offset += len;
        }

        /*
        println!(
            "{} has created PP with {} requests: {:?}",
            self.id,
            batch.len(),
            pp
        );
        */

        self.send_message_to_all_replicas(&pp);

        // create consensus and update hashmap
        let consensus = Consensus {
            batch,
            pp: Some(pp),
            p: Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f)),
            c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
            rep: None,
        };
        self.consensus
            .borrow_mut()
            .insert(self.next_pp_seqnum.get(), consensus);

        // move to next sequence number
        self.next_pp_seqnum.set(self.next_pp_seqnum.get() + 1);

        /*
        println!(
            "Replica {} moves to next seqnum {}",
            self.id,
            self.next_pp_seqnum.get()
        );
        */
    }

    fn handle_preprepare(&self, m: RawMessage) {
        assert!(!self.is_primary());

        let pp = m.message::<PrePrepare>();
        dbg_println!("Replica {} has received a PP {:?}", self.id, pp);

        let pp_seq_num = pp.seqnum;
        if pp_seq_num < self.seqnum.get() {
            /*
            println!(
                "Replica {} has received a PP from the past: {} < {}",
                self.id, pp_seq_num, self.s
            );
            */
            return;
        }

        // deserialize the batch
        let mut batch = Vec::<RawMessage>::new();
        let payload = m.message_payload::<PrePrepare>().unwrap();
        let mut offset = 0;
        let mut nreqs = 0;
        //println!("The batch is as follows (payload == {:?})", payload);
        while offset < payload.len() {
            //println!("offset = {}, payload.len() = {}", offset, payload.len());
            let len = unsafe {
                let header = &*(payload.as_ptr() as *const MessageHeader) as &MessageHeader;
                std::mem::size_of::<MessageHeader>() + header.len
            };

            //println!("Create a new batch request of size {}", len);
            let mut batch_request = RawMessage::new(len);
            unsafe {
                let src = payload.as_ptr().add(offset);
                let dst = batch_request.inner.as_mut_ptr() as *mut u8;
                std::ptr::copy(src, dst, len);
            }

            //println!("Send request {:?} to crypto threads", batch_request);
            if SPECULATIVE_VERIFICATION {
                // Speculative verification, send request to queue for verification
                self.speculative_smr_to_crypto_sender
                    .send(batch_request.clone())
                    .unwrap();
                batch.push(batch_request);
            } else {
                self.batch_smr_to_crypto_sender.send(batch_request).unwrap();
            }
            offset += len;
            nreqs += 1;
        }

        if !SPECULATIVE_VERIFICATION {
            while nreqs > 0 {
                let batch_request = self.batch_crypto_to_smr_receiver.recv().unwrap();
                //println!("Received request {:?} from crypto threads", batch_request);
                if !batch_request.inner.is_empty() {
                    batch.push(batch_request);
                }
                nreqs -= 1;
            }
        }

        // create request digest, or keep the original request if small enough
        let mut digest = [0; CryptoLayer::digest_length()];
        if pp.payload_len() < CryptoLayer::digest_length() {
            unsafe {
                let src = m.message_payload::<PrePrepare>().unwrap().as_ptr();
                let dst = digest.as_mut_ptr() as *mut u8;
                std::ptr::copy(src, dst, pp.payload_len());
            }
        } else {
            digest = CryptoLayer::digest_request_batch(m.message_payload::<PrePrepare>().unwrap());
        }

        // create <P, v, n, r, req_digest>_mac and send it to all replicas
        let p = RawMessage::new_prepare(
            self.v,
            pp_seq_num,
            self.id,
            digest[..CryptoLayer::digest_length()]
                .try_into()
                .expect("wrong size"),
        );
        self.send_message_to_all_replicas(&p);

        // create consensus and update hashmap
        // maybe we already received a P and already have a consensus
        {
            let mut borrowed_consensus = self.consensus.borrow_mut();
            let consensus = borrowed_consensus.entry(pp_seq_num).or_insert_with(|| {
                /*
                println!(
                    "Create a new consensus for {}; adding the P {:?}",
                    pp_seq_num, p
                );
                */

                Consensus {
                    batch: vec![],
                    pp: None,
                    p: Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f)),
                    c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
                    rep: None,
                }
            });
            consensus.batch = batch;
            consensus.pp = Some(m);
        }

        // need to be here so we don't call it while the consensus is borrowed as mutable
        self.handle_prepare(p);
    }

    fn handle_prepare(&self, m: RawMessage) {
        let p = m.message::<Prepare>();

        dbg_println!("Replica {} has received a P {:?}", self.id, p);

        let prepare_seq_num = p.seqnum;
        if prepare_seq_num < self.seqnum.get() {
            /*
            println!(
            "Replica {} has received a P from the past: {} < {}",
            self.id, prepare_seq_num, self.s
            );
            */
            return;
        }

        let mut consensus_prepare_is_already_complete = false;
        let mut consensus_prepare_is_complete = false;

        // there might not be a consensus yet because we didn't receive the PP yet, but we
        // still need to keep the prepare
        // the new scope is here to ensure the borrows ends before the call to handle_commit()
        {
            let mut borrowed_consensus = self.consensus.borrow_mut();
            let consensus = borrowed_consensus
                .entry(prepare_seq_num)
                .or_insert_with(|| {
                    assert!(!self.is_primary());

                    /*
                    println!(
                        "Replica {} cannot find consensus for P {}",
                        self.id, prepare_seq_num
                    );
                    */

                    // add the consensus
                    Consensus {
                        batch: vec![],
                        pp: None,
                        p: Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f)),
                        c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
                        rep: None,
                    }
                });

            if consensus.pp.is_none()
                || (consensus.pp.as_ref().unwrap().message::<PrePrepare>().v == p.v
                    && consensus
                        .pp
                        .as_ref()
                        .unwrap()
                        .message::<PrePrepare>()
                        .seqnum
                        == prepare_seq_num)
            {
                consensus_prepare_is_already_complete = consensus.p.is_complete();
                consensus.p.add(p.r, m);
                consensus_prepare_is_complete = consensus.p.is_complete();

                /*
                println!(
                    "Consensus add P {:?}, complete? {}",
                    m, consensus_prepare_is_complete
                );
                */
            } else {
                println!(
                    "Replica {} has a consensus for {} but the PP doesn't match: {:?} != {:?}",
                    self.id, p.seqnum, consensus.pp, p
                );
            }
        }

        // if the quorum of prepare was already complete then we have already sent a commit,
        // so no need to do it again.
        if !consensus_prepare_is_already_complete && consensus_prepare_is_complete {
            // create <C, v, n, r, req_digest>_mac and send it to all replicas
            let c = RawMessage::new_commit(self.v, prepare_seq_num, self.id);
            self.send_message_to_all_replicas(&c);

            //consensus.c.add(self.id, c);
            self.handle_commit(c);
        }
    }

    fn handle_commit(&self, m: RawMessage) {
        let c = m.message::<Commit>();

        dbg_println!("Replica {} has received a C {:?}", self.id, c);

        if c.seqnum < self.seqnum.get() {
            /*
            println!(
                "Replica {} has received a C from the past: {} < {}",
                self.id,
                c.seqnum,
                self.seqnum.get()
            );
            */
            return;
        }

        // there might not be a consensus yet because we didn't receive the PP yet, but we
        // still need to keep the commit
        if !self.is_primary() && self.consensus.borrow_mut().get_mut(&c.seqnum).is_none() {
            // println!("Replica {} cannot find consensus for P {}", self.id, p.n);
            // add the consensus
            let consensus = Consensus {
                batch: vec![],
                pp: None,
                p: Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f)),
                c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
                rep: None,
            };
            self.consensus.borrow_mut().insert(c.seqnum, consensus);
        }

        self.consensus
            .borrow_mut()
            .get_mut(&c.seqnum)
            .unwrap()
            .c
            .add(c.r, m.clone());

        /*
        let consensus_commit_is_complete = self
            .consensus
            .borrow()
            .get(&c.seqnum)
            .unwrap()
            .c
            .is_complete();
        println!(
            "Consensus add C {:?}, complete? {}",
            m, consensus_commit_is_complete
        );
        */
    }

    fn execute_single_request(
        &self,
        f: &dyn Fn(Vec<u8>) -> Vec<u8>,
        request: &RawMessage,
    ) -> RawMessage {
        //println!(
        //    "Single-mode: Replica {} has received request {:?}",
        //    self.id, request
        //);

        let payload = f(request
            .message_payload::<Request>()
            .unwrap_or(&Vec::new())
            .to_vec());

        let mut reply = RawMessage::new_reply(
            self.id,
            self.v,
            request.message::<Request>().seqnum,
            payload.len(),
        );

        if !payload.is_empty() {
            let _ = reply
                .message_payload_mut::<Reply>()
                .unwrap()
                .write(&payload);
        }
        reply
    }

    fn execute_requests_and_reply(&self, f: &dyn Fn(Vec<u8>) -> Vec<u8>) {
        if self.f == 0 {
            for request in self.pending_req.borrow().iter() {
                let reply = self.execute_single_request(f, request);
                self.send_message(request.message::<Request>().c, reply);
            }
            self.pending_req.borrow_mut().clear();
            return;
        }

        // execute all the consensus that we can execute
        for (consensus_num, consensus) in self.consensus.borrow().iter() {
            // check if the consensus has been done and the reply has not been executed yet
            if *consensus_num == self.seqnum.get()
                && consensus.rep.is_none()
                && !consensus.batch.is_empty()
                && consensus.pp.is_some()
                && consensus.p.is_complete()
                && consensus.c.is_complete()
            {
                let can_execute = if SPECULATIVE_VERIFICATION {
                    let mut can_execute = true;
                    for request in consensus.batch.iter() {
                        let r = request.message::<Request>();
                        if let Some((s, _)) = self.speculatively_verified.borrow().get(&r.c) {
                            if *s != r.seqnum {
                                /*
                                println!(
                                    "Request ({}, {}) not verified yet (seqnum = {})",
                                    r.c, r.seqnum, *s
                                );
                                */
                                can_execute = false;
                                break;
                            }
                        } else {
                            // println!("Request ({}, {}) not verified yet", r.c, r.seqnum);
                            can_execute = false;
                            break;
                        }
                    }
                    can_execute
                } else {
                    true
                };

                if can_execute {
                    /*
                    println!(
                        "Replica {} executes {} requests for consensus {}; last exec is {}",
                        self.id,
                        consensus.batch.len(),
                        consensus_num,
                        self.seqnum.get()
                    );
                    */

                    for request in consensus.batch.iter() {
                        let r = request.message::<Request>();
                        if !SPECULATIVE_VERIFICATION
                            || self.speculatively_verified.borrow()[&r.c] == (r.seqnum, true)
                        {
                            let reply = self.execute_single_request(f, request);
                            self.send_message(request.message::<Request>().c, reply);
                        } else {
                            /*
                            println!(
                                "Execute request ({}, {}): speculative verification == {},\
                                entry = {:?}",
                                r.c,
                                r.seqnum,
                                SPECULATIVE_VERIFICATION,
                                self.speculatively_verified.borrow().get(&r.c)
                            );
                            */
                        }
                    }

                    //FIXME we never save the reply
                    //consensus.rep = Some(rep);
                    self.seqnum.set(consensus_num + 1);
                }
            }
        }

        // remove the old consensus
        // FIXME This might be a problem once we implement retransmissions
        self.consensus
            .borrow_mut()
            .retain(|&k, _| k >= self.seqnum.get());
    }
}
