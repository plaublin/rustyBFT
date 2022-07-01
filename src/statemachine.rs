use crate::configuration::*;
use crate::crypto::CryptoLayer;
use crate::message::*;
use crate::network::NetworkLayer;
use crate::quorum::Quorum;
use crate::udpnetwork::UDPNetwork;
use crossbeam_channel::{Receiver, Sender};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::Write;
use std::rc::Rc;
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
        let n = 3 * f + 1;
        assert!(id >= n, "Invalid client id {} >= {}", id, n);

        let nodes = parse_configuration_file(config);
        let crypto = CryptoLayer::new(id, &nodes);
        let network = UDPNetwork::new(id, false, &nodes);

        Self {
            n,
            f,
            id,
            seqnum: Cell::new(0),
            v: Cell::new(0),
            primary: 0,
            nodes,
            network: Box::new(network),
            crypto,
        }
    }

    pub fn my_address(&self) -> &Node {
        &self.nodes[self.id as usize]
    }

    pub fn create_request(&self, reqlen: usize) -> RawMessage {
        let mut request = RawMessage::new_request(self.id, self.seqnum.get(), reqlen);
        self.crypto.sign_request(&mut request);
        self.crypto.authenticate_message(self.primary, &mut request);
        request
    }

    fn send_request(&self, req: &RawMessage) {
        self.network.send(self.primary, req);
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
        //println!("Sending request (len {}) {:?}", req.message_len(), req);
        self.send_request(req);

        let mut start = time::Instant::now();

        let mut q = Quorum::new(self.n as usize, (self.f + 1) as usize);
        while !q.is_complete() {
            let ret = self.accept_reply();
            match ret {
                Some(reply) => {
                    let rep = reply.message::<Reply>();
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
    consensus: RefCell<BTreeMap<u64, Consensus>>, // the current consensus
    pending_req: RefCell<VecDeque<RawMessage>>,   // for batching
}

fn create_network_thread(
    id: u32,
    nodes: Vec<Node>,
    net_to_crypto: Sender<RawMessage>,
    crypto_to_net: Receiver<(u32, RawMessage)>,
) {
    let _ = thread::spawn(move || {
        println!("Starting the network thread");
        let network = UDPNetwork::new(id, true, &nodes);

        loop {
            while let Ok((i, m)) = crypto_to_net.try_recv() {
                //println!("net sends {:?} to {}", m, i);
                network.send(i, &m);
            }

            if let Ok(m) = network.receive() {
                //println!("net has received {:?} and sends it to crypto", m);
                net_to_crypto.send(m).unwrap();
            }
        }
    });
}

fn create_crypto_threads(
    id: u32,
    nodes: Vec<Node>,
    nthreads: usize,
    smr_to_crypto: Receiver<(u32, RawMessage)>,
    crypto_to_smr: Sender<RawMessage>,
    crypto_to_net: Sender<(u32, RawMessage)>,
    net_to_crypto: Receiver<RawMessage>,
) {
    for t in 0..nthreads {
        let n = nodes.clone();
        let s2c = smr_to_crypto.clone();
        let c2s = crypto_to_smr.clone();
        let c2n = crypto_to_net.clone();
        let n2c = net_to_crypto.clone();

        let _ = thread::spawn(move || {
            println!("Starting crypto thread {}/{}", t, nthreads);
            let crypto = CryptoLayer::new(id, &n);

            // let's reuse the same for every batched request
            let mut batch_request = RawMessage::default();

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
                    let mut valid = crypto.message_authentication_is_valid(&mut m);
                    if valid && m.message_type() == MessageType::Request {
                        valid = crypto.request_signature_is_valid(&mut m)
                    } else if valid && m.message_type() == MessageType::PrePrepare {
                        // deserialize the batch to check requests signature
                        let payload = m.message_payload_mut::<PrePrepare>().unwrap();
                        let mut offset = 0;
                        //println!("I have received a PP with payload {:?}", payload);
                        while offset < payload.len() {
                            //println!("offset = {}, payload.len() = {}", offset, payload.len());
                            let len = unsafe {
                                let header =
                                    &*(payload.as_ptr() as *const MessageHeader) as &MessageHeader;
                                std::mem::size_of::<MessageHeader>() + header.len
                            };

                            unsafe {
                                let src = payload.as_ptr().add(offset);
                                let dst = batch_request.inner.as_mut_ptr() as *mut u8;
                                std::ptr::copy(src, dst, len);
                            }

                            valid &= crypto.request_signature_is_valid(&mut batch_request);
                            offset += len;
                        }
                    }

                    if valid {
                        //println!("{} sends {:?} to smr", t, m);
                        c2s.send(m).unwrap();
                    }
                }
            }
        });
    }
}

impl Replica {
    pub fn new(config: &str, f: u32, id: u32, crypto_threads: usize) -> Self {
        let n = 3 * f + 1;
        assert!(id < n, "Invalid replica ID {} < {}", id, n);
        assert!(crypto_threads > 0, "Need at least one crypto thread");

        let nodes = parse_configuration_file(config);

        let (smr_to_crypto_sender, smr_to_crypto_receiver) = crossbeam_channel::unbounded();
        let (crypto_to_smr_sender, crypto_to_smr_receiver) = crossbeam_channel::unbounded();
        let (crypto_to_net_sender, crypto_to_net_receiver) = crossbeam_channel::unbounded();
        let (net_to_crypto_sender, net_to_crypto_receiver) = crossbeam_channel::unbounded();

        create_network_thread(
            id,
            nodes.clone(),
            net_to_crypto_sender,
            crypto_to_net_receiver,
        );
        create_crypto_threads(
            id,
            nodes.clone(),
            crypto_threads,
            smr_to_crypto_receiver,
            crypto_to_smr_sender,
            crypto_to_net_sender,
            net_to_crypto_receiver,
        );

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
            consensus: RefCell::new(BTreeMap::new()),
            pending_req: RefCell::new(VecDeque::new()),
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
            let m = self.crypto_to_smr_receiver.recv().unwrap();
            //println!("\nReceived correctly authenticated message {:?}", m);
            match m.message_type() {
                MessageType::Request => self.handle_request(m),
                MessageType::PrePrepare => self.handle_preprepare(m),
                MessageType::Prepare => self.handle_prepare(m),
                MessageType::Commit => self.handle_commit(m),
                t => eprintln!(
                    "Replica {} has received message of unknown type {:?}",
                    self.id, t
                ),
            }

            self.execute_requests_and_reply(f);

            if self.f > 0 && self.is_primary() {
                self.create_and_send_pp();
            }
        }
    }

    fn send_message(&self, i: u32, m: RawMessage) {
        assert!(m.message_type() != MessageType::Request);
        self.smr_to_crypto_sender.send((i, m)).unwrap();
    }

    fn send_message_to_all_replicas(&self, m: &RawMessage) {
        assert!(m.message_type() != MessageType::Request);
        for i in 0..self.n {
            if i != self.id {
                self.smr_to_crypto_sender.send((i, m.clone())).unwrap();
            }
        }
    }

    fn handle_request(&self, request: RawMessage) {
        assert!(request.message_type() == MessageType::Request);

        //println!("Replica {} has received request {:?}", self.id, request);

        // The client will retransmit to all and find the correct primary
        if !self.is_primary() {
            // maybe we have a reply? If so then retransmit it
            unimplemented!();
        }

        let mut pending = self.pending_req.borrow_mut();
        pending.push_back(request);
    }

    fn create_and_send_pp(&self) {
        if self.pending_req.borrow().is_empty() {
            return;
        }

        // if too many consensus in progress then forget about creating a new one for now
        if self.consensus.borrow().len() > MAX_PENDING_CONSENSUS {
            return;
        }

        // TODO: retransmissions
        // We need to record the latest client sequence number received for each
        // client in a hashmap so that we don't accept an old request

        // batching: send a PP for multiple requests
        let mut batch = Vec::new();
        let max_batch_size = PrePrepare::max_payload_max();
        let mut current_batch_size = 0;
        while current_batch_size < max_batch_size && !self.pending_req.borrow().is_empty() {
            let sz = self.pending_req.borrow()[0].message_len();
            if current_batch_size + sz > max_batch_size {
                break;
            } else {
                batch.push(self.pending_req.borrow_mut().pop_front().unwrap());
                current_batch_size += sz;
            }
        }

        //println!("Creating PP of size {}", current_batch_size);

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
        let pp = m.message::<PrePrepare>();
        //println!("Replica {} has received a PP {:?}", self.id, pp);

        if self.is_primary() {
            return;
        }

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
            batch.push(batch_request);
            offset += len;
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

        // create consensus and updathashmap
        // maybe we already received a P and already have a consensus
        let mut need_to_handle_prepare = false;
        {
            let mut borrowed_consensus = self.consensus.borrow_mut();
            if let Some(consensus) = borrowed_consensus.get_mut(&pp_seq_num) {
                //FIXME ideally we would need to check that the existing consensus is compatible with
                //this pre-prepare
                /*
                println!(
                    "I already have a consensus for {}; adding the P {:?}",
                    pp_seq_num, p
                );
                */
                consensus.batch = batch;
                consensus.pp = Some(m);
                // consensus.p.add(self.id, p); // done by the call below
                need_to_handle_prepare = true;
            } else {
                /*
                println!(
                    "Create a new consensus for {}; adding the P {:?}",
                    pp_seq_num, p
                );
                */

                let mut pq = Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f));
                pq.add(self.id, p.clone());
                let consensus = Consensus {
                    batch,
                    pp: Some(m),
                    p: pq,
                    c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
                    rep: None,
                };
                borrowed_consensus.insert(pp_seq_num, consensus);
            }
        }

        // need to be here so we don't call it while the consensus is borrowed as mutable
        if need_to_handle_prepare {
            self.handle_prepare(p);
        }
    }

    fn handle_prepare(&self, m: RawMessage) {
        let p = m.message::<Prepare>();

        //println!("Replica {} has received a P {:?}", self.id, p);

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

        // there might not be a consensus yet because we didn't receive the PP yet, but we
        // still need to keep the prepare
        // we have a new scope to ensure the borrows is as short as possible
        let no_consensus = { self.consensus.borrow().get(&prepare_seq_num).is_none() };
        if !self.is_primary() && no_consensus {
            /*
            println!(
                "Replica {} cannot find consensus for P {}",
                self.id, prepare_seq_num
            );
            */

            // add the consensus
            let consensus = Consensus {
                batch: vec![],
                pp: None,
                p: Quorum::new(self.n as usize, Consensus::prepare_quorum_size(self.f)),
                c: Quorum::new(self.n as usize, Consensus::commit_quorum_size(self.f)),
                rep: None,
            };
            self.consensus
                .borrow_mut()
                .insert(prepare_seq_num, consensus);
        }

        let mut consensus_prepare_is_complete = false;

        // this new scope is necessary to ensure the current mut borrow finishes before we borrow_mut again in
        // handle_commit
        {
            if let Some(consensus) = self.consensus.borrow_mut().get_mut(&prepare_seq_num) {
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
                    if consensus.p.is_complete() {
                        // We have already sent a commit
                        return;
                    }

                    consensus.p.add(p.r, m);
                    consensus_prepare_is_complete = consensus.p.is_complete();
                    /*
                    if !consensus_prepare_is_complete {
                        println!(
                            "Replica {}, P consensus not complete yet: {:?}",
                            self.id, consensus.p
                        );
                    }
                    */
                } else {
                    println!(
                        "Replica {} has a consensus for {} but the PP doesn't match: {:?} != {:?}",
                        self.id, p.seqnum, consensus.pp, p
                    );
                }
            }

            if consensus_prepare_is_complete {
                // create <C, v, n, r, req_digest>_mac and send it to all replicas
                let c = RawMessage::new_commit(self.v, prepare_seq_num, self.id);
                self.send_message_to_all_replicas(&c);

                //consensus.c.add(self.id, c);
                self.handle_commit(c);
            }
        }
    }

    fn handle_commit(&self, m: RawMessage) {
        let c = m.message::<Commit>();

        //println!("Replica {} has received a C {:?}", self.id, c);

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
                p: Quorum::new(self.n as usize, 2 * self.f as usize),
                c: Quorum::new(self.n as usize, (2 * self.f + 1) as usize),
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
                /*
                println!(
                    "Replica {} executes request for consensus {}; last exec is {}",
                    self.id,
                    consensus_num,
                    self.seqnum.get()
                );
                println!(
                    "Replica {} executes {} requests in consensus {}",
                    self.id,
                    consensus.batch.len(),
                    consensus_num
                );
                */

                for request in consensus.batch.iter() {
                    let reply = self.execute_single_request(f, request);
                    self.send_message(request.message::<Request>().c, reply);
                }

                //FIXME we never save the reply
                //consensus.rep = Some(rep);
                self.seqnum.set(consensus_num + 1);
            }
        }

        // remove the old consensus
        // FIXME This might be a problem once we implement retransmissions
        self.consensus
            .borrow_mut()
            .retain(|&k, _| k >= self.seqnum.get());
    }
}