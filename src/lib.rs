// The rusty_bft library
// Core functionalities of the SMR

mod communication;
mod cryptografer;
mod quorum;

use communication::*;
use cryptografer::*;
use quorum::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::time;

#[derive(Debug)]
pub struct Node {
    pub id: u32,
    pub ip: String,
    pub port: u16,
    pub crypto: Cryptografer,
}

pub struct StateMachine {
    pub n: u32,
    pub f: u32,
    pub id: u32, // this node's id; can be a client (id >= n) or a replica (id < n)
    pub s: u64,  // for clients: request sequence number; for replicas: PP sequence number
    pub next_pp_seqnum: u64, // next PP sequence number
    v: u64,      // view number
    primary: u32, // who is the current primary
    nodes: Vec<Node>,
    network: Network,
    consensus: BTreeMap<u64, Consensus>, // the current consensus
    pending_req: Vec<Request>,           // for batching
}

const CLIENT_TIMEOUT: time::Duration = time::Duration::from_millis(500); // in miliseconds

impl StateMachine {
    pub fn parse(config: &str, f: u32, id: u32) -> Self {
        let n = 3 * f + 1;
        let mut nodes = Vec::new();

        let mut i = 0;
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

            let port = split
                .next()
                .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
                .parse::<u16>()
                .expect("Invalid port number");

            let hmac = split
                .next()
                .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
                .to_string();

            let key = split
                .next()
                .unwrap_or_else(|| panic!("Malformed line \"{}\"", line))
                .to_string();

            let crypto = Cryptografer::new(hmac, key);

            nodes.push(Node {
                id: i,
                ip,
                port,
                crypto,
            });
            i += 1;
        }

        // check that f and id are valid, i.e., there are enough entries in the config
        assert!(
            nodes.len() >= n.try_into().unwrap(),
            "Not enough replicas in the config file"
        );
        assert!(
            nodes.len() >= id.try_into().unwrap(),
            "Not enough entries in the config file for id {}",
            id
        );

        // The primary is in non-blocking mode for request batching
        // Clients are in non-blocking mode to transmit a new request if the current one failed
        let nb = id == 0 || id >= n;
        let network = Network::create_network(
            format!("{}:{}", nodes[id as usize].ip, nodes[id as usize].port),
            nb,
        );

        let consensus = BTreeMap::new();

        StateMachine {
            n,
            f,
            id,
            s: 0,
            v: 0,
            primary: 0,
            next_pp_seqnum: 0,
            nodes,
            network,
            consensus,
            pending_req: vec![],
        }
    }

    pub fn is_primary(&self) -> bool {
        self.id == 0
    }

    pub fn is_replica(&self) -> bool {
        self.id < self.n
    }

    pub fn is_client(&self) -> bool {
        self.id >= self.n
    }

    pub fn get_node(&self, id: usize) -> &Node {
        &self.nodes[id]
    }

    pub fn create_request(&self, reqlen: usize) -> Request {
        let mut r = Request {
            s: self.s,
            c: self.id,
            o: vec![0; reqlen],
            sign: vec![],
        };
        r.sign = self
            .get_node(self.id as usize)
            .crypto
            .gen_sign(&rmp_serde::to_vec(&r).unwrap()[..]);
        r
    }

    pub fn create_reply(&self, s: u64, u: Vec<u8>) -> Reply {
        Reply {
            v: self.v,
            s,
            r: self.id,
            u,
        }
    }

    fn create_authenticated_raw_message<'a>(
        &self,
        d: u32,
        t: RawMessageType,
        s: u32,
        inner: &'a [u8],
    ) -> RawMessage<'a> {
        let mut m = RawMessage {
            t,
            s,
            inner,
            hmac: vec![],
        };
        m.hmac = self
            .get_node(d as usize)
            .crypto
            .gen_hmac(&rmp_serde::to_vec(&m).unwrap()[..]);
        m
    }

    pub fn send_request(&self, req: &Request) {
        let addr = format!(
            "{}:{}",
            self.get_node(self.primary as usize).ip,
            self.get_node(self.primary as usize).port
        );

        let inner = rmp_serde::to_vec(req).unwrap();
        let m = self.create_authenticated_raw_message(
            self.primary,
            RawMessageType::Request,
            self.id,
            &inner,
        );

        self.network.send_raw_message(addr, &m);
    }

    pub fn send_reply(&self, c: u32, rep: &Reply) {
        let addr = format!(
            "{}:{}",
            self.get_node(c as usize).ip,
            self.get_node(c as usize).port
        );

        // create a reply
        let inner = rmp_serde::to_vec(rep).unwrap();
        let m = self.create_authenticated_raw_message(c, RawMessageType::Reply, self.id, &inner);
        self.network.send_raw_message(addr, &m);
    }

    fn send_to_all_replicas(&self, w: MessageWrapper) {
        //println!("Replica {} is sending a {:?}", self.id, w);

        let (inner, t) = match w {
            MessageWrapper::WrapPrePrepare(pp) => {
                (rmp_serde::to_vec(&pp).unwrap(), RawMessageType::PrePrepare)
            }
            MessageWrapper::WrapPrepare(p) => {
                (rmp_serde::to_vec(&p).unwrap(), RawMessageType::Prepare)
            }
            MessageWrapper::WrapCommit(c) => {
                (rmp_serde::to_vec(&c).unwrap(), RawMessageType::Commit)
            }
            _ => {
                println!("Don't know how to send message {:?} to all replicas", w);
                return;
            }
        };

        for i in 0..self.n {
            if i != self.id {
                let addr = format!(
                    "{}:{}",
                    self.get_node(i as usize).ip,
                    self.get_node(i as usize).port
                );

                let m = self.create_authenticated_raw_message(i, t, self.id, &inner);
                self.network.send_raw_message(addr, &m);
            }
        }
    }

    fn receive_authenticated_raw_message<'a>(&self, buf: &'a mut [u8]) -> Option<RawMessage<'a>> {
        let m = self.network.receive_raw_message(buf);
        if let Some(mut m) = m {
            let hmac = m.hmac.clone();
            m.hmac = vec![];

            if self
                .get_node(self.id as usize)
                .crypto
                .verify_hmac(&rmp_serde::to_vec(&m).unwrap()[..], &hmac)
            {
                m.hmac = hmac;
                Some(m)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn receive_reply(&self) -> Option<Reply> {
        let mut buf = [0; Network::MAX_MESSAGE_LEN];
        if let Some(m) = self.receive_authenticated_raw_message(&mut buf) {
            if m.t == RawMessageType::Reply {
                Some(rmp_serde::from_slice::<Reply>(m.inner).unwrap())
            } else {
                println!("Received something that is not a reply: {:?}", m);
                None
            }
        } else {
            // reply authentication failed or timeout
            None
        }
    }

    pub fn accept_reply(self: &mut StateMachine) -> Option<Reply> {
        let start = time::Instant::now();

        let mut q = quorum::Quorum::new(self.n as usize, (2 * self.f + 1) as usize);
        while !q.is_complete() {
            if let Some(r) = self.receive_reply() {
                if r.s == self.s {
                    q.add(r.r, r.clone());
                } else {
                    // println!("Reply {:?} is too old: current seq num = {}", r, self.s);
                }
            } else if start.elapsed() >= CLIENT_TIMEOUT {
                //println!("Client {} timeouts!", self.id);
                self.s += 1; // move to next sequence number
                return None;
            }
        }

        self.s += 1; // move to next sequence number

        Some(q.value())
    }

    fn replica_receive_message(&self) -> MessageWrapper {
        let mut buf = [0; Network::MAX_MESSAGE_LEN];
        if let Some(m) = self.receive_authenticated_raw_message(&mut buf) {
            match m.t {
                RawMessageType::Request => {
                    MessageWrapper::WrapRequest(rmp_serde::from_slice::<Request>(m.inner).unwrap())
                }
                RawMessageType::Reply => {
                    MessageWrapper::WrapReply(rmp_serde::from_slice::<Reply>(m.inner).unwrap())
                }
                RawMessageType::PrePrepare => MessageWrapper::WrapPrePrepare(
                    rmp_serde::from_slice::<PrePrepare>(m.inner).unwrap(),
                ),
                RawMessageType::Prepare => {
                    MessageWrapper::WrapPrepare(rmp_serde::from_slice::<Prepare>(m.inner).unwrap())
                }
                RawMessageType::Commit => {
                    MessageWrapper::WrapCommit(rmp_serde::from_slice::<Commit>(m.inner).unwrap())
                }
            }
        } else {
            MessageWrapper::Error("Couldn't get authenticated raw message".to_string())
        }
    }

    pub fn verify_request_signature(&self, req: &mut Request) -> bool {
        let sign = req.sign.clone();
        req.sign = vec![];

        let v = self
            .get_node(req.c as usize)
            .crypto
            .verify_sign(&rmp_serde::to_vec(req).unwrap()[..], &sign);

        req.sign = sign;
        v
    }

    pub fn run_replica(&mut self, f: &dyn Fn(Vec<u8>) -> Vec<u8>) {
        let mut i: u64 = 0;
        loop {
            let m = self.replica_receive_message();
            match m {
                MessageWrapper::WrapRequest(r) => self.process_request(r),
                MessageWrapper::WrapPrePrepare(pp) => self.process_preprepare(pp),
                MessageWrapper::WrapPrepare(p) => self.process_prepare(p),
                MessageWrapper::WrapCommit(p) => self.process_commit(p),
                _ => (),
            }

            if self.f != 0 {
                self.execute_requests_and_reply(f);
            }

            i += 1;
            if i % 2 == 0 && self.is_primary() {
                self.create_and_send_pp();
            }
        }
    }

    fn create_and_send_pp(&mut self) {
        if self.pending_req.is_empty() {
            return;
        }

        // if too many consensus in progress then forget about creating a new one for now
        if self.consensus.len() > MAX_PENDING_CONSENSUS {
            return;
        }

        // TODO: retransmissions
        // We need to record the latest client sequence number received for each
        // client in a hashmap so that we don't accept an old request

        // batching: send a PP for multiple requests
        let max_batch_size =
            Network::MAX_MESSAGE_LEN - 128 /* Serde overhead */ - Cryptografer::digest_len();
        let mut cur_batch_size = 0;
        let mut n_reqs = 0;
        for req in self.pending_req.iter() {
            let reqlen = 32 /* Serde overhead */ + Cryptografer::sign_len() + req.o.len();
            if cur_batch_size + reqlen > max_batch_size {
                break;
            }
            cur_batch_size += reqlen;
            n_reqs += 1;
        }
        let batch = &self.pending_req[..n_reqs];
        let rawbatch = rmp_serde::to_vec(&batch).unwrap();

        /*
        println!(
            "Replica {} creates PP {} with {} requests",
            self.id,
            self.next_pp_seqnum,
            batch.len()
        );
        */

        // create <PP, v, n, <req>_sign>_mac and send it to all replicas
        let pp = PrePrepare {
            v: self.v,
            n: self.next_pp_seqnum,
            r: rawbatch,
        };

        self.send_to_all_replicas(MessageWrapper::WrapPrePrepare(pp.clone()));

        // create consensus and update hashmap
        let consensus = Consensus {
            batch: batch.to_vec(),
            pp: Some(pp),
            p: Quorum::new(self.n as usize, 2 * self.f as usize),
            c: Quorum::new(self.n as usize, (2 * self.f + 1) as usize),
            rep: None,
        };
        self.consensus.insert(self.next_pp_seqnum, consensus);

        self.next_pp_seqnum += 1; // move to next sequence number

        let _ = self.pending_req.drain(..n_reqs).collect::<Vec<Request>>();

        /*
        println!(
            "Replica {} moves to next seqnum {}",
            self.id, self.next_pp_seqnum
        );
        */
    }

    fn process_request(&mut self, mut req: Request) {
        //println!("Replica {} has received request {:?}", self.id, req);

        // The client will retransmit to all and find the correct primary
        if !self.is_primary() {
            // maybe we have a reply? If so then retransmit it
            unimplemented!();
        }

        if !self.verify_request_signature(&mut req) {
            println!("Request signature is invalid: {:?}", req);
            return;
        }

        self.pending_req.push(req.clone());
    }

    fn process_preprepare(&mut self, pp: PrePrepare) {
        //println!("Replica {} has received a PP {:?}", self.id, pp);

        if self.is_primary() {
            return;
        }

        let pp_seq_num = pp.n;
        if pp_seq_num < self.s {
            /*
            println!(
                "Replica {} has received a PP from the past: {} < {}",
                self.id, pp_seq_num, self.s
            );
            */
            return;
        }

        let mut batch = rmp_serde::from_slice::<Vec<Request>>(&pp.r).unwrap();
        for req in batch.iter_mut() {
            //verify the signature
            if !self.verify_request_signature(req) {
                println!("Request signature is invalid: {:?}", req);
                return;
            }
        }

        // create request digest, or keep the original request if small enough
        let d = if pp.r.len() < Cryptografer::digest_len() {
            pp.r.clone()
        } else {
            Cryptografer::create_digest(&pp.r)
        };

        // create <P, v, n, r, req_digest>_mac and send it to all replicas
        let p = Prepare {
            v: self.v,
            n: pp_seq_num,
            r: self.id,
            d,
        };

        self.send_to_all_replicas(MessageWrapper::WrapPrepare(p.clone()));

        // create consensus and update hashmap
        // maybe we already received a P and already have a consensus
        if let Some(consensus) = self.consensus.get_mut(&pp_seq_num) {
            //FIXME ideally we would need to check that the existing consensus is compatible with
            //this pre-prepare
            /*
            println!(
                "I already have a consensus for {}; adding the P {:?}",
                pp_seq_num, p
            );
            */
            consensus.batch = batch;
            consensus.pp = Some(pp);
            // consensus.p.add(self.id, p); // done by the call below
            self.process_prepare(p);
        } else {
            /*
            println!(
                "Create a new consensus for {}; adding the P {:?}",
                pp_seq_num, p
            );
            */

            let mut pq = Quorum::new(self.n as usize, 2 * self.f as usize);
            pq.add(self.id, p);
            let consensus = Consensus {
                batch,
                pp: Some(pp),
                p: pq,
                c: Quorum::new(self.n as usize, (2 * self.f + 1) as usize),
                rep: None,
            };
            self.consensus.insert(pp_seq_num, consensus);
        }
    }

    fn process_prepare(&mut self, p: Prepare) {
        //println!("Replica {} has received a P {:?}", self.id, p);

        let prepare_seq_num = p.n;
        if prepare_seq_num < self.s {
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
        if !self.is_primary() && self.consensus.get_mut(&prepare_seq_num).is_none() {
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
                p: Quorum::new(self.n as usize, 2 * self.f as usize),
                c: Quorum::new(self.n as usize, (2 * self.f + 1) as usize),
                rep: None,
            };
            self.consensus.insert(prepare_seq_num, consensus);
        }

        if let Some(consensus) = self.consensus.get_mut(&prepare_seq_num) {
            if consensus.pp.is_none()
                || (consensus.pp.as_ref().unwrap().v == p.v
                    && consensus.pp.as_ref().unwrap().n == prepare_seq_num)
            {
                if consensus.p.is_complete() {
                    // We have already sent a commit
                    return;
                }

                consensus.p.add(p.r, p);
                if consensus.p.is_complete() {
                    // create <C, v, n, r, req_digest>_mac and send it to all replicas
                    let c = Commit {
                        v: self.v,
                        n: prepare_seq_num,
                        r: self.id,
                    };

                    self.send_to_all_replicas(MessageWrapper::WrapCommit(c.clone()));

                    //consensus.c.add(self.id, c);
                    self.process_commit(c);
                } else {
                    /*
                    println!(
                        "Replica {}, P consensus not complete yet: {:?}",
                        self.id, consensus.p
                    );
                    */
                }
            } else {
                println!(
                    "Replica {} has a consensus for {} but the PP doesn't match: {:?} != {:?}",
                    self.id, p.n, consensus.pp, p
                );
            }
        }
    }

    fn process_commit(&mut self, c: Commit) {
        //println!("Replica {} has received a C {:?}", self.id, c);

        if c.n < self.s {
            /*
            println!(
                "Replica {} has received a C from the past: {} < {}",
                self.id, c.n, self.s
            );
            */
            return;
        }

        // there might not be a consensus yet because we didn't receive the PP yet, but we
        // still need to keep the commit
        if !self.is_primary() && self.consensus.get_mut(&c.n).is_none() {
            // println!("Replica {} cannot find consensus for P {}", self.id, p.n);
            // add the consensus
            let consensus = Consensus {
                batch: vec![],
                pp: None,
                p: Quorum::new(self.n as usize, 2 * self.f as usize),
                c: Quorum::new(self.n as usize, (2 * self.f + 1) as usize),
                rep: None,
            };
            self.consensus.insert(c.n, consensus);
        }

        self.consensus.get_mut(&c.n).unwrap().c.add(c.r, c.clone());
    }

    fn execute_requests_and_reply(&mut self, f: &dyn Fn(Vec<u8>) -> Vec<u8>) {
        if self.f == 0 {
            for req in self.pending_req.iter() {
                println!(
                    "Single-mode: Replica {} has received request {:?}",
                    self.id, req
                );

                let u = f(req.o.to_vec());
                let rep = self.create_reply(req.s, u);
                self.send_reply(req.c, &rep);
            }
            self.pending_req.clear();
            return;
        }

        // execute all the consensus that we can execute
        for (consensus_num, consensus) in self.consensus.iter() {
            // check if the consensus has been done and the reply has not been executed yet
            if *consensus_num == self.s
                && consensus.rep.is_none()
                && !consensus.batch.is_empty()
                && consensus.pp.is_some()
                && consensus.p.is_complete()
                && consensus.c.is_complete()
            {
                /*
                println!(
                    "Replica {} executes request for consensus {}; last exec is {}",
                    self.id, consensus_num, self.s
                );
                println!(
                    "Replica {} executes {} requests in consensus {}",
                    self.id,
                    consensus.batch.len(),
                    consensus_num
                );
                */
                for req in consensus.batch.iter() {
                    let result = f(req.o.clone());
                    let req = &req;
                    let rep = self.create_reply(req.s, result);
                    self.send_reply(req.c, &rep);
                }
                //FIXME we never save the reply
                //consensus.rep = Some(rep);
                self.s = consensus_num + 1;
            }
        }

        // remove the old consensus
        // FIXME This might be a problem once we implement retransmissions
        self.consensus.retain(|&k, _| k >= self.s);
    }
}

const MAX_PENDING_CONSENSUS: usize = 1;

#[derive(Debug)]
struct Consensus {
    batch: Vec<Request>,
    pp: Option<PrePrepare>,
    p: Quorum<Prepare>, // 2f matching prepare
    c: Quorum<Commit>,  // 2f+1 matching commit
    rep: Option<Reply>,
}
