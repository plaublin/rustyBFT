use bit_vec::BitVec;
use std::rc::Rc;

#[derive(Debug)]
struct QuorumValue<T> {
    v: Rc<T>,  // the value
    b: BitVec, // need p identical values from distinch replicas
}

#[derive(Debug)]
pub struct Quorum<T> {
    size: usize,             // quorum size, i.e., number of distinct users
    complete: usize,         // size of a completed quorum
    v: Option<usize>,        // the index in vv to the accepted value
    vv: Vec<QuorumValue<T>>, // the received values
}

impl<T> Quorum<T>
where
    T: PartialEq,
{
    pub fn new(size: usize, complete: usize) -> Quorum<T> {
        Quorum::<T> {
            size,
            complete,
            v: None,
            vv: vec![],
        }
    }

    pub fn is_complete(&self) -> bool {
        self.v.is_some()
    }

    pub fn value(&self) -> Rc<T> {
        assert!(self.v.is_some());
        self.vv[self.v.unwrap()].v.clone()
    }

    pub fn add(&mut self, id: u32, r: T) {
        // Don't bother if the quorum is already complete
        if self.is_complete() {
            return;
        }

        // is the reply already in vv?
        // If not, then add it (we potentially already have a reply from the same replica)
        // else, update the entry in vv
        if let Some(v) = self.vv.iter_mut().find(|v| *v.v == r) {
            v.b.set(id.try_into().unwrap(), true);
        } else {
            let mut b = BitVec::from_elem(self.size, false);
            b.set(id.try_into().unwrap(), true);
            self.vv.push(QuorumValue { v: Rc::new(r), b });
        }

        // check whether the quorum is complete or not
        if let Some((i, _)) = self
            .vv
            .iter()
            .enumerate()
            .find(|(_, v)| v.b.iter().filter(|x| *x).count() == self.complete)
        {
            self.v = Some(i);
        }
    }
}

#[cfg(test)]
mod quorum_test {
    use super::*;
    use crate::message::{RawMessage, Reply};

    fn add_to_quorum(mut q: Quorum<RawMessage>, r: RawMessage) -> Quorum<RawMessage> {
        println!("Adding {:?} to quorum {:?}", r, q);
        let m = r.message::<Reply>();
        q.add(m.r, r);
        if q.is_complete() {
            println!("Quorum is complete: {:?}", q.value());
        } else {
            println!("Quorum is not complete yet...");
        }
        q
    }

    #[test]
    fn test_simple_quorum() {
        let r0 = RawMessage::new_reply(0, 0, 0, 0);
        let mut q = Quorum::new(1, 1);
        q = add_to_quorum(q, r0);
        assert!(q.is_complete());
    }
    #[test]
    fn test_complete_quorum() {
        let r0 = RawMessage::new_reply(0, 0, 0, 0);
        let r1 = RawMessage::new_reply(1, 0, 0, 0);
        let r2 = RawMessage::new_reply(2, 0, 0, 0);
        let mut q = Quorum::new(4, 3);
        q = add_to_quorum(q, r0);
        q = add_to_quorum(q, r1);
        q = add_to_quorum(q, r2);
        assert!(q.is_complete());
    }

    #[test]
    fn test_not_complete_quorum() {
        let r0 = RawMessage::new_reply(0, 0, 0, 0);
        let r1 = RawMessage::new_reply(1, 1, 0, 0);
        let r2 = RawMessage::new_reply(2, 0, 3, 0);
        let mut q = Quorum::new(4, 3);
        q = add_to_quorum(q, r0);
        q = add_to_quorum(q, r1);
        q = add_to_quorum(q, r2);
        assert!(!q.is_complete());
    }
}
