use crate::configuration::MAX_MESSAGE_LENGTH;
use crate::crypto::CryptoLayer;

#[derive(PartialEq, Eq, Debug)]
pub enum MessageType {
    Request,
    Reply,
    PrePrepare,
    Prepare,
    Commit,
}

impl MessageType {
    pub fn to_u8(self) -> u8 {
        match self {
            MessageType::Request => 0,
            MessageType::Reply => 1,
            MessageType::PrePrepare => 2,
            MessageType::Prepare => 3,
            MessageType::Commit => 4,
        }
    }

    pub fn from_u8(t: u8) -> Self {
        match t {
            0 => MessageType::Request,
            1 => MessageType::Reply,
            2 => MessageType::PrePrepare,
            3 => MessageType::Prepare,
            4 => MessageType::Commit,
            _ => panic!("Unknown message type {}", t),
        }
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct Request {
    pub seqnum: u64, // seq num
    pub ro: bool,    // read only?
    pub c: u32,      // client id
    pub len: usize,  // payload length
    pub signature: [u8; CryptoLayer::signature_length()], // request signature
                     // followed by the payload
}

#[derive(Debug)]
#[repr(C)]
pub struct Reply {
    pub v: u64,      // view
    pub seqnum: u64, // seq num
    pub r: u32,      // sender replica id
    pub len: usize,  // payload length
                     // followed by the payload
}

#[derive(Debug)]
#[repr(C)]
pub struct PrePrepare {
    pub v: u64,      // view
    pub seqnum: u64, // seq num
    pub r: u32,      // sender replica id
    pub len: usize,  // payload length
                     // followed by the payload
}

#[derive(Debug)]
#[repr(C)]
pub struct Prepare {
    pub v: u64,                                     // view
    pub seqnum: u64,                                // seq num
    pub r: u32,                                     // sender replica id
    pub digest: [u8; CryptoLayer::digest_length()], // PP(v, seqnum) request(s) digest
}

#[derive(Debug)]
#[repr(C)]
pub struct Commit {
    pub v: u64,      // view
    pub seqnum: u64, // seq num
    pub r: u32,      // sender replica id
}

pub trait ProtocolMessage {
    // where Self: Sized is needed to be able to construct a vtable and use `dyn ProtocolMessage`

    fn header_length() -> usize
    where
        Self: Sized;

    fn message_type() -> MessageType
    where
        Self: Sized;

    fn max_payload_max() -> usize
    where
        Self: Sized,
    {
        MAX_MESSAGE_LENGTH - std::mem::size_of::<MessageHeader>() - Self::header_length()
    }

    fn payload_len(&self) -> usize;
}

impl ProtocolMessage for Request {
    fn header_length() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<Request>()
    }

    fn message_type() -> MessageType
    where
        Self: Sized,
    {
        MessageType::Request
    }

    fn payload_len(&self) -> usize {
        self.len
    }
}

impl ProtocolMessage for Reply {
    fn header_length() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<Reply>()
    }

    fn message_type() -> MessageType
    where
        Self: Sized,
    {
        MessageType::Reply
    }

    fn payload_len(&self) -> usize {
        self.len
    }
}

impl ProtocolMessage for PrePrepare {
    fn header_length() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<PrePrepare>()
    }

    fn message_type() -> MessageType
    where
        Self: Sized,
    {
        MessageType::PrePrepare
    }

    fn payload_len(&self) -> usize {
        self.len
    }
}

impl ProtocolMessage for Prepare {
    fn header_length() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<Prepare>()
    }

    fn message_type() -> MessageType
    where
        Self: Sized,
    {
        MessageType::Prepare
    }

    fn max_payload_max() -> usize
    where
        Self: Sized,
    {
        0
    }

    fn payload_len(&self) -> usize {
        0
    }
}

impl ProtocolMessage for Commit {
    fn header_length() -> usize
    where
        Self: Sized,
    {
        std::mem::size_of::<Commit>()
    }

    fn message_type() -> MessageType
    where
        Self: Sized,
    {
        MessageType::Commit
    }

    fn max_payload_max() -> usize
    where
        Self: Sized,
    {
        0
    }

    fn payload_len(&self) -> usize {
        0
    }
}

#[derive(Debug)]
#[repr(C)]
pub struct MessageHeader {
    pub t: u8, // type
    pub sender: u32,
    pub len: usize,
    pub digest: [u8; CryptoLayer::digest_length()],
}

#[derive(Debug, Clone)]
pub struct RawMessage {
    pub inner: Vec<u8>,
}

impl PartialEq for RawMessage {
    fn eq(&self, rhs: &Self) -> bool {
        if self.message_type() != rhs.message_type() {
            return false;
        }

        match self.message_type() {
            MessageType::Reply => {
                let lhs = self.message::<Reply>();
                let rhs = rhs.message::<Reply>();
                lhs.v == rhs.v && lhs.seqnum == rhs.seqnum
            }
            MessageType::Prepare => {
                let lhs = self.message::<Prepare>();
                let rhs = rhs.message::<Prepare>();
                lhs.v == rhs.v && lhs.seqnum == rhs.seqnum && lhs.digest == rhs.digest
            }
            MessageType::Commit => {
                let lhs = self.message::<Commit>();
                let rhs = rhs.message::<Commit>();
                lhs.v == rhs.v && lhs.seqnum == rhs.seqnum
            }
            t => unimplemented!("PartialEq for {:?}", t),
        }
    }
}

impl Default for RawMessage {
    fn default() -> Self {
        Self::new(MAX_MESSAGE_LENGTH)
    }
}

impl RawMessage {
    /// Creates a new empty RawMessage
    pub fn new(len: usize) -> Self {
        assert!(len <= MAX_MESSAGE_LENGTH);
        Self {
            inner: vec![0; len],
        }
    }

    /// Creates a new RawMessage, filling-in the MessageHeader
    fn new_with_header(t: MessageType, sender: u32, len: usize) -> Self {
        assert!(std::mem::size_of::<MessageHeader>() + len <= MAX_MESSAGE_LENGTH);

        let mut raw_message = Self::new(std::mem::size_of::<MessageHeader>() + len);
        let header = raw_message.header_mut();
        header.t = MessageType::to_u8(t);
        header.sender = sender;
        header.len = len;

        raw_message
    }

    /// Return the "useful" length of this message that will be sent over the network
    pub fn message_len(&self) -> usize {
        std::mem::size_of::<MessageHeader>() + self.header().len
    }

    /// Return the type of this RawMessage
    pub fn message_type(&self) -> MessageType {
        MessageType::from_u8(self.header().t)
    }

    /// Return the length capacity left in this message after the headers
    pub fn capacity(&self) -> usize {
        let sz = match MessageType::from_u8(self.header().t) {
            MessageType::Request => Request::header_length(),
            MessageType::Reply => Reply::header_length(),
            MessageType::PrePrepare => PrePrepare::header_length(),
            MessageType::Prepare => Prepare::header_length(),
            MessageType::Commit => Commit::header_length(),
        };
        MAX_MESSAGE_LENGTH - std::mem::size_of::<MessageHeader>() - sz
    }

    pub fn trim(&mut self, len: usize) {
        assert!(len <= MAX_MESSAGE_LENGTH);
        self.inner.resize(len, 0);
    }

    /// Returns a reference to the MessageHeader structure in this RawMessage
    pub fn header(&self) -> &MessageHeader {
        unsafe { &*(self.inner.as_ptr() as *const MessageHeader) as &MessageHeader }
    }

    /// Returns a mutable reference to the MessageHeader structure in this RawMessage
    fn header_mut(&mut self) -> &mut MessageHeader {
        unsafe { &mut *(self.inner.as_ptr() as *mut MessageHeader) as &mut MessageHeader }
    }

    /// Sets the digest of this MessageHeader
    pub fn set_header_digest(&mut self, digest: [u8; CryptoLayer::digest_length()]) {
        self.header_mut().digest = digest;
    }

    /// Resets the digest of this MessageHeader to [0] and returns the previous one
    pub fn reset_header_digest(&mut self) -> [u8; CryptoLayer::digest_length()] {
        let digest = self.header().digest;
        self.header_mut().digest = [0; CryptoLayer::digest_length()];
        digest
    }

    /// Sets the signature of this Request (panics if this RawMessage is not of type Request)
    pub fn set_request_signature(&mut self, signature: [u8; CryptoLayer::signature_length()]) {
        self.message_mut::<Request>().signature = signature;
    }

    /// Resets the signature of this Request to [0] and returns the previous one (panics if this
    /// RawMessage is not of type Request)
    pub fn reset_request_signature(&mut self) -> [u8; CryptoLayer::signature_length()] {
        let signature = self.message::<Request>().signature;
        self.message_mut::<Request>().signature = [0; CryptoLayer::signature_length()];
        signature
    }

    /// Create a new request from client `c` with seqnum `seqnum` and payload length `payload_len`
    /// if ro, then this is a read-only request
    pub fn new_request(c: u32, ro: bool, seqnum: u64, payload_len: usize) -> Self {
        let mut raw_message = RawMessage::new_with_header(
            Request::message_type(),
            c,
            Request::header_length() + payload_len,
        );

        let request: &mut Request = raw_message.message_mut();
        request.c = c;
        request.ro = ro;
        request.seqnum = seqnum;
        request.len = payload_len;

        raw_message
    }

    /// Create a new reply from replica `r` with seqnum `seqnum` in view `v` and payload length `payload_len`
    pub fn new_reply(r: u32, v: u64, seqnum: u64, payload_len: usize) -> Self {
        let mut raw_message = RawMessage::new_with_header(
            MessageType::Reply,
            r,
            Reply::header_length() + payload_len,
        );

        let reply = raw_message.message_mut::<Reply>();
        reply.v = v;
        reply.r = r;
        reply.seqnum = seqnum;
        reply.len = payload_len;

        raw_message
    }

    /// Create a new pre-prepare from replica `r` with seqnum `seqnum` in view `v` and batch length `batch_len`
    pub fn new_preprepare(v: u64, seqnum: u64, r: u32, batch_len: usize) -> Self {
        let mut raw_message = RawMessage::new_with_header(
            MessageType::PrePrepare,
            r,
            PrePrepare::header_length() + batch_len,
        );

        let pp = raw_message.message_mut::<PrePrepare>();
        pp.v = v;
        pp.seqnum = seqnum;
        pp.r = r;
        pp.len = batch_len;

        raw_message
    }

    /// Create a new prepare from replica `r` with seqnum `seqnum` in view `v` and request(s) digest `digest`
    pub fn new_prepare(
        v: u64,
        seqnum: u64,
        r: u32,
        digest: [u8; CryptoLayer::digest_length()],
    ) -> Self {
        let mut raw_message =
            RawMessage::new_with_header(MessageType::Prepare, r, Prepare::header_length());

        let p = raw_message.message_mut::<Prepare>();
        p.v = v;
        p.r = r;
        p.seqnum = seqnum;
        p.digest = digest;

        raw_message
    }

    /// Create a new commit from replica `r` with seqnum `seqnum` in view `v`
    pub fn new_commit(v: u64, seqnum: u64, r: u32) -> Self {
        let mut raw_message =
            RawMessage::new_with_header(MessageType::Commit, r, Commit::header_length());

        let c = raw_message.message_mut::<Commit>();
        c.v = v;
        c.r = r;
        c.seqnum = seqnum;

        raw_message
    }

    /// Returns a reference to the Message (structure that implements the trait, e.g., a Request or
    /// a Reply) in this RawMessage
    //XXX Not sure if we'll use this method, or if we just manually test the type before trying
    // to convert with message() below
    pub fn message_generic(&self) -> Box<&dyn ProtocolMessage> {
        match MessageType::from_u8(self.header().t) {
            MessageType::Request => Box::new(self.message::<Request>() as &dyn ProtocolMessage),
            MessageType::Reply => Box::new(self.message::<Reply>() as &dyn ProtocolMessage),
            MessageType::PrePrepare => {
                Box::new(self.message::<PrePrepare>() as &dyn ProtocolMessage)
            }
            MessageType::Prepare => Box::new(self.message::<Prepare>() as &dyn ProtocolMessage),
            MessageType::Commit => Box::new(self.message::<Commit>() as &dyn ProtocolMessage),
        }
    }

    /// Returns a reference to the Message (structure that implements the trait, e.g., a Request or
    /// a Reply) in this RawMessage
    pub fn message<T: ProtocolMessage>(&self) -> &T {
        assert!(self.header().t == MessageType::to_u8(T::message_type()));
        unsafe {
            &*(self
                .inner
                .as_ptr()
                .add(std::mem::size_of::<MessageHeader>()) as *const T) as &T
        }
    }

    /// Returns a mutable reference to the Message (structure that implements the trait, e.g., a
    /// Request or a Reply) in this RawMessage
    fn message_mut<T: ProtocolMessage>(&mut self) -> &mut T {
        assert!(self.header().t == MessageType::to_u8(T::message_type()));
        unsafe {
            &mut *(self
                .inner
                .as_ptr()
                .add(std::mem::size_of::<MessageHeader>()) as *mut T) as &mut T
        }
    }

    /// Returns a reference to this raw message content (after the MessageHeader)
    pub fn rawmessage_content(&self) -> &[u8] {
        let sz = std::mem::size_of::<MessageHeader>();
        &self.inner[sz..]
    }

    /// Returns a mutable reference to this raw message content (after the MessageHeader)
    pub fn rawmessage_content_mut(&mut self) -> &mut [u8] {
        let sz = std::mem::size_of::<MessageHeader>();
        &mut self.inner[sz..]
    }

    /// Returns None if this RawMessage doesn't have a message payload, a reference to the payload
    /// in this RawMessage as an Option otherwise
    pub fn message_payload<T: ProtocolMessage>(&self) -> Option<&[u8]> {
        if self.message::<T>().payload_len() == 0 {
            None
        } else {
            let sz = std::mem::size_of::<MessageHeader>() + T::header_length();
            let payload_len = self.message::<T>().payload_len();
            Some(&self.inner[sz..sz + payload_len])
        }
    }

    /// Returns None if this RawMessage doesn't have a message payload, a reference to the payload
    /// in this RawMessage as an Option otherwise
    pub fn message_payload_mut<T: ProtocolMessage>(&mut self) -> Option<&mut [u8]> {
        if self.message::<T>().payload_len() == 0 {
            None
        } else {
            let sz = std::mem::size_of::<MessageHeader>() + T::header_length();
            let payload_len = self.message::<T>().payload_len();
            Some(&mut self.inner[sz..sz + payload_len])
        }
    }
}
