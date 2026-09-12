//! Borrowed chunk parsing and owned, segmented message assembly.
use super::{DecodeError, DecoderLimits};
use crate::{
    messages::RawMessage,
    payload::{Payload, Segments},
    time::RtmpTimestamp,
};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;

type Error = DecodeError;

/// Message identity carried on every fragment. `offset` is independent of TCP reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub chunk_stream_id: u32,
    pub timestamp: RtmpTimestamp,
    pub type_id: u8,
    pub message_stream_id: u32,
    pub message_length: usize,
}

/// A borrowed payload piece. It may be shorter than an RTMP chunk.
#[derive(Debug)]
pub struct MessageFragment<'a> {
    pub header: MessageHeader,
    pub offset: usize,
    pub data: &'a [u8],
}
impl MessageFragment<'_> {
    pub fn is_start(&self) -> bool {
        self.offset == 0
    }
    pub fn is_end(&self) -> bool {
        self.offset + self.data.len() == self.header.message_length
    }
}

/// Consume exactly `consumed` input bytes before calling the parser again.
/// `None` means more input is required; header bytes may still have been consumed.
#[derive(Debug)]
pub struct ParseStep<'a> {
    pub consumed: usize,
    pub fragment: Option<MessageFragment<'a>>,
}

#[derive(Clone, Copy)]
struct Stream {
    header: MessageHeader,
    time_field: u32,
    received: usize,
    active: bool,
}

/// Streaming sans-I/O parser. Payload bytes are never stored or copied.
///
/// Feed all wire bytes in order. Handle a complete SetChunkSize or Abort message
/// before parsing the next message. An error is terminal: discard the parser.
/// Header scratch storage is inline; stream slots allocate only for new CSIDs.
/// `maximum_buffered_bytes` applies to the owned adapter, not borrowed fragments.
pub struct ChunkParser {
    streams: HashMap<u32, Stream>,
    scratch: [u8; 18],
    used: usize,
    current: Option<u32>,
    chunk_left: usize,
    chunk_size: usize,
    limits: DecoderLimits,
}
impl Default for ChunkParser {
    fn default() -> Self {
        Self::new()
    }
}
impl ChunkParser {
    pub fn new() -> Self {
        Self::with_limits(DecoderLimits::default())
    }
    pub fn with_limits(limits: DecoderLimits) -> Self {
        Self {
            streams: HashMap::new(),
            scratch: [0; 18],
            used: 0,
            current: None,
            chunk_left: 0,
            chunk_size: 128,
            limits,
        }
    }
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
    pub fn set_chunk_size(&mut self, size: usize) -> Result<(), Error> {
        if size == 0 || size > 0x7fff_ffff || size > self.limits.maximum_chunk_size {
            return Err(Error::InvalidMaxChunkSize { chunk_size: size });
        }
        self.chunk_size = size;
        Ok(())
    }
    pub fn abort_chunk_stream(&mut self, csid: u32) {
        if let Some(stream) = self.streams.get_mut(&csid) {
            stream.active = false;
            stream.received = 0;
        }
        if self.current == Some(csid) {
            self.current = None;
            self.chunk_left = 0;
        }
    }
    /// False if the last input ended inside a header, chunk, or message.
    pub fn is_idle(&self) -> bool {
        self.used == 0 && self.current.is_none() && self.streams.values().all(|s| !s.active)
    }
    pub fn consume<'a>(&mut self, input: &'a [u8]) -> Result<ParseStep<'a>, Error> {
        let mut consumed = 0;
        while self.current.is_none() {
            let needed = self.header_needed()?;
            if self.used < needed {
                let n = (needed - self.used).min(input.len() - consumed);
                self.scratch[self.used..self.used + n]
                    .copy_from_slice(&input[consumed..consumed + n]);
                self.used += n;
                consumed += n;
                if self.used < needed {
                    return Ok(ParseStep {
                        consumed,
                        fragment: None,
                    });
                }
                continue;
            }
            self.begin_chunk()?;
            self.used = 0;
        }
        let csid = self.current.unwrap();
        let stream = self.streams.get_mut(&csid).unwrap();
        let n = self.chunk_left.min(input.len() - consumed);
        if n == 0 && self.chunk_left > 0 {
            return Ok(ParseStep {
                consumed,
                fragment: None,
            });
        }
        let fragment = MessageFragment {
            header: stream.header,
            offset: stream.received,
            data: &input[consumed..consumed + n],
        };
        stream.received += n;
        self.chunk_left -= n;
        consumed += n;
        if stream.received == stream.header.message_length {
            stream.active = false;
        }
        if self.chunk_left == 0 {
            self.current = None;
        }
        Ok(ParseStep {
            consumed,
            fragment: Some(fragment),
        })
    }
    fn basic_len(&self) -> usize {
        match self.scratch[0] & 63 {
            0 => 2,
            1 => 3,
            _ => 1,
        }
    }
    fn csid(&self) -> u32 {
        match self.scratch[0] & 63 {
            0 => 64 + self.scratch[1] as u32,
            1 => 64 + self.scratch[1] as u32 + 256 * self.scratch[2] as u32,
            n => n as u32,
        }
    }
    fn header_needed(&self) -> Result<usize, Error> {
        if self.used == 0 {
            return Ok(1);
        }
        let basic = self.basic_len();
        if self.used < basic {
            return Ok(basic);
        }
        let fmt = self.scratch[0] >> 6;
        let body = [11, 7, 3, 0][fmt as usize];
        if self.used < basic + body {
            return Ok(basic + body);
        }
        let extended = if fmt == 3 {
            self.streams
                .get(&self.csid())
                .ok_or(Error::NoPreviousChunkOnStream { csid: self.csid() })?
                .time_field
                >= 0xff_ffff
        } else {
            u24(&self.scratch[basic..]) == 0xff_ffff
        };
        Ok(basic + body + if extended { 4 } else { 0 })
    }
    fn begin_chunk(&mut self) -> Result<(), Error> {
        let csid = self.csid();
        let fmt = self.scratch[0] >> 6;
        let basic = self.basic_len();
        let old = self.streams.get(&csid).copied();
        if fmt != 0 && old.is_none() {
            return Err(Error::NoPreviousChunkOnStream { csid });
        }
        if old.is_none() && self.streams.len() >= self.limits.maximum_tracked_chunk_streams {
            return Err(limit(
                "tracked chunk streams",
                self.streams.len() + 1,
                self.limits.maximum_tracked_chunk_streams,
            ));
        }
        let continuing = old.is_some_and(|s| s.active);
        let mut header = old.map(|s| s.header).unwrap_or(MessageHeader {
            chunk_stream_id: csid,
            timestamp: RtmpTimestamp::new(0),
            type_id: 0,
            message_stream_id: 0,
            message_length: 0,
        });
        if fmt <= 1 {
            header.message_length = u24(&self.scratch[basic + 3..]) as usize;
            header.type_id = self.scratch[basic + 6];
        }
        if fmt == 0 {
            header.message_stream_id =
                u32::from_le_bytes(self.scratch[basic + 7..basic + 11].try_into().unwrap());
        }
        if header.message_length > self.limits.maximum_message_size {
            return Err(limit(
                "message payload bytes",
                header.message_length,
                self.limits.maximum_message_size,
            ));
        }
        let field = if fmt == 3 {
            old.unwrap().time_field
        } else {
            u24(&self.scratch[basic..])
        };
        let extended = field >= 0xff_ffff;
        let time = if extended {
            let i = basic + [11, 7, 3, 0][fmt as usize];
            u32::from_be_bytes(self.scratch[i..i + 4].try_into().unwrap())
        } else {
            field
        };
        if !continuing {
            header.timestamp = if fmt == 0 {
                RtmpTimestamp::new(time)
            } else {
                header.timestamp + time
            };
            // A complete single-chunk control message need not occupy a partial-message slot.
            if header.message_length > self.chunk_size {
                let active = self.streams.values().filter(|s| s.active).count();
                if active >= self.limits.maximum_partial_messages {
                    return Err(limit(
                        "partial messages",
                        active + 1,
                        self.limits.maximum_partial_messages,
                    ));
                }
            }
        } else {
            let previous = old.unwrap();
            if header.message_length < previous.received {
                return Err(Error::MessageLengthSmallerThanBufferedPayload {
                    csid,
                    message_length: header.message_length,
                    buffered: previous.received,
                });
            }
            if header.message_length != previous.header.message_length
                || header.type_id != previous.header.type_id
                || header.message_stream_id != previous.header.message_stream_id
                || (fmt == 0 && time != previous.header.timestamp.value)
            {
                return Err(Error::InvalidContinuation { csid });
            }
        }
        let received = if continuing { old.unwrap().received } else { 0 };
        self.chunk_left = (header.message_length - received).min(self.chunk_size);
        self.current = Some(csid);
        // Preserve the actual extended delta for the next type-3 message.
        self.streams.insert(
            csid,
            Stream {
                header,
                time_field: time,
                received,
                active: true,
            },
        );
        Ok(())
    }
}
fn u24(bytes: &[u8]) -> u32 {
    ((bytes[0] as u32) << 16) | ((bytes[1] as u32) << 8) | bytes[2] as u32
}
fn limit(resource: &'static str, attempted: usize, maximum: usize) -> Error {
    Error::ResourceLimitExceeded {
        resource,
        attempted,
        maximum,
    }
}

/// Owned zero-copy adapter. Input is advanced in place; unread bytes remain with
/// the caller when a message completes, so control messages can be applied first.
/// Payload descriptors are bounded separately from payload bytes.
pub struct MessageDecoder {
    parser: ChunkParser,
    partials: HashMap<u32, Assembly>,
    buffered: usize,
    reserved: usize,
    limits: DecoderLimits,
    pool: Option<crate::PayloadPool>,
}

enum Assembly {
    Segmented(Payload),
    Contiguous(BytesMut),
}
impl Assembly {
    fn len(&self) -> usize {
        match self {
            Self::Segmented(p) => p.len(),
            Self::Contiguous(p) => p.len(),
        }
    }
    fn reserved(&self) -> usize {
        match self {
            Self::Segmented(p) => p.len(),
            Self::Contiguous(p) => p.capacity(),
        }
    }
    fn finish(self) -> Payload {
        match self {
            Self::Segmented(p) => p,
            Self::Contiguous(p) => p.freeze().into(),
        }
    }
}

// Both session entry points share framing and partial-message state.
pub(crate) enum Input<'a> {
    Owned(Bytes),
    Borrowed(&'a [u8]),
}
impl Input<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(b) => b,
            Self::Borrowed(b) => b,
        }
    }
    fn advance(&mut self, n: usize) {
        match self {
            Self::Owned(b) => b.advance(n),
            Self::Borrowed(b) => *b = &b[n..],
        }
    }
    fn take(&mut self, n: usize) -> Bytes {
        match self {
            Self::Owned(b) => b.split_to(n),
            Self::Borrowed(b) => {
                let out = Bytes::copy_from_slice(&b[..n]);
                *b = &b[n..];
                out
            }
        }
    }
}
impl Default for MessageDecoder {
    fn default() -> Self {
        Self::new()
    }
}
impl MessageDecoder {
    pub fn new() -> Self {
        Self::with_limits(DecoderLimits::default())
    }
    pub fn with_limits(limits: DecoderLimits) -> Self {
        Self {
            parser: ChunkParser::with_limits(limits),
            partials: HashMap::new(),
            buffered: 0,
            reserved: 0,
            limits,
            pool: None,
        }
    }
    /// Recycle descriptors after a payload is dropped, including on another thread.
    /// Existing partial messages keep their current storage.
    pub fn set_payload_pool(&mut self, pool: crate::PayloadPool) {
        self.pool = Some(pool);
    }
    pub fn set_chunk_size(&mut self, size: usize) -> Result<(), Error> {
        self.parser.set_chunk_size(size)
    }
    pub fn chunk_size(&self) -> usize {
        self.parser.chunk_size()
    }
    pub fn is_idle(&self) -> bool {
        self.parser.is_idle()
    }
    pub fn abort_chunk_stream(&mut self, csid: u32) {
        self.parser.abort_chunk_stream(csid);
        if let Some(body) = self.partials.remove(&csid) {
            self.buffered -= body.len();
            self.reserved -= body.reserved();
        }
    }
    pub fn decode(&mut self, input: &mut Bytes) -> Result<Option<RawMessage<Payload>>, Error> {
        let mut source = Input::Owned(std::mem::take(input));
        let result = self.decode_input(&mut source);
        if let Input::Owned(b) = source {
            *input = b;
        }
        result
    }
    /// Copy borrowed input directly into a contiguous message allocation.
    /// The caller retains the unread tail. Apply control messages before decoding it.
    /// Mixing this with `decode` is supported; assembly follows the first fragment.
    pub fn decode_slice(
        &mut self,
        input: &mut &[u8],
    ) -> Result<Option<RawMessage<Payload>>, Error> {
        let mut source = Input::Borrowed(input);
        let result = self.decode_input(&mut source);
        if let Input::Borrowed(b) = source {
            *input = b;
        }
        result
    }
    pub(crate) fn decode_input(
        &mut self,
        input: &mut Input<'_>,
    ) -> Result<Option<RawMessage<Payload>>, Error> {
        loop {
            let step = self.parser.consume(input.as_slice())?;
            let consumed = step.consumed;
            let Some(fragment) = step.fragment else {
                input.advance(consumed);
                return Ok(None);
            };
            let header = fragment.header;
            let n = fragment.data.len();
            let complete = fragment.is_end();
            let csid = header.chunk_stream_id;
            let attempted = self.buffered.saturating_add(n);
            if attempted > self.limits.maximum_buffered_bytes {
                return Err(limit(
                    "buffered bytes",
                    attempted,
                    self.limits.maximum_buffered_bytes,
                ));
            }
            input.advance(consumed - n);
            let owned = matches!(input, Input::Owned(_));
            if owned
                && n != 0
                && self.limits.maximum_fragments_per_message == 0
                && !self.partials.contains_key(&csid)
            {
                return Err(limit("payload fragments", 1, 0));
            }
            let data = if complete && !self.partials.contains_key(&csid) {
                if self.reserved.saturating_add(n) > self.limits.maximum_buffered_bytes {
                    return Err(limit(
                        "reserved bytes",
                        self.reserved.saturating_add(n),
                        self.limits.maximum_buffered_bytes,
                    ));
                }
                input.take(n).into()
            } else {
                if !self.partials.contains_key(&csid) {
                    if self.partials.len() >= self.limits.maximum_partial_messages {
                        return Err(limit(
                            "partial messages",
                            self.partials.len() + 1,
                            self.limits.maximum_partial_messages,
                        ));
                    }
                    let body = if owned {
                        Assembly::Segmented(match &self.pool {
                            Some(p) => p.acquire(),
                            None => Payload::new(),
                        })
                    } else {
                        let capacity = header.message_length as usize;
                        if self.reserved.saturating_add(capacity)
                            > self.limits.maximum_buffered_bytes
                        {
                            return Err(limit(
                                "reserved bytes",
                                self.reserved.saturating_add(capacity),
                                self.limits.maximum_buffered_bytes,
                            ));
                        }
                        Assembly::Contiguous(BytesMut::with_capacity(capacity))
                    };
                    self.reserved += body.reserved();
                    self.partials.insert(csid, body);
                }
                let body = self.partials.get_mut(&csid).unwrap();
                match body {
                    Assembly::Segmented(p) => {
                        if n != 0 && p.segment_count() >= self.limits.maximum_fragments_per_message
                        {
                            return Err(limit(
                                "payload fragments",
                                p.segment_count() + 1,
                                self.limits.maximum_fragments_per_message,
                            ));
                        }
                        if self.reserved.saturating_add(n) > self.limits.maximum_buffered_bytes {
                            return Err(limit(
                                "reserved bytes",
                                self.reserved.saturating_add(n),
                                self.limits.maximum_buffered_bytes,
                            ));
                        }
                        p.push(input.take(n));
                        self.reserved += n;
                    }
                    Assembly::Contiguous(p) => {
                        p.extend_from_slice(&input.as_slice()[..n]);
                        input.advance(n);
                    }
                }
                self.buffered = attempted;
                if !complete {
                    continue;
                }
                let body = self.partials.remove(&csid).unwrap();
                self.buffered -= body.len();
                self.reserved -= body.reserved();
                body.finish()
            };
            return Ok(Some(RawMessage {
                timestamp: header.timestamp,
                type_id: header.type_id,
                message_stream_id: header.message_stream_id,
                data,
            }));
        }
    }
}
