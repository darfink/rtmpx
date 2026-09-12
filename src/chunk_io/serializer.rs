use super::chunk_header::{ChunkHeader, ChunkHeaderFormat};
use crate::{
    chunk_io::EncodeError,
    messages::{RawMessage, RtmpMessage},
    payload::Segments,
    time::RtmpTimestamp,
};
use std::io::IoSlice;

const INITIAL_MAX_CHUNK_SIZE: u32 = 128;
const MAX_INITIAL_TIMESTAMP: u32 = 0xff_ffff;

/// Whether an unsent packet may be omitted. Partially written packets must finish.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DropPolicy {
    #[default]
    Never,
    Allowed,
}
/// Header encoding for a low-level message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeaderMode {
    #[default]
    Compressed,
    Full,
}
/// Low-level encoding options. Session sends need only a drop policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EncodeOptions {
    pub drop_policy: DropPolicy,
    pub headers: HeaderMode,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct InlineHeader {
    bytes: [u8; 18],
    len: usize,
}
impl InlineHeader {
    fn encode(header: &ChunkHeader, format: ChunkHeaderFormat) -> Self {
        let mut out = Self {
            bytes: [0; 18],
            len: 0,
        };
        let fmt = match format {
            ChunkHeaderFormat::Full => 0,
            ChunkHeaderFormat::TimeDeltaWithoutMessageStreamId => 1,
            ChunkHeaderFormat::TimeDeltaOnly => 2,
            ChunkHeaderFormat::Empty => 3,
        };
        out.push(&[(fmt << 6) | header.chunk_stream_id as u8]);
        if format != ChunkHeaderFormat::Empty {
            out.push(
                &header
                    .timestamp_field
                    .min(MAX_INITIAL_TIMESTAMP)
                    .to_be_bytes()[1..],
            );
            if format != ChunkHeaderFormat::TimeDeltaOnly {
                out.push(&header.message_length.to_be_bytes()[1..]);
                out.push(&[header.message_type_id]);
                if format == ChunkHeaderFormat::Full {
                    out.push(&header.message_stream_id.to_le_bytes());
                }
            }
        }
        if header.timestamp_field >= MAX_INITIAL_TIMESTAMP {
            out.push(&header.timestamp_field.to_be_bytes());
        }
        out
    }
    fn push(&mut self, bytes: &[u8]) {
        self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
    fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// An RTMP wire plan that retains its payload without copying it.
///
/// Send plans in preparation order. Only a droppable plan whose transmission has
/// not started may be discarded. The plan snapshots the negotiated chunk size.
/// A borrowed plan uses `&Bytes`, `&[u8]`, or another borrowed [`Segments`] value;
/// an owned plan can be queued independently of the serializer.
#[derive(Debug, PartialEq)]
pub struct Packet<P = crate::Payload> {
    payload: P,
    first: InlineHeader,
    continuation: InlineHeader,
    chunk_size: usize,
    drop_policy: DropPolicy,
    state: CursorState,
    remaining: usize,
}
impl<P: Segments> Packet<P> {
    pub fn payload(&self) -> &P {
        &self.payload
    }
    pub fn into_payload(self) -> P {
        self.payload
    }
    /// Number of wire bytes, including every chunk header.
    pub fn wire_len(&self) -> usize {
        let chunks = self.payload.len().div_ceil(self.chunk_size).max(1);
        self.payload.len() + self.first.len + (chunks - 1) * self.continuation.len
    }
    fn cursor(&self) -> PacketCursor<'_, P> {
        PacketCursor {
            packet: self,
            state: self.state,
            remaining: self.remaining,
        }
    }
    pub fn drop_policy(&self) -> DropPolicy {
        self.drop_policy
    }
    pub fn remaining(&self) -> usize {
        self.remaining
    }
    pub fn is_complete(&self) -> bool {
        self.remaining == 0
    }
    /// True only while this droppable packet is entirely unsent.
    pub fn can_drop(&self) -> bool {
        self.drop_policy == DropPolicy::Allowed && self.remaining == self.wire_len()
    }
    /// Advance only by the number of bytes accepted by the transport.
    pub fn advance(&mut self, count: usize) {
        let mut cursor = self.cursor();
        cursor.advance(count);
        let state = cursor.state;
        let remaining = cursor.remaining;
        self.state = state;
        self.remaining = remaining;
    }
    /// Borrow remaining headers and payload for a vectored write.
    pub fn io_slices<'a>(&'a self, output: &mut [IoSlice<'a>]) -> usize {
        self.cursor().io_slices(output)
    }
    /// Append a contiguous representation. Reuses the caller's allocation.
    pub fn copy_to(&self, output: &mut Vec<u8>) {
        output.reserve(self.remaining());
        let mut cursor = self.cursor();
        while let Some(bytes) = cursor.current() {
            let n = bytes.len();
            output.extend_from_slice(bytes);
            cursor.advance(n);
        }
    }
    /// Explicitly copy headers and payload into one allocation.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.remaining());
        self.copy_to(&mut out);
        out
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct CursorState {
    chunk: usize,
    header: usize,
    body: usize,
    segment: usize,
    offset: usize,
}

/// A resumable view of a packet. Advancing never allocates or copies payloads.
///
/// Fill a stack array of `IoSlice`s, write it, then call `advance` with the
/// number of bytes actually written. A short write may stop inside a header.
struct PacketCursor<'a, P> {
    packet: &'a Packet<P>,
    state: CursorState,
    remaining: usize,
}
impl<'p, P: Segments> PacketCursor<'p, P> {
    fn current(&self) -> Option<&[u8]> {
        if self.remaining == 0 {
            return None;
        }
        let h = if self.state.chunk == 0 {
            &self.packet.first
        } else {
            &self.packet.continuation
        };
        if self.state.header < h.len {
            return Some(&h.as_slice()[self.state.header..]);
        }
        let segment = self.packet.payload.segment(self.state.segment);
        let n = (segment.len() - self.state.offset)
            .min(self.packet.chunk_size - self.state.body)
            .min(
                self.packet.payload.len()
                    - (self.state.chunk * self.packet.chunk_size + self.state.body),
            );
        Some(&segment[self.state.offset..self.state.offset + n])
    }
    /// Fill as many nonempty slices as fit. The unused slots are cleared.
    pub fn io_slices(&self, output: &mut [IoSlice<'p>]) -> usize {
        let mut state = Self {
            packet: self.packet,
            state: self.state,
            remaining: self.remaining,
        };
        let mut count = 0;
        // Build slices directly from packet storage, so they do not borrow the temporary cursor.
        for slot in output.iter_mut() {
            *slot = IoSlice::new(&[]);
            if state.remaining == 0 {
                continue;
            }
            let s = state.state;
            let h = if s.chunk == 0 {
                &self.packet.first
            } else {
                &self.packet.continuation
            };
            let bytes = if s.header < h.len {
                &h.as_slice()[s.header..]
            } else {
                let segment = self.packet.payload.segment(s.segment);
                let n = (segment.len() - s.offset)
                    .min(self.packet.chunk_size - s.body)
                    .min(self.packet.payload.len() - (s.chunk * self.packet.chunk_size + s.body));
                &segment[s.offset..s.offset + n]
            };
            *slot = IoSlice::new(bytes);
            count += 1;
            state.advance(bytes.len());
        }
        count
    }
    /// Advance by bytes successfully written. Panics if `count > remaining()`.
    pub fn advance(&mut self, mut count: usize) {
        assert!(count <= self.remaining, "advance exceeds packet length");
        while count > 0 {
            let n = count.min(self.current().unwrap().len());
            let h = if self.state.chunk == 0 {
                &self.packet.first
            } else {
                &self.packet.continuation
            };
            if self.state.header < h.len {
                self.state.header += n;
            } else {
                self.state.body += n;
                self.state.offset += n;
                if self.state.offset == self.packet.payload.segment(self.state.segment).len() {
                    self.state.segment += 1;
                    self.state.offset = 0;
                }
                if self.state.body == self.packet.chunk_size {
                    self.state.chunk += 1;
                    self.state.body = 0;
                    self.state.header = 0;
                }
            }
            self.remaining -= n;
            count -= n;
        }
    }
}

/// Stateful RTMP header compressor. One instance per outbound connection.
pub struct ChunkEncoder {
    previous_headers: [Option<ChunkHeader>; 5],
    max_chunk_size: u32,
}
impl Default for ChunkEncoder {
    fn default() -> Self {
        Self::new()
    }
}
impl ChunkEncoder {
    pub fn new() -> Self {
        Self {
            previous_headers: [None; 5],
            max_chunk_size: INITIAL_MAX_CHUNK_SIZE,
        }
    }
    /// Encode SetChunkSize using the old chunk size, then update the encoder.
    pub fn set_chunk_size(
        &mut self,
        new_size: u32,
        time: RtmpTimestamp,
    ) -> Result<Packet, EncodeError> {
        if new_size == 0 || new_size > 0x7fff_ffff {
            return Err(EncodeError::InvalidMaxChunkSize {
                attempted_chunk_size: new_size,
            });
        }
        let payload =
            RawMessage::from_rtmp_message(RtmpMessage::SetChunkSize { size: new_size }, time, 0)?;
        let packet = self.encode(
            payload.map_data(crate::Payload::from),
            EncodeOptions {
                headers: HeaderMode::Full,
                ..Default::default()
            },
        )?;
        self.max_chunk_size = new_size;
        Ok(packet)
    }
    /// Encode owned or borrowed payload storage without copying it.
    /// Use `message.as_ref()` to borrow a message body.
    pub fn encode<P: Segments>(
        &mut self,
        message: RawMessage<P>,
        options: EncodeOptions,
    ) -> Result<Packet<P>, EncodeError> {
        let force_uncompressed = options.headers == HeaderMode::Full;
        let can_be_dropped = options.drop_policy == DropPolicy::Allowed;
        if message.data.len() > 0xff_ffff {
            return Err(EncodeError::MessageTooLong {
                size: message.data.len().min(u32::MAX as usize) as u32,
            });
        }
        let csid = get_csid_for_message_type(message.type_id);
        let slot = &mut self.previous_headers[(csid - 2) as usize];
        let mut header = ChunkHeader {
            chunk_stream_id: csid,
            timestamp: message.timestamp,
            timestamp_field: message.timestamp.value,
            message_type_id: message.type_id,
            message_stream_id: message.message_stream_id,
            message_length: message.data.len() as u32,
            can_be_dropped,
        };
        let format = match slot.as_ref() {
            Some(previous)
                if !force_uncompressed
                    && !previous.can_be_dropped
                    && previous.message_stream_id == message.message_stream_id =>
            {
                header.timestamp_field = (message.timestamp - previous.timestamp).value;
                if previous.message_type_id != message.type_id
                    || previous.message_length != header.message_length
                {
                    ChunkHeaderFormat::TimeDeltaWithoutMessageStreamId
                } else if previous.timestamp_field != header.timestamp_field {
                    ChunkHeaderFormat::TimeDeltaOnly
                } else {
                    ChunkHeaderFormat::Empty
                }
            }
            _ => ChunkHeaderFormat::Full,
        };
        let first = InlineHeader::encode(&header, format);
        let continuation = InlineHeader::encode(
            &header,
            if force_uncompressed {
                ChunkHeaderFormat::Full
            } else {
                ChunkHeaderFormat::Empty
            },
        );
        *slot = Some(header);
        let remaining = first.len
            + (message
                .data
                .len()
                .div_ceil(self.max_chunk_size as usize)
                .max(1)
                - 1)
                * continuation.len
            + message.data.len();
        Ok(Packet {
            payload: message.data,
            first,
            continuation,
            chunk_size: self.max_chunk_size as usize,
            drop_policy: options.drop_policy,
            state: CursorState::default(),
            remaining,
        })
    }
    // Internal command handlers operate on contiguous AMF bodies. This adapter
    // retains the body and returns the same packet type used by media sends.
    pub(crate) fn serialize(
        &mut self,
        message: &RawMessage,
        full: bool,
        droppable: bool,
    ) -> Result<Packet, EncodeError> {
        self.encode(
            message.clone().map_data(crate::Payload::from),
            EncodeOptions {
                headers: if full {
                    HeaderMode::Full
                } else {
                    HeaderMode::Compressed
                },
                drop_policy: if droppable {
                    DropPolicy::Allowed
                } else {
                    DropPolicy::Never
                },
            },
        )
    }
}
fn get_csid_for_message_type(message_type_id: u8) -> u32 {
    match message_type_id {
        1..=6 => 2,
        18 | 19 => 3,
        9 => 4,
        8 => 5,
        _ => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::RtmpTimestamp;
    use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
    use bytes::Bytes;
    use std::io::{Cursor, Read};

    #[test]
    fn type_0_chunk_for_first_message_with_small_timestamp() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet = serializer.serialize(&message1, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            72,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &message1.data[..],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_0_chunk_for_first_message_with_extended_timestamp() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(16777216),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet = serializer.serialize(&message1, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            16777215,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            16777216,
            "Unexpected extended timestamp"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            [1_u8, 2_u8, 3_u8, 4_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_1_chunk_for_second_message_with_same_stream_id_and_different_message_length_and_different_type_id_and_small_timestamp()
     {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 51,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b01000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            10,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            3,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 51, "Unexpected type id");

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 3, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[1_u8, 2_u8, 3_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_1_chunk_for_second_message_with_same_stream_id_and_different_message_length_and_different_type_id_and_extended_timestamp()
     {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(10),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(16777226),
            type_id: 51,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b01000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            16777215,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            3,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 51, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            16777216,
            "Unexpected extended timestamp"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 3, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[1_u8, 2_u8, 3_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_2_chunk_for_second_message_with_same_stream_id_and_same_message_length_and_same_type_id_and_small_timestamp()
     {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![5_u8, 6_u8, 7_u8, 8_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b10000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            10,
            "Unexpected timestamp value"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[5_u8, 6_u8, 7_u8, 8_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_2_chunk_for_second_message_with_same_stream_id_and_same_message_length_and_same_type_id_and_extended_timestamp()
     {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(10),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(16777226),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![5_u8, 6_u8, 7_u8, 8_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b10000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            16777215,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            16777216,
            "Unexpected extended timestamp"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[5_u8, 6_u8, 7_u8, 8_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_3_chunk_for_third_message_with_all_matching_details() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![5_u8, 6_u8, 7_u8, 8_u8]),
        };

        let message3 = RawMessage {
            timestamp: RtmpTimestamp::new(92),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![9_u8, 10_u8, 11_u8, 12_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let _ = serializer.serialize(&message2, false, false).unwrap();
        let packet = serializer.serialize(&message3, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b11000000,
            "Unexpected csid value"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[9_u8, 10_u8, 11_u8, 12_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_0_chunks_used_when_new_message_on_different_csid_serialized() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 1,
            message_stream_id: 12,
            data: Bytes::from(vec![6_u8, 7_u8, 8_u8, 9_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            2 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            82,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 1, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[6_u8, 7_u8, 8_u8, 9_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn type_0_chunk_for_second_message_when_forcing_uncompressed() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![5_u8, 6_u8, 7_u8, 8_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let _ = serializer.serialize(&message1, false, false).unwrap();
        let packet = serializer.serialize(&message2, true, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            82,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &[5_u8, 6_u8, 7_u8, 8_u8],
            "Unexpected payload contents"
        );
    }

    #[test]
    fn message_split_when_payload_exceeds_max_chunk_size() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[11_u8; 75]);
        payload.extend_from_slice(&[22_u8; 25]);

        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(payload.clone()),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(73),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(payload),
        };

        let mut serializer = ChunkEncoder::new();
        serializer
            .set_chunk_size(75, RtmpTimestamp::new(0))
            .unwrap();

        let packet = serializer.serialize(&message1, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            72,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            100,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );

        let mut payload_bytes = [0_u8; 75];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 75, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([11_u8; 75])[..],
            "Unexpected payload contents"
        );

        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b11000000,
            "Unexpected 2nd csid value"
        );
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 25, "Unexpected 2nd payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([22_u8; 25])[..],
            "Unexpected 2nd payload contents"
        );

        let packet = serializer.serialize(&message2, false, false).unwrap();
        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b10000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            1,
            "Unexpected timestamp value"
        );

        let mut payload_bytes = [0_u8; 75];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 75, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([11_u8; 75])[..],
            "Unexpected payload contents"
        );

        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b11000000,
            "Unexpected chunk format"
        );
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 25, "Unexpected 2nd payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([22_u8; 25])[..],
            "Unexpected 2nd payload contents"
        );
    }

    #[test]
    fn message_split_extended_timestamp() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[11_u8; 75]);
        payload.extend_from_slice(&[22_u8; 25]);

        let timestamp_value = MAX_INITIAL_TIMESTAMP + 1;
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(timestamp_value),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(payload.clone()),
        };

        let mut serializer = ChunkEncoder::new();
        serializer
            .set_chunk_size(75, RtmpTimestamp::new(0))
            .unwrap();

        let packet = serializer.serialize(&message1, false, false).unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            MAX_INITIAL_TIMESTAMP,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            100,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            timestamp_value,
            "Unexpected extended timestamp value"
        );

        let mut payload_bytes = [0_u8; 75];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 75, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([11_u8; 75])[..],
            "Unexpected payload contents"
        );

        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b11000000,
            "Unexpected 2nd csid value"
        );
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            timestamp_value,
            "Unexpected extended timestamp value on second chunk"
        );
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 25, "Unexpected 2nd payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &([22_u8; 25])[..],
            "Unexpected 2nd payload contents"
        );
    }

    #[test]
    fn changing_size_returns_set_chunk_size_outbound_message() {
        let mut serializer = ChunkEncoder::new();
        let packet = serializer
            .set_chunk_size(75, RtmpTimestamp::new(152))
            .unwrap();

        let mut cursor = Cursor::new(packet.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            2 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            152,
            "Unexpected timestamp"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 1, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            0,
            "Unexpected message stream id"
        );
        assert_eq!(
            cursor.read_u32::<BigEndian>().unwrap(),
            75,
            "Unexpected chunk size"
        );
    }

    #[test]
    fn type_0_chunk_comes_after_droppable_packet() {
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(72),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(82),
            type_id: 50,
            message_stream_id: 12,
            data: Bytes::from(vec![1_u8, 2_u8, 3_u8, 4_u8]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet1 = serializer.serialize(&message1, false, true).unwrap();

        let mut cursor = Cursor::new(packet1.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            72,
            "Unexpected timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected message stream id"
        );
        assert_eq!(
            packet1.can_drop(),
            true,
            "First packet was expected to be droppable"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &message1.data[..],
            "Unexpected payload contents"
        );

        let packet2 = serializer.serialize(&message2, false, false).unwrap();
        let mut cursor = Cursor::new(packet2.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            6 | 0b00000000,
            "Unexpected 2nd csid value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            82,
            "Unexpected 2nd timestamp value"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            4,
            "Unexpected 2nd message length value"
        );
        assert_eq!(cursor.read_u8().unwrap(), 50, "Unexpected 2nd type id");
        assert_eq!(
            cursor.read_u32::<LittleEndian>().unwrap(),
            12,
            "Unexpected 2nd message stream id"
        );
        assert_eq!(
            packet2.can_drop(),
            false,
            "Second packet was not expected to be droppable"
        );

        let mut payload_bytes = [0_u8; 50];
        let bytes_read = cursor.read(&mut payload_bytes[..]).unwrap();
        assert_eq!(bytes_read, 4, "Unexpected payload bytes read");
        assert_eq!(
            &payload_bytes[..bytes_read],
            &message1.data[..],
            "Unexpected payload contents"
        );
    }
    #[test]
    fn timestamp_delta_wraps_around_u32_boundary() {
        // A stream that stays live past the u32 millisecond rollover (~49 days):
        // the message before the wrap sits just below `u32::MAX`, the next one
        // just above zero. The on-wire delta must be the forward distance mod
        // 2^32, i.e. 20 - (u32::MAX - 10) == 31, not a backwards jump.
        let message1 = RawMessage {
            timestamp: RtmpTimestamp::new(u32::MAX - 10),
            type_id: 8,
            message_stream_id: 1,
            data: Bytes::from(vec![0xAF, 0x01, 0x02]),
        };
        let message2 = RawMessage {
            timestamp: RtmpTimestamp::new(20),
            type_id: 8,
            message_stream_id: 1,
            data: Bytes::from(vec![0xAF, 0x01, 0x03]),
        };

        let mut serializer = ChunkEncoder::new();
        let packet1 = serializer.serialize(&message1, false, false).unwrap();
        let packet2 = serializer.serialize(&message2, false, false).unwrap();

        // Audio (type 8) rides chunk stream 5; the first message is a full header.
        let mut cursor = Cursor::new(packet1.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            5 | 0b00000000,
            "First chunk after idle must be a full (type 0) header"
        );

        // Same stream/type/length with a nonzero delta compresses to a
        // time-delta-only (type 2) header carrying the wrapped delta.
        let mut cursor = Cursor::new(packet2.to_vec());
        assert_eq!(
            cursor.read_u8().unwrap(),
            5 | 0b10000000,
            "Chunk after wrap-around must stay a time-delta-only (type 2) header"
        );
        assert_eq!(
            cursor.read_u24::<BigEndian>().unwrap(),
            31,
            "Delta across the u32 rollover must wrap mod 2^32"
        );
    }
}

impl<P: Segments> bytes::Buf for PacketCursor<'_, P> {
    fn remaining(&self) -> usize {
        self.remaining
    }
    fn chunk(&self) -> &[u8] {
        self.current().unwrap_or(&[])
    }
    fn advance(&mut self, count: usize) {
        PacketCursor::advance(self, count);
    }
    fn chunks_vectored<'a>(&'a self, output: &mut [IoSlice<'a>]) -> usize {
        self.io_slices(output)
    }
}

impl<P: Segments> bytes::Buf for Packet<P> {
    fn remaining(&self) -> usize {
        self.remaining
    }
    fn chunk(&self) -> &[u8] {
        // All returned bytes belong to packet storage, independent of the snapshot.
        let s = self.state;
        if self.remaining == 0 {
            return &[];
        }
        let h = if s.chunk == 0 {
            &self.first
        } else {
            &self.continuation
        };
        if s.header < h.len {
            return &h.as_slice()[s.header..];
        }
        let segment = self.payload.segment(s.segment);
        let n = (segment.len() - s.offset)
            .min(self.chunk_size - s.body)
            .min(self.payload.len() - (s.chunk * self.chunk_size + s.body));
        &segment[s.offset..s.offset + n]
    }
    fn advance(&mut self, count: usize) {
        Packet::advance(self, count);
    }
    fn chunks_vectored<'a>(&'a self, slices: &mut [IoSlice<'a>]) -> usize {
        self.io_slices(slices)
    }
}
